import os
from pytest import fixture
from zpz.filesys.path import relative_path

from zpz.spark import PySparkSession, ScalaSparkSession, SparkSession, SparkSessionError

livy_server_url = None

@fixture(scope='module')
def pysession():
    return PySparkSession(livy_server_url)


@fixture(scope='module')
def scalasession():
    return ScalaSparkSession(livy_server_url)


pi_py = """\
    import random

    NUM_SAMPLES = 100000
    def sample(p):
        x, y = random.random(), random.random()
        return 1 if x*x + y*y < 1 else 0

    count = sc.parallelize(range(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
    pi = 4.0 * count / NUM_SAMPLES

    mylist = [1, 3, 'abc']
    mytuple = ('a', 'b', 'c', 1, 2, 3)
    mydict = {'a': 13, 'b': 'usa'}

    # spark 2.0    
    # from pyspark.sql import Row
    # pi_df = spark.createDataFrame([Row(value=pi)])

    # spark 1.6:
    from pyspark.sql import SQLContext, Row
    pi_df = SQLContext(sc).createDataFrame([Row(value=pi)])
"""


def test_py(pysession):
    print()

    pysession.run('z = 1 + 3')
    z = pysession.read('z')
    assert z == 4

    pysession.run(pi_py)

    pi = pysession.read('pi')
    print('printing a number:')
    print(pi)
    assert 3.0 < pi < 3.2

    code = '''pip2 = pi + 2'''
    pysession.run(code)
    pip2 = pysession.read('pip2')
    assert 3.0 < pip2 - 2 < 3.2

    mylist = pysession.read('mylist')
    assert mylist == [1, 3, 'abc']
    mytuple = pysession.read('mytuple')
    assert mytuple == ('a', 'b', 'c', 1, 2, 3)
    mydict = pysession.read('mydict')
    assert mydict == {'a': 13, 'b': 'usa'}

    local_df = pysession.read('pi_df')
    print()
    print('printing a {}:'.format(type(local_df)))
    print(local_df)
    pi = local_df.iloc[0, 0]
    assert 3.0 < pi < 3.2

    assert pysession.read('3 + 6') == 9

    print()
    print('printing in Spark session:')
    z = pysession.run('''print(type(pip2))''')
    # `run` does not print.
    # printouts in Spark are collected in the return of `run`.
    print(z)

    # `str` comes out as `str`
    print()
    print(pysession.read('str(type(pi))'))
    print(pysession.read('type(pi_df).__name__'))

    # `bool` comes out as `bool`
    z = pysession.read('''isinstance(pi, float)''')
    print()
    print('printing boolean:')
    print(z)
    print(type(z))
    assert z is True

    assert pysession.read('str(isinstance(pi, float))') == 'True'

    # `bool` comes out as `numpy.bool_`
    # assert session.read(
    #     '''isinstance(pi_df, pyspark.sql.dataframe.DataFrame)''')


py_error = """\
    class MySparkError(Exception):
        pass

    a = 3
    b = 4
    raise MySparkError('some thing is so wrong!)
    print('abcd')
"""


def test_py_error(pysession):
    try:
        z = pysession.run(py_error)
    except SparkSessionError as e:
        print(e)


def test_file(pysession):
    pysession.run_file(relative_path('./spark_test_scripts/script_a.py'))
    z = pysession.read('magic')
    assert 6.0 < z < 7.0


def test_func(pysession):
    f = '''\
    def myfunc(a, b, names, squared=False):
        assert len(a) == 3
        assert len(b) == 3
        assert len(names) == 3
        c = [aa + bb for (aa, bb) in zip(a, b)]
        if squared:
            c = [x*x for x in c]
        d = {k:v for (k,v) in zip(names, c)}
        return d
    '''
    pysession.run(f)
    z = pysession.run_function('myfunc', [1,2,3], [4,6,8], ['first', 'second', 'third'])
    assert {k: z[k] for k in sorted(z)} == {'first': 5, 'second': 8, 'third': 11}

    z = pysession.run_function('myfunc', [1,2,3], [4,6,8], squared=True, names=['first', 'second', 'third'])
    assert {k: z[k] for k in sorted(z)} == {'first': 25, 'second': 64, 'third': 121}


pi_scala = """
    val NUM_SAMPLES = 100000;
    val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
        val x = Math.random();
        val y = Math.random();
        if (x*x + y*y < 1) 1 else 0
        }.reduce(_ + _);
    val pi = 4.0 * count / NUM_SAMPLES;
    println(\"Pi is roughly \" + pi)
    """


def test_scala(scalasession):
    z = scalasession.run('1 + 1')
    assert z == 'res0: Int = 2'

    z = scalasession.run(pi_scala)
    assert 'Pi is roughly 3.1' in z


scala_error = """
    val NUM = 1000
    val count = abc.NUM
"""


def test_scala_error(scalasession):
    try:
        z = scalasession.run(scala_error)
    except SparkSessionError as e:
        print(e)


def test_pyspark():
    sess = SparkSession(livy_server_url, kind='pyspark')

    z = sess.run('1 + 1')
    assert z == '2'

    z = sess.run('import math; math.sqrt(2.0)')
    assert z.startswith('1.4142')
