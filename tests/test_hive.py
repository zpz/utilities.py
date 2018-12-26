import os

import arrow
import pytest
import impala
from zpz.sql.hive import Hive, HiveTable
from zpz.sql.athena import Athena, AthenaTable


@pytest.fixture(scope='module')
def hive():
    return Hive(user='abc', host='def')


@pytest.fixture(scope='module')
def athena():
    return Athena()


def make_tmp_name():
    return 'tmp_test_hive_fell_free_to_delete'


def test_read(hive):
    db_name = 'default'
    tb_name = 'mytesttable'

    assert db_name in hive.get_databases()
    assert hive.has_database(db_name)
    assert tb_name in hive.get_tables(db_name)
    assert hive.has_table(db_name, tb_name)

    print()
    n = 3
    print('--- sample from table {} ---'.format(tb_name))
    dt = arrow.now().shift(days=-3).format('YYYY-MM-DD')
    sql = f"SELECT * FROM {db_name}.{tb_name} WHERE dt='{dt}' AND hour='13' LIMIT {3}"
    v = hive.read(sql).fetchall_pandas()
    assert len(v) == n
    print(v)


def test_read_iter(hive):
    # Before 'execute' is ever called,
    # iterating over the cursor is an error.
    with pytest.raises(impala.error.ProgrammingError):
        for row in hive.iterrows():
            print(row)

    sql = 'SHOW DATABASES'
    hive.read(sql)
    ndbs = 0
    print('databases:')
    for row in hive.iterrows():
        print(row[0], end=' ')
        ndbs += 1
    assert ndbs > 1

    # After 'execute' and 'fetch*', cursor may contain
    # empty result, and it's not an error to iterate over it.
    ndbs = 0
    for row in hive.iterrows():
        print(row)
        ndbs += 1
    assert ndbs == 0


def test_write_plain(hive):
    db_name = hive.user

    dbs = hive.get_databases()
    assert db_name in dbs
    assert hive.has_database(db_name)

    TMP_TB_NAME = make_tmp_name()

    # No partitions

    sql = '''
        DROP TABLE IF EXISTS {db}.{tb};
        CREATE TABLE {db}.{tb} (
            a STRING,
            b STRING,
            c BIGINT
            )
            STORED AS parquet
        '''.format(
        db=db_name, tb=TMP_TB_NAME)
    print('creating table "{}.{}"'.format(db_name, TMP_TB_NAME))
    hive.write(sql)

    assert TMP_TB_NAME in hive.get_tables(db_name)

    sql = '''
        INSERT INTO TABLE {db}.{tb}
        VALUES ('ab', 'cd', 23), ('xx', 'y', 45)
    '''.format(
        db=db_name, tb=TMP_TB_NAME)
    hive.write(sql)

    z = hive.read('select * from {}.{}'.format(db_name,
                                               TMP_TB_NAME)).fetchall_pandas()
    assert len(z) == 2

    print('dropping table "{}.{}"'.format(db_name, TMP_TB_NAME))
    hive.write("""
        DROP TABLE {db}.{tb};
        """.format(db=db_name, tb=TMP_TB_NAME))
    assert TMP_TB_NAME not in hive.get_tables(db_name)

    # Now with partitions

    sql = '''
        DROP TABLE IF EXISTS {db}.{tb};
        CREATE TABLE {db}.{tb} (
            a STRING,
            b STRING,
            c BIGINT
            )
            PARTITIONED BY (x STRING, y STRING)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
            STORED AS TEXTFILE
        '''.format(
        db=db_name, tb=TMP_TB_NAME)
    print('creating table "{}.{}"'.format(db_name, TMP_TB_NAME))
    hive.write(sql)

    assert TMP_TB_NAME in hive.get_tables(db_name)

    sql = '''
        INSERT OVERWRITE TABLE {db}.{tb}
        PARTITION(x, y)
        VALUES ('ab', 'cd', 23, '12', 'adsf'), ('xx', 'y', 56, '34', '56')
    '''.format(
        db=db_name, tb=TMP_TB_NAME)
    hive.write(sql)

    z = hive.read('select * from {}.{}'.format(db_name,
                                               TMP_TB_NAME)).fetchall_pandas()
    assert len(z) == 2

    print('dropping table "{}.{}"'.format(db_name, TMP_TB_NAME))
    hive.write("""
        DROP TABLE {db}.{tb};
        """.format(db=db_name, tb=TMP_TB_NAME))
    assert TMP_TB_NAME not in hive.get_tables(db_name)


def test_write_table(hive):
    db_name = hive.user

    dbs = hive.get_databases()
    assert db_name in dbs
    assert hive.has_database(db_name)

    TMP_TB_NAME = make_tmp_name()

    # No partitions

    table = HiveTable(
        db_name=db_name,
        tb_name=TMP_TB_NAME,
        columns=[('a', 'string'), ('b', 'string'), ('c', 'bigint')],
        stored_as='parquet'
    )
    print('creating table "{}.{}"'.format(db_name, TMP_TB_NAME))
    table.create(hive, drop_if_exists=True)

    assert TMP_TB_NAME in hive.get_tables(db_name)

    sql = f'''
        INSERT INTO TABLE {table.full_name}
        VALUES ('ab', 'cd', 23), ('xx', 'y', 45)
    '''
    hive.write(sql)

    z = hive.read(f'select * from {table.full_name}').fetchall_pandas()
    assert len(z) == 2

    print(f'dropping table "{table.full_name}"')
    table.drop(hive)
    assert TMP_TB_NAME not in hive.get_tables(db_name)

    # Now with partitions

    table = HiveTable(
        db_name=db_name,
        tb_name=TMP_TB_NAME,
        columns=[('a', 'string'), ('b', 'string'), ('c', 'bigint')],
        partitions=[('x', 'string'), ('y', 'string')],
        stored_as='textfile'
    )
    print('creating table "{}.{}"'.format(db_name, TMP_TB_NAME))
    table.create(hive)

    assert TMP_TB_NAME in hive.get_tables(db_name)

    sql = '''
        INSERT OVERWRITE TABLE {db}.{tb}
        PARTITION(x, y)
        VALUES ('ab', 'cd', 23, '12', 'adsf'), ('xx', 'y', 56, '34', '56')
    '''.format(
        db=db_name, tb=TMP_TB_NAME)
    hive.write(sql)

    z = hive.read('select * from {}.{}'.format(db_name,
                                               TMP_TB_NAME)).fetchall_pandas()
    assert len(z) == 2

    print('dropping table "{}.{}"'.format(db_name, TMP_TB_NAME))
    table.drop(hive)
    assert TMP_TB_NAME not in hive.get_tables(db_name)


def test_s3_table(hive):
    db_name = hive.user

    assert hive.has_database(db_name)

    TMP_TB_NAME = make_tmp_name()

    table = HiveTable(
        db_name=db_name,
        tb_name=TMP_TB_NAME,
        columns=[('a', 'string'), ('b', 'string'), ('c', 'bigint')],
        stored_as='textfile',
        location='s3n://myorg/tmp/' + TMP_TB_NAME
    )
    table.purge_data()

    print('creating table "{}.{}"'.format(db_name, TMP_TB_NAME))
    table.create(hive, drop_if_exists=True)

    assert TMP_TB_NAME in hive.get_tables(db_name)

    sql = f'''
        INSERT INTO TABLE {table.full_name}
        VALUES ('ab', 'cd', 23), ('xx', 'y', 45)
    '''
    hive.write(sql)

    z = hive.read(f'select * from {table.full_name}').fetchall_pandas()
    assert len(z) == 2

    print(f'dropping table "{table.full_name}"')
    table.purge_data()
    table.drop(hive)
    assert TMP_TB_NAME not in hive.get_tables(db_name)

    # Now with partitions

    table = HiveTable(
        db_name=db_name,
        tb_name=TMP_TB_NAME,
        columns=[('a', 'string'), ('b', 'string'), ('c', 'bigint')],
        partitions=[('x', 'string'), ('y', 'string')],
        location='s3n://myorg/tmp/' + TMP_TB_NAME
    )

    print('creating table "{}.{}"'.format(db_name, TMP_TB_NAME))
    table.create(hive, drop_if_exists=True)

    assert TMP_TB_NAME in hive.get_tables(db_name)

    sql = f'''
        INSERT OVERWRITE TABLE {table.full_name}
        PARTITION(x, y)
        VALUES ('ab', 'cd', 23, '12', 'adsf'), ('xx', 'y', 56, '34', '56')
    '''
    hive.write(sql)

    z = hive.read(f'select * from {table.full_name}').fetchall_pandas()
    assert len(z) == 2

    print('dropping table "{}.{}"'.format(db_name, TMP_TB_NAME))
    table.drop(hive)
    assert TMP_TB_NAME not in hive.get_tables(db_name)



def test_athena_hive(hive, athena):
    athena_db_name = 'tmp'
    athena_tb_name = make_tmp_name() + '_athena'

    athena_table = AthenaTable(
        db_name=athena_db_name,
        tb_name=athena_tb_name,
        columns=[('a', 'string'), ('b', 'string'), ('c', 'bigint')],
        partitions=[('x', 'string'), ('y', 'string')],
        location='s3://myorg/tmp/' + athena_tb_name
    )

    athena_table.create(athena, drop_if_exists=True)
    athena_table.purge_data()

    sql = f'''
        SELECT * FROM (
            VALUES 
                ('ab', 'cd', 23, '12', 'adsf'), 
                ('xx', 'y', 56, '34', '56'),
                ('xxd', 'ay', 156, '234', '596'),
                ('xxc', 'y3', 6, '345', '516')
        ) AS source_table (a, b, c, x, y)
    '''
    athena_table.insert_overwrite_partition(athena, sql)

    z = athena.read(f'select * from {athena_table.full_name}').fetchall_pandas()
    assert len(z) == 4

    hive_db_name = hive.user

    hive_table = HiveTable.from_athena_table(athena_table, db_name=hive_db_name)
    hive_table.create(hive, drop_if_exists=True)

    z = hive.read(f'select * from {hive_table.full_name}').fetchall_pandas()
    assert len(z) == 4

    hive_table.drop(hive)
    athena_table.drop(athena)

