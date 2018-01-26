from sqlink.util import read_ini_config
from sqlink.postgres import PostgresReader


def test_connect():
    reader = PostgresReader(**read_ini_config('system.cfg')['postgres-server'])
    assert isinstance(reader, PostgresReader)


# def test_reader():
#     reader = PostgresReader(**read_ini_config('system.cfg')['postgres-server'])
#     assert isinstance(reader, PostgresReader)

    # n = reader.rowcount('mydb.mytb')
    # print('  table "mytb" has {} records'.format(n))
    # assert n > 0
    #
    # sql = 'select * from mydb.mytb limit 10'
    # print(reader.read(sql))
