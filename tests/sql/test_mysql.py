from sqlink.util import read_ini_config
from sqlink.mysql import MySQLReader


def test_connect():
    reader = MySQLReader(**read_ini_config('system.cfg')['mysql-server'])
    assert isinstance(reader, MySQLReader)


def test_reader():
    reader = MySQLReader(**read_ini_config('system.cfg')['mysql-server'])

    n = reader.rowcount('mydb.mytb')
    print('  table "mytb" has {} records'.format(n))
    assert n > 0

    sql = 'select * from mydb.mytb limit 10'
    print(reader.read(sql))
