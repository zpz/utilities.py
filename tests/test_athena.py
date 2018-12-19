from zpz.sql.athena import get_athena, AthenaTable
from zpz.sql.hive import get_hive, HiveTable


def make_tmp_name():
    return 'tmp_test_athena_feel_free_to_delete'


def test_athena():
    athena = get_athena()
    db_name = 'myname'
    tb_name = make_tmp_name()

    table = AthenaTable(
        db_name=db_name,
        tb_name=tb_name,
        columns=[('a', 'string'), ('b', 'string'), ('c', 'bigint')],
        partitions=[('x', 'string'), ('y', 'string')],
        location='s3://my-org/tmp/' + tb_name
    )

    table.create(athena, drop_if_exists=True)
    table.purge_data()

    sql = f'''
        SELECT * FROM (
            VALUES 
                ('ab', 'cd', 23, '12', 'adsf'), 
                ('xx', 'y', 56, '34', '56')
        ) AS source_table (a, b, c, x, y)
    '''
    table.insert_overwrite_partition(athena, sql)
    
    z = athena.read(f'select * from {table.full_name}').fetchall_pandas()
    assert len(z) == 2

    table.purge_data()
    table.drop(athena)


def test_hive_athena():
    hive = get_hive()
    hive_db_name = hive.user
    hive_tb_name = make_tmp_name() + '_hive'

    hive_table = HiveTable(
        db_name=hive_db_name,
        tb_name=hive_tb_name,
        columns=[('a', 'string'), ('b', 'string'), ('c', 'bigint')],
        partitions=[('x', 'string'), ('y', 'string')],
        location='s3n://myorg/tmp/' + hive_tb_name
    )

    hive_table.create(hive, drop_if_exists=True)
    hive_table.purge_data()

    sql = f'''
        INSERT OVERWRITE TABLE {hive_table.full_name}
        PARTITION(x, y)
        VALUES
            ('ab', 'cd', 23, '12', 'adsf'),
            ('xx', 'y', 56, '34', '56'),
            ('tt', 'z', 38, '29', 'dkdk')
    '''
    hive.write(sql)

    z = hive.read(f'select * from {hive_table.full_name}').fetchall_pandas()
    assert len(z) == 3

    athena = get_athena()
    athena_db_name = 'myname'

    athena_table = AthenaTable.from_hive_table(hive_table, db_name=athena_db_name)
    athena_table.create(athena, drop_if_exists=True)

    z = athena.read(f'select * from {athena_table.full_name}').fetchall_pandas()
    assert len(z) == 3

    hive_table.drop(hive)
    athena_table.drop(athena)