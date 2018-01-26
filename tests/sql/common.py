def do_read(reader):
    db_name = 'default'
    tb_name = 'default'

    dbs = reader.get_databases()
    assert db_name in dbs
    tbs = reader.get_tables(db_name)
    assert tb_name in tbs

    print()
    print('--- sample from table {} ---'.format(tb_name))
    sql = "SELECT * FROM {}.{} WHERE d='2016-09-01' AND h='1800' LIMIT 3".format(
        db_name, tb_name)
    reader.execute(sql)
    v = reader.fetchall_pandas()
    print(v)


def do_read_iter(reader):
    # Before 'execute' is ever called,
    # iterating over the cursor gets nothing but it's not an error.
    n = 0
    for row in reader:
        print(row)
        n += 1
    assert n == 0

    sql = 'SHOW DATABASES'
    reader.execute(sql)
    ndbs = 0
    print('databases:')
    for row in reader:
        print(row[0], end=' ')
        ndbs += 1
    assert ndbs > 1

    # After 'execute' and 'fetch*', cursor may contain
    # empty result, and it's not an error to iterate over it.
    ndbs = 0
    for row in reader:
        print(row)
        ndbs += 1
    assert ndbs == 0


def do_write(reader, writer):
    db_name = 'tmp'

    dbs = reader.get_databases()
    assert db_name in dbs

    TMP_TB_NAME = 'tmp_tttzzzyyyxxx1324asdfo'

    sql = [
        'DROP TABLE IF EXISTS {}.{}'.format(db_name, TMP_TB_NAME),
        """
        CREATE TABLE {db}.{tb} (
            a STRING,
            b STRING,
            c BIGINT
            )
            STORED AS parquet
        """.format(db=db_name, tb=TMP_TB_NAME),
    ]
    print('creating table "{}.{}"'.format(db_name, TMP_TB_NAME))
    writer.execute(sql)
    assert TMP_TB_NAME in reader.get_tables(db_name)

    print('dropping table "{}.{}"'.format(db_name, TMP_TB_NAME))
    writer.write("""
        DROP TABLE {db}.{tb};
        """.format(db=db_name, tb=TMP_TB_NAME))
    assert TMP_TB_NAME not in reader.get_tables(db_name)
