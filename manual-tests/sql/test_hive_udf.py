import base64
import inspect
import os
import pathlib

import numpy as numpy
import pytest
from zpz.sql.hive import Hive, HiveTable
from zpz.sql import hive_udf_example, hive_udaf_example, hive_udf_args_example


@pytest.fixture(scope='module')
def hive():
    return Hive(user='abc', host='def')


def make_tmp_name():
    return 'tmp_test_hive_feel_free_to_delete'


@pytest.fixure(scope='module')
def table(hive):
    db_name = hive.user
    tb_name = make_tmp_name()

    assert hive.has_database(db_name)

    table = HiveTable(
        db_name=db_name,
        tb_name=tb_name,
        columns=[('id', 'int'), ('info_json', 'string')]
    )
    print('creating table "{}.{}"'.format(db_name, tb_name))
    table.create(hive, drop_if_exists=True)

    assert hive.has_database(db_name, tb_name)

    sql = f'''
        INSERT INTO TABLE {table.full_name}
        SELECT * FROM (
            SELECT STACK (
                6,
                1, '{{"make": "honda", "price": 1000}}',
                2, '{{"make": "ford", "price": 2000}}',
                3, '{{"make": "ford"}}',
                4, '{{"make": "tesla", "price": 3000}}',
                5, '{{"make": "honda", "price": 2000}}',
                6, '{{"make": "ford", "price": 4000}}'
            )
        ) s
    '''

    hive.write(sql)
    assert hive.has_table(db_name, tb_name)
    print(f'table {db_name}.{tb_name} created successfully')
    yield table

    print(f'dropping table {db_name}.{tb_name}')
    table.drop(hive)
    assert not hive.has_table(db_name, tb_name)
    print(f'table {db_name}.{tb_name} dropped successfully')


def test_udf(hive, table):
    code = make_udf(hive_udf_example)

    sql = f'''
        SELECT
            TRANSFORM (
                id,
                info_json
            )
            USING '{code}'
            AS (make STRING, price FLOAT)
        FROM {table.full_name}
    '''
    print(sql)

    hive.read(sql)
    z = hive.fetchall_pandas()
    z = z.sort_values(['make', 'price'])
    print(z)

    # Expected result:
    #
    #     make   price
    # 1   ford   2000.0
    # 5   ford   4000.0
    # 2   ford      NaN
    # 0  honda   1000.0
    # 4  honda   2000.0
    # 3  tesla   3000.0

    assert len(z) == 6
    assert z['make'].tolist() == ['ford', 'ford', 'ford', 'honda', 'honda', 'tesla']
    assert np.isnan(z['price'].iloc[2])
    assert z['price'].iloc[3] == 1000


def test_udaf(hive, table):
    code = make_udf(hive_udaf_example)

    sql = f'''
        SELECT
            TRANSFORM (
                info_json
            )
            USING '{code}'
            AS (make STRING, avg_price FLOAT, null_prices INT)
        FROM (
            SELECT
                id,
                info_json
            FROM {table.full_name}
            CLUSTER BY GET_JSON_OBJECT(info_json, '$.make')
        ) AS t
    '''
    print(sql)

    hive.read(sql)
    z = hive.fetchall_pandas()
    z = z.sort_values(['make'])
    print(z)

    # Expected result:
    #
    #    make   avg_price  null_prices
    # 1   ford     3000.0            1
    # 0  honda     1500.0            0
    # 2  tesla     3000.0            0

    assert len(z) == 3
    assert z['make'].tolist() == ['ford', 'honda', 'tesla']
    assert z['avg_price'].tolist() == [3000, 1500, 3000]
    assert z['null_prices'].tolist() == [1, 0, 0]



def test_udf_args(hive, table):
    def make_sql(country, default_price):
        code = make_udf(hive_udf_args_example, country, default_price)
        sql = f'''
            SELECT
                TRANSFORM (
                    id,
                    info_json
                )
                USING '{code}'
                AS (make STRING, price FLOAT)
            FROM {table.full_name}
        '''
    return sql

    sql = make_sql('jap', 250)
    print(sql)
    hive.read(sql)
    z = hive.fetchall_pandas()
    z = z.sort_values(['make', 'price'])
    print(z)

    # Expected result:
    #
    #     make   price
    # 0  honda  1000.0
    # 1  honda  2000.0

    assert z['make'].tolist() == ['honda', 'honda']
    assert z['price'].tolist() == [1000, 2000]

    sql = make_sql('america', 550)
    print(sql)
    hive.read(sql)
    z = hive.fetchall_pandas()
    z = z.sort_values(['make', 'price'])
    print(z)

    # Expected result:
    #
    #     make   price
    # 1   ford   550.0
    # 0   ford  2000.0
    # 3   ford  4000.0
    # 2  tesla  3000.0

    assert z['make'].tolist() == ['ford', 'ford', 'ford', 'tesla']
    assert z['price'].tolist() == [550, 2000, 4000, 3000]


    sql = make_sql('all', 340)
    print(sql)
    hive.read(sql)
    z = hive.fetchall_pandas()
    z = z.sort_values(['make', 'price'])
    print(z)

    # Expected result:
    #
    #     make   price
    # 2   ford   340.0
    # 1   ford  2000.0
    # 5   ford  4000.0
    # 0  honda  1000.0
    # 4  honda  2000.0
    # 3  tesla  3000.0

    assert z['make'].tolist() == ['ford', 'ford', 'ford', 'honda', 'honda', 'tesla']
    assert z['price'].tolist() == [340, 2000, 4000, 1000, 2000, 3000]


def test_udf_follow_by_agg(hive, table):
    code = make_udf(hive_udf_example)

    sql = f'''
        SELECT
            make,
            SUM(price) AS price_total
        FROM
            (
             SELECT
                TRANSFORM (
                    id,
                    info_json
                )
                USING '{code}'
                AS (make STRING, price FLOAT)
             FROM {table.full_name}
            ) AS A
        GROUP BY make
        '''
    print(sql)

    hive.read(sql)
    z = hive.fetchall_pandas()
    z = z.sort_values(['make', 'price_total'])
    print(z)
    
    # Expected result:
    #
    #     make   price
    # 0   ford  6000.0
    # 1  honda  3000.0
    # 2  tesla  3000.0

    assert len(z) == 3
    assert z['make'].tolist() == ['ford', 'honda', 'tesla']
    assert z['price_total'].tolist() == [6000.0, 3000.0, 3000.0]
