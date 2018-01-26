import os

import pandas as pd
from impala.error import NotSupportedError
import pytest

from sqlink.util import read_ini_config
from sqlink.impala import ImpalaReader, ImpalaWriter

from .common import *


@pytest.fixture(scope='module')
def reader():
    return ImpalaReader(**read_ini_config('system.cfg')['impala-server'])


@pytest.fixture(scope='module')
def writer():
    return ImpalaWriter(**read_ini_config('system.cfg')['impala-server'])


def test_impyla_read(reader):
    do_read(reader)


def test_impyla_read_iter(reader):
    do_read_iter(reader)


def test_impyla_write(reader, writer):
    do_write(reader, writer)


# Passed in prod cluster.
#
# This test verifies that some suggested solutions on the web
# regarding writing a Pandas DataFrame to Impla using 'impyla'
# does not work.
def test_impyla_write_pandas(reader):
    db_name = 'tmp'
    assert db_name in reader.get_databases()

    conn_args = read_ini_config('system.cfg')['impala-server']
    writer = ImpalaWriter(**conn_args, **{'database': db_name})

    TMP_TB_NAME = 'tmp_tttzzzyyyxxx1324asdfo'

    sql = 'DROP TABLE IF EXISTS {}'.format(TMP_TB_NAME)
    writer.execute(sql)

    # work-around.
    os.environ['LOGNAME'] = conn_args['user']

    TMP_DF = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    df = TMP_DF
    with pytest.raises(NotSupportedError) as e:
        df.to_sql(name=TMP_TB_NAME, con=writer._conn, flavor='mysql')
