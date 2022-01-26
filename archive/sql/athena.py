import logging
from traceback import format_exc
import os
import random

from retrying import retry
import pyathena

from .hive import HiveTableMixin
from .sql import SQLClient
from ..s3 import reduce_boto_logging


logger = logging.getLogger(__name__)


TMP_DB = 'tmp'


def is_athena_error(e):
    if isinstance(e, pyathena.error.DatabaseError):
        logger.warning('error info before passed on to retrying')
        logger.warning(str(e))
        logger.warning(format_exc())
        return True
    return False


class Athena(SQLClient):
    def __init__(self, s3_result_dir: str = None) -> None:
        # `s3_result_dir` is where Athena query results are stored.
        # The default location is used if not specified.
        if s3_result_dir is None:
            s3_result_dir = 's3://aws-athena-query-results-{}-{}'.format(
                os.environ['AWS_ACCOUNT_ID'], os.environ['AWS_DEFAULT_REGION'])
        else:
            assert s3_result_dir.startswith('s3://')
        super().__init__(
            conn_func=pyathena.connect,
            s3_staging_dir=s3_result_dir,
            cursor_arraysize=1000,  # 1000 is Athena's upper limit
        )

    @retry(retry_on_exception=is_athena_error,
           wait_exponential_multiplier=30000,  # 30 seconds
           wait_exponential_max=120000,  # 2 minutes
           stop_max_attempt_number=7)
    def read(self, *args, **kwargs):
        return super().read(*args, **kwargs)

    @retry(retry_on_exception=is_athena_error,
           wait_exponential_multiplier=30000,  # 30 seconds
           wait_exponential_max=120000,  # 2 minutes
           stop_max_attempt_number=7)
    def write(self, *args, **kwargs):
        return super().write(*args, **kwargs)


def reduce_athena_logging():
    import pyathena.common
    assert pyathena.common  # silence pyflakes
    for name in logging.Logger.manager.loggerDict.keys():
        if name.startswith('pyathena'):
            logging.getLogger(name).setLevel(logging.ERROR)

    reduce_boto_logging()


class AthenaTable(HiveTableMixin):
    def __init__(self, location: str, **kwargs) -> None:
        assert location.startswith('s3://')
        super().__init__(location=location, **kwargs)

    def insert_overwrite_partition(self, engine: Athena, sql: str, *partition_values, purge_data: bool = True):
        '''
        `purge_data`: if `True`, purge existing data in the partion pointed to by `partition_values`.
            If `False`, do not purge; instead, assume the operation will write into sub-directories that
            are already purged, and there may be other subdirectories that should be left alone.

        See https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html
        '''
        tmp_tb = 'tmp' + str(random.random()).replace('.', '').replace('-', '')
        tmp_tb = f'{TMP_DB}.{tmp_tb}'

        if len(partition_values) < len(self.partitions):
            parts = ', '.join(
                [f"'{k}'" for k, v in self.partitions[len(partition_values):]])
            parts = f'partitioned_by = ARRAY[{parts}],'
            # Be sure to verify that the last columns in `sql` match these partition fields.
        else:
            parts = ''

        ppath = self.partition_location(*partition_values)

        assert self.stored_as.lower() in ('orc', 'parquet')

        sql = f'''
            CREATE TABLE {tmp_tb}
            WITH (
                external_location = '{ppath}',
                format = '{self.stored_as}',
                {parts}
                {self.stored_as.lower()}_compression = '{self.compression}'
            )
            AS
            {sql}
        '''

        engine.write(f'DROP TABLE IF EXISTS {tmp_tb}')
        if purge_data:
            self.purge_data(*partition_values)
        logger.debug('\n' + sql)
        engine.write(sql)
        self.update_partitions(engine, *partition_values)
        engine.write(f'DROP TABLE IF EXISTS {tmp_tb}')
