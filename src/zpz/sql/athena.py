import logging
from traceback import format_exc
import os
import random
from typing import List, Tuple, Union

from retrying import retry
import pyathena

from .hive import HiveTableMixin
from .sql import SQLClient
from ..s3 import reduce_boto_logging


import logging

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
            wait_exponential_multiplier=30000, # 30 seconds
            wait_exponential_max=120000,  # 2 minutes
            stop_max_attempt_number=7)
    def read(self, *args, **kwargs):
        return super().read(*args, **kwargs)

    @retry(retry_on_exception=is_athena_error, 
            wait_exponential_multiplier=30000, # 30 seconds
            wait_exponential_max=120000,  # 2 minutes
            stop_max_attempt_number=7)
    def write(self, *args, **kwargs):
        return super().write(*args, **kwargs)


def reduce_athena_logging():
    import pyathena.common
    for name in logging.Logger.manager.loggerDict.keys():
        if name.startswith('pyathena'):
            logging.getLogger(name).setLevel(logging.ERROR)

    reduce_boto_logging()


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=7)
def get_athena():
    return Athena()


class AthenaTable(HiveTableMixin):
    def __init__(self, location: str, **kwargs) -> None:
        assert location.startswith('s3://')
        super().__init__(location=location, **kwargs)

    def insert_overwrite_partition(self, engine: Athena, sql: str, *partition_values, purge_data: bool=True):
        '''
        `purge_data`: if `True`, purge existing data in the partion pointed to by `partition_values`.
            If `False`, do not purge; instead, assume the operation will write into sub-directories that
            are already purged, and there may be other subdirectories that should be left alone.

        See https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html
        '''
        tmp_tb = 'tmp' + str(random.random()).replace('.', '').replace('-', '')
        tmp_tb = f'{TMP_DB}.{tmp_tb}'

        if len(partition_values) < len(self.partitions):
            parts = ', '.join([f"'{k}'" for k,v in self.partitions[len(partition_values): ]])
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

    @classmethod
    def from_hive_table(cls, table: 'HiveTable', db_name: str, tb_name: str=None) -> 'AthenaTable':
        '''
        Use case of this method:

            If we have defined and manipulated an _external_ table in Hive,
            then the data is in S3 but meta data is in Hadoop.
            This table is not available to Athena because the meta data is not in S3.
            The current method takes the existing Hive (external) table definition
            and creates an AthenaTable object, so that the table's meta data is in sync
            with the Hive table. After calling `create` on the AthenaTable object,
            the table is present in the S3 meta data store, so that the table can be used
            by Athena.

            The table meta data in Hadoop and in S3 are independent of each other.
            Therefore if the table's data is modified by either side of Hive and Athena,
            the other side needs to update partitions.
        '''
        assert table.s3external
        location = table.location
        if location.startswith('s3n://'):
            location = 's3://' + location[len('s3n://') :]
        return cls(
            db_name=db_name,
            tb_name=tb_name or table.tb_name,
            columns=table.columns,
            partitions=table.partitions,
            stored_as=table.stored_as,
            field_delimiter=table.field_delimiter,
            compression=table.compression,
            location=location)