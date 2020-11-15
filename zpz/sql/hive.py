import asyncio
import base64
import inspect
import logging
import os
import os.path
import random
import time
from pprint import pprint
from types import ModuleType
from typing import Sequence, Union, List
import warnings

import aioodbc
import pyodbc

from .sql import SQLClient, Connection, AsyncConnection
from ..s3 import Bucket


logger = logging.getLogger(__name__)


class HiveConnection(Connection):
    def get_databases(self) -> List[str]:
        z = self.read('SHOW DATABASES').fetchall()
        return [v[0] for v in z]

    def get_tables(self, db_name: str) -> List[str]:
        z = self.read(f'SHOW TABLES IN {db_name}').fetch_all()
        return [v[0] for v in z]

    def show_create_table(self, db_name: str, tb_name: str) -> None:
        sql = f'SHOW CREATE TABLE {db_name}.{tb_name}'
        z = self.read(sql).fetchall()
        z = '\n'.join(v[0] for v in z)
        print(z)

    def describe_table(self, db_name: str, tb_name: str) -> None:
        sql = f'DESCRIBE FORMATTED {db_name}.{tb_name}'
        z = self.read(sql).fetchall_pandas()
        print(z)


class HiveAsyncConnection(AsyncConnection):
    async def get_databases(self) -> List[str]:
        await self.read('SHOW DATABASES')
        z = await self.fetchall()
        return [v[0] for v in z]

    async def get_tables(self, db_name: str) -> List[str]:
        await self.read(f'SHOW TABLES IN {db_name}')
        z = await self.fetchall()
        return [v[0] for v in z]


class Hive(SQLClient):
    CONNECTION_CLASS = HiveConnection
    ASYNCCONNECTION_CLASS = HiveAsyncConnection

    def __init__(self,
                 *,
                 user: str,
                 password: str,
                 host: str,
                 port: int = 10000,
                 database: str = None,
                 dynamic_partition: bool = True,
                 configuration: dict = None):
        '''
        `configuration` is a dict containing things you would otherwise write as

            set abc=def;
            set jkl=xyz;

        in scripts. You can continuue to write including those statements in a call
        to `write`, or specify them in `configuration`.
        '''
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

        if configuration:
            config = {k: str(v) for k, v in configuration.items()}
        else:
            config = {}
        if dynamic_partition:
            config['hive.exec.dynamic.partition'] = 'true'
            config['hive.exec.dynamic.partition.mode'] = 'nonstrict'

        # config['hive.execution.engine'] = 'tez'
        # config['tez.queue.name'] = 'myqueue'
        # config['hive.optimize.s3.query'] = 'true'
        # config['hive.enforce.bucketing'] = 'true'

        # Maybe more options need to be set.
        cc = [
            ('mapred.min.split.size', '2048000000'),
            ('mapred.max.split.size', '2048000000'),
            ('yarn.nodemanager.resource.memory-mb', '20480'),
            ('mapreduce.map.memory.mb', '16384'),
            ('mapreduce.map.java.opts', '-Xmx6144m'),
            ('mapred.map.tasks', '64'),
            ('mapred.reduce.tasks', '64')
        ]
        self._configuration = {**dict(cc), **config}

    def connection_string(self):
        conn_args = {
            'driver': '{Hortonworks Hive ODBC Driver 64-bit}',
            'host': self.host,
            'HiveServerType': 2,
            'port': self.port,
            'uid': '{' + self.user + '}',
            'pwd': '{' + self.password + '}',
            'database': self.database,
            'AuthMech': 3,
            'AutoCommit': 1,
            'ssl': 1,
            'AllowSelfSignedServerCert': 1,
            'ApplySSPWithQueries': 1,
            'EnableTempTable': 1,
            'FastSQLPrepare': 1,
            'InvalidSessionAutoRecover': 1,
            'UseNativeQuery': 1,
            'EnableAsyncExec': 1,
            'DefaultStringColumnLength': 100000,
            **{f'SSP_{k}': f'{{{{v}}}}' for k, v in self._configuration.items()
        }
        return ';'.join(f'{k} = {v}' for k, v in conn_args.items()

    def connect(self):
        conn_str=self.connection_string
        conn=pyodbc.connect(conn_str, autocommit=True)
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn.setencoding(encoding='utf-8')
        logger.info('connected to Hive server @%s', self.host)
        return conn

    async def a_connect(self):
        conn_str=self.connection_string
        conn=await aioodbc.connect(dsn=conn_str, autocommit=True)
        conn._conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        conn._conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn._conn.setencoding(encoding='utf-8')
        logger.info('connected to Hive server @%s', self.host)
        logger.info('connected to Hive server @%s', self.host)
        return conn


class HiveTableMixin:
    def __init__(self, *,
                 db_name: str,
                 tb_name: str,
                 columns: List[Tuple[str, str]],
                 partitions: List[Tuple[str, str]]=None,
                 stored_as: str='ORC',
                 field_delimiter: str='\\t',
                 compression: str='ZLIB',
                 location: str=None) -> None:
        self.db_name=db_name
        self.tb_name=tb_name
        self.columns=[(name, type_.upper()) for (name, type_) in columns]
        if partitions:
            self.partitions=[(name, type_.upper())
                               for (name, type_) in partitions]
        else:
            self.partitions=[]

        stored_as=stored_as.upper()
        assert stored_as in ('ORC', 'PARQUET', 'TEXTFILE')
        self.stored_as=stored_as
        self.field_delimiter=field_delimiter
        self.compression=compression

        self.external=False
        self.s3external=False
        if location:
            self.location=location.rstrip('/') + '/'   # Ensure trailing '/'
            loc=self.location
            if loc.startswith('s3://') or loc.startswith('s3n://'):
                if loc.startswith('s3://'):
                    z=self.location[len('s3://'):]
                else:
                    z=self.location[len('s3n://'):]
                assert '/' in z
                self._s3_bucket_key=z[: z.find('/')]
                self._s3_bucket_path=z[(z.find('/') + 1):]
                self.s3external=True
            self.external=True

    @ property
    def full_name(self):
        return self.db_name + '.' + self.tb_name

    def create(self, engine, drop_if_exists: bool=False) -> None:
        '''
        `engine` is a `Hive` or `Athena` object.
        '''
        def collapse(spec):
            return ', '.join(name + ' ' + type_ for (name, type_) in spec)

        columns=collapse(self.columns)

        if self.partitions:
            partitions=f"PARTITIONED BY ({collapse(self.partitions)})"
        else:
            partitions=''

        if self.external:
            location=f"LOCATION '{self.location}'"
            external='EXTERNAL'
        else:
            location=''
            external=''

        if self.stored_as in ('ORC', 'PARQUET'):
            stored_as=f'''
                STORED AS {self.stored_as}
                {location}
                TBLPROPERTIES ('{self.stored_as.lower()}.compress' = '{self.compression}')
                '''
        else:
            stored_as=f'''
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '{self.field_delimiter}'
                STORED AS {self.stored_as}
                {location}
                '''

        sql=f'''
            CREATE {external} TABLE {self.full_name}
            ({columns})
            {partitions}
            {stored_as}
            '''

        if drop_if_exists:
            self.drop(engine)

        engine.write(sql)
        if self.external:
            engine.write(f'MSCK REPAIR TABLE {self.full_name}')

    def drop(self, engine) -> None:
        engine.write(f'DROP TABLE IF EXISTS {self.full_name}')

    def _partition_values_to_path(self, *partition_values) -> str:
        assert 0 < len(partition_values) <= len(self.partitions)
        return '/'.join(f"{k}={v}" for v, (k, _) in zip(partition_values, self.partitions[: len(partition_values)]))

    def _partition_values_to_condition(self, *partition_values) -> str:
        '''
        `partition_values` appear in the order they are listed
        in the table definition.
        '''
        assert 0 < len(partition_values) <= len(self.partitions)
        elements=[]
        for v, (k, t) in zip(partition_values, self.partitions[: len(partition_values)]):
            if t.lower() in ('string', 'char', 'varchar'):
                elements.append(f"{k}='{v}'")
            else:
                elements.append(f"{k}={v}")
        return ', '.join(elements)

    def get_partitions(self, engine, *partition_values) -> List[str]:
        if not self.partitions:
            raise Exception('not a partitioned table')
        sql=f'SHOW PARTITIONS {self.full_name}'
        if partition_values:
            cond_str=self._partition_values_to_condition(*partition_values)
            sql += f' PARTITION({cond_str})'
        z=engine.read(sql).fetchall()
        return [v[0] for v in z]

    def show_partitions(self, engine, *partition_values) -> None:
        pprint(sorted(self.get_partitions(engine, *partition_values)))

    def show_partition_counts(self, engine, *partition_values):
        if not self.partitions:
            raise Exception('not a partitioned table')
        cols=', '.join(v[0] for v in self.partitions)
        sql=f'''
            SELECT
                {cols},
                COUNT(*) AS count
            FROM {self.full_name}'''
        if partition_values:
            cond_str=self._partition_values_to_condition(*partition_values)
            sql=sql + f'''
                WHERE {cond_str}'''
        sql=sql + f'''
            GROUP BY {cols}
            ORDER BY {cols}'''
        z=engine.read(sql).fetchall_pandas()
        print(z)

    def drop_partitions(self, engine, *partition_values):
        if partition_values:
            cond_str=self._partition_values_to_condition(*partition_values)
            sql=f"ALTER TABLE {self.full_name} DROP IF EXISTS PARTITION({cond_str})"
            engine.write(sql)
        else:
            logger.warning(
                f'dropping all partitions in table {self.full_name}')
            parts=self.get_partitions(engine)
            for p in parts:
                cond_str=p.split('/')[0]
                sql=f"ALTER TABLE {self.full_name} DROP IF EXISTS PARTITION({cond_str})"
                engine.write(sql)

    def add_partitions(self, engine, *partition_values):
        '''
        Add partitions for an external table to the table meta data.

        Use case is adding partition to the meta data of an external table
        after new data has been added w/o the knowledge of the meta store.

        `*partition_values`: values of first partition columns; update below that.

        If empty, update under root location.
        '''
        assert self.s3external

        existing_partitions=self.get_partitions(engine, *partition_values)
        if not partition_values:
            root=self.location
        else:
            parts_path_str=self._partition_values_to_path(*partition_values)
            parts_cond_str=self._partition_values_to_condition(
                *partition_values)
            root=self.location + parts_path_str + '/'

        bucket=Bucket(self._s3_bucket_key)
        files=bucket.ls(root, recursive=True)

        if len(partition_values) == len(self.partitions):
            if list(files):
                sql=f'''ALTER TABLE {self.full_name} ADD PARTITION({parts_cond_str}) LOCATION '{self.location + parts_path_str}' '''
                logger.debug(sql)
                engine.write(sql)
            return

        dirs=set(v[: v.rfind('/')]
                   for v in files if '/' in v)  # no trailing '/'
        for dd in dirs:
            path_str=dd
            if partition_values:
                path_str=parts_path_str + '/' + path_str
            if path_str not in existing_partitions:
                cond_str=', '.join(f"{k}='{v}'" for k, v in (
                    v.split('=') for v in dd.split('/')))
                if partition_values:
                    cond_str=parts_cond_str + ', ' + cond_str
                sql=f'''ALTER TABLE {self.full_name} ADD PARTITION({cond_str}) LOCATION '{self.location + path_str}' '''
                logger.debug(sql)
                engine.write(sql)

    def update_partitions(self, engine, *partition_values):
        assert self.s3external
        self.drop_partitions(engine, *partition_values)
        self.add_partitions(engine, *partition_values)

    def partition_location(self, *partition_values):
        if not self.s3external:
            raise NotImplementedError
        if not partition_values:
            return self.location
        path=self.location + \
            self._partition_values_to_path(*partition_values) + '/'
        return path

    def purge_data(self, *partition_values) -> int:
        if not self.s3external:
            raise NotImplementedError
        bucket=Bucket(self._s3_bucket_key)
        path=self.partition_location(*partition_values)
        return bucket.delete_tree(path)


class HiveTable(HiveTableMixin):
    def __init__(self, location: str=None, **kwargs) -> None:
        if location:
            assert location.startswith('s3n://')
        super().__init__(**kwargs, location=location)

    def to_athena_table(self, db_name: str, tb_name: str=None) -> 'AthenaTable':
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
        from .athena import AthenaTable
        assert self.s3external
        location=self.location
        if location.startswith('s3n://'):
            location='s3://' + location[len('s3n://'):]
        return AthenaTable(
            db_name=db_name,
            tb_name=tb_name or self.tb_name,
            columns=self.columns,
            partitions=self.partitions,
            stored_as=self.stored_as,
            field_delimiter=self.field_delimiter,
            compression=self.compression,
            location=location)

    @ classmethod
    def from_athena_table(cls, table: 'AthenaTable', db_name: str, tb_name: str=None) -> 'HiveTable':
        '''
        Use case is analogous to `to_athena_table`.
        '''
        assert table.s3external
        location=table.location
        if location.startswith('s3://'):
            location='s3n://' + location[len('s3://'):]
        return cls(
            db_name=db_name,
            tb_name=tb_name or table.tb_name,
            columns=table.columns,
            partitions=table.partitions,
            stored_as=table.stored_as,
            field_delimiter=table.field_delimiter,
            compression=table.compression,
            location=location)


def make_udf(module_or_code: Union[ModuleType, str], *args, py_command: str='python') -> str:
    '''
    This function takes a Python module or a code string,
    encodes it into a byte string, which is suitable for transmission over the Internet,
    and returns a simple Python code snipplet that decodes and executes the original source code.

    Args:
        `module_or_code`: either a Python module *object* (not the module name),
            or a code block as a string.

            If the UDF is module `mypackage.udfs.udf`, then you may do

                import mypackage.udfs.udf
                s = make_udf(mypackage.udfs.udf)

            but not

                s = make_udf('mypackage.udfs.udf')

        `args`: if present, positional arguments to the UDF script.
            Each argument value should be a simple string or number (which will be converted to string)
            that does not contain any special characters (such as space, quotes) that
            could potentially cause trouble on the command-line.

            In this case, `module_or_code` processes a known number of arguments
            specified on the command-line before processing records coming from `stdin`.
            The number of args must be fixed; there can not be optional arguments with
            default values, because the UDF assumes data records begin after the known number
            of arguments.

    Suppose `s` is the output of this function, then it is used like this
    to construct a HiveQL statement:

        sql = f"""
            SELECT
                TRANSFORM ( ...input_columns... )
                USING '{s}'
                AS (...output_columns...)
            FROM {db_name}.{table_name}
        """

    (The part after `FROM` is often a subquery rather than a table directly.)

    An important detail is that this string (`s`) is wrapped by single quotes
    in the HiveQL statement, as shown above.

    If `module_or_code` is a UDAF rather than UDF, a `CLUSTER BY` clause is needed, like this:

        sql = f"""
            SELECT
                TRANSFORM (
                    ...input_columns_including_aggregation_columns...
                    )
                USING '{s}'
                AS (...output_columns...)
            FROM (
                SELECT
                    ...columns_including_aggregation_columns...
                FROM {db_name}.{table_name}
                CLUSTER BY ...aggregation_columns...
                ) AS t
        """

    This function suppose 4 scenarios:

        1. UDF
        2. UDAF
        3. UDF with arguments
        4. UDAF with arguments

    The difference between UDF and UDAF does not appear in this function, but rather
    in `module_or_code` and the HiveQL statements that use the output of this function
    (i.e. presence of `CLUSTER BY`).

    While the current function is written in Python 3.6+, the UDF `module_or_code`
    is written in the Python version that is installed on the Hive server.
    '''

    if inspect.ismodule(module_or_code):
        s=inspect.getsource(module_or_code)
    else:
        assert isinstance(module_or_code, str)
        s=module_or_code

    encoded=base64.urlsafe_b64encode(s.encode('utf-8'))

    assert py_command in ('python', 'python3')
    if py_command == 'python':  # 'python' is assumed to be Python 2 here.
        script='import sys, base64; code = sys.argv[1]; exec(base64.urlsafe_b64decode(code));'
    else:
        script='import sys, base64; code = sys.argv[1]; exec(base64.urlsafe_b64decode(code).decode());'

    code=f'{py_command} -c "{script}" {str(encoded)[2:-1]}'

    if args:
        code=code + ' ' + ' '.join(str(v) for v in args)

    return code