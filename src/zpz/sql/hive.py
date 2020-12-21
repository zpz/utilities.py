import itertools
import logging
from typing import List, Tuple

import aioodbc
import pyodbc

from .sql import SQLClient, Connection, AsyncConnection


logger = logging.getLogger(__name__)


def _sql_create_table(
        db_name: str,
        tb_name: str,
        cols: List[Tuple[str, str]],
        *,
        partitions: List[Tuple[str, str]] = None,
        stored_as: str = 'ORC',
        field_delimiter: str = '\\t',
        compression: str = 'ZLIB',
        external: bool = False,
        external_location: str = None,
) -> str:
    def collapse(spec):
        return ', '.join(name + ' ' + type_.upper() for (name, type_) in spec)

    columns = collapse(cols)

    if partitions:
        partitions = [(name, type_.upper()) for (name, type_) in partitions]
        partitions = f"PARTITIONED BY ({collapse(partitions)})"
    else:
        partitions = ''

    if external:
        location = f"LOCATION '{external_location}'"
        external = 'EXTERNAL'
    else:
        location = ''
        external = ''

    stored_as = stored_as.upper()
    assert stored_as in ('ORC', 'PARQUET', 'TEXTFILE')
    if stored_as in ('ORC', 'PARQUET'):
        stored_as = f'''
            STORED AS {stored_as}
            {location}
            TBLPROPERTIES ('{stored_as.lower()}.compress' = '{compression}')
            '''
    else:
        stored_as = f'''
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '{field_delimiter}'
            STORED AS {stored_as}
            {location}
            '''

    sql = f'''
        CREATE {external} TABLE {db_name}.{tb_name}
        ({columns})
        {partitions}
        {stored_as}
        '''

    if external:
        sql = sql + ';\n' + f'MSCK REPAIR TABLE {db_name}.{tb_name}'

    return sql


def _sql_insert_batch(db_name: str,
                      tb_name: str,
                      rows: List[Tuple],
                      *,
                      partitions: List[str] = None,
                      overwrite: bool = False,
                      ):
    if partitions:
        parts = ', '.join(partitions)
        parts = f'PARTITION({parts})'
    else:
        parts = ''
    sql = f'''
        INSERT {'OVERWRITE' if overwrite else 'INTO'} TABLE {db_name}.{tb_name}
        {parts}
        SELECT * FROM
        (
            SELECT STACK (
                {len(rows)},
                {str(list(itertools.chain.from_iterable(rows)))[1:-1]}
            )
        ) s
        '''
    # This is for older versions of Hive.
    # Since 0.14, a nice syntax is
    #     INSERT INTO TABLE xxx VALUES (col1, col2), (col1, col2)

    return sql


def _sql_sample_table(db_name: str, tb_name: str, n: int = 5) -> str:
    if n > 0:
        sql = f"SELECT * FROM {db_name}.{tb_name} LIMIT {n}"
    else:
        sql = f"SELECT * FROM {db_name}.{tb_name}"
    return sql


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

    def drop_table(self, db_name: str, tb_name: str) -> None:
        self.write(f'DROP TABLE IF EXISTS {db_name}.{tb_name}')

    def create_table(self, db_name: str, tb_name: str, *,
                     drop_if_exists: bool = False, **kwargs) -> None:
        sql = _sql_create_table(db_name=db_name, tb_name=tb_name, **kwargs)
        if drop_if_exists:
            self.drop_table(db_name, tb_name)
        self.write(sql)

    def insert_batch(self, *args, **kwargs) -> None:
        sql = _sql_insert_batch(*args, **kwargs)
        self.write(sql)

    def sample_table(self, *args, **kwargs) -> Tuple[Tuple]:
        sql = _sql_sample_table(*args, **kwargs)
        return self.read(sql).fetchall()


class HiveAsyncConnection(AsyncConnection):
    async def get_databases(self) -> List[str]:
        await self.read('SHOW DATABASES')
        z = await self.fetchall()
        return [v[0] for v in z]

    async def get_tables(self, db_name: str) -> List[str]:
        await self.read(f'SHOW TABLES IN {db_name}')
        z = await self.fetchall()
        return [v[0] for v in z]

    async def drop_table(self, db_name: str, tb_name: str) -> None:
        await self.write(f'DROP TABLE IF EXISTS {db_name}.{tb_name}')

    async def create_table(
            self, db_name: str, tb_name: str, *,
            drop_if_exists: bool = False, **kwargs) -> None:
        sql = _sql_create_table(db_name=db_name, tb_name=tb_name, **kwargs)
        if drop_if_exists:
            await self.drop_table(db_name, tb_name)
        await self.write(sql)

    async def insert_batch(self, *args, **kwargs) -> None:
        sql = _sql_insert_batch(*args, **kwargs)
        await self.write(sql)

    async def sample_table(self, *args, **kwargs) -> Tuple[Tuple]:
        sql = _sql_sample_table(*args, **kwargs)
        await self.read(sql)
        return await self.fetchall()


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
            **{f'SSP_{k}': f'{{{{v}}}}' for k, v in self._configuration.items()}
        }
        return ';'.join(f'{k} = {v}' for k, v in conn_args.items())

    def connect(self):
        conn_str = self.connection_string
        conn = pyodbc.connect(conn_str, autocommit=True)
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn.setencoding(encoding='utf-8')

        # See https://github.com/mkleehammer/pyodbc/issues/194
        conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-32le')

        logger.info('connected to Hive server @%s', self.host)
        return conn

    async def a_connect(self):
        conn_str = self.connection_string
        conn = await aioodbc.connect(dsn=conn_str, autocommit=True)
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn.setencoding(encoding='utf-8')

        # See https://github.com/mkleehammer/pyodbc/issues/194
        conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-32le')

        logger.info('connected to Hive server @%s', self.host)
        return conn
