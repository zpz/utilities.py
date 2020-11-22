import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
from typing import List, Iterable

import aioodbc
import MySQLdb as mysqlclient
from boltons.iterutils import chunked_iter

from .sql import SQLClient, Connection, AsyncConnection, ConnectionPool

logger = logging.getLogger(__name__)


class MysqlConnection(Connection):
    def get_tables(self) -> List[str]:
        z = self.read('SHOW TABLES').fetchall()
        return [v[0] for v in z]

    def describe_table(self, tb_name: str) -> None:
        z = self.read(f'DESCRIBE {tb_name}').fetchall_pandas()
        pprint(z)

    def insert_batch(self,
                     rows: Iterable[Iterable[str]],
                     *,
                     tb_name: str,
                     cols: List[str]) -> int:
        if hasattr(rows, '__len__'):
            assert len(rows) <= 10000

        columns_str = ", ".join(cols)
        symbol = "%s"
        val_place_holders = ", ".join([symbol] * (len(cols)))

        try:
            self._cursor.executemany(
                f"INSERT INTO {tb_name} ({columns_str}) VALUES ({val_place_holders})",
                rows,
            )
        except Exception as e:
            logger.exception(e)
            print('data rows:')
            for row in rows:
                print(row)
            raise

        return self._cursor.rowcount
        # This may not be the number of rows inserted.
        # The meaning depends on the MySQL client package.

    def table_rowcount(self, tb_name: str, exact: bool = True) -> int:
        if exact:
            sql = f"""SELECT COUNT(*) FROM {tb_name}"""
        else:
            sql = f"""SELECT table_rows FROM information_schema.tables WHERE table_name='{tb_name}' """
        z = self.read(sql).fetchall()
        return int(z[0][0])


class MysqlConnectionPool(ConnectionPool):
    def insert_stream(self,
                      rows: Iterable,
                      *,
                      tb_name: str,
                      cols: List[str],
                      batch_size: int,
                      **kwargs,
                      ):
        def func(conn, rows):
            return conn.insert_batch(
                rows, tb_name=tb_name, cols=cols)
        return self.execute_stream(rows, func,
                                   batch_size=batch_size, **kwargs)


class MysqlAsyncConnection(AsyncConnection):
    async def insert_batch(self,
                           rows: Iterable[Iterable[str]],
                           *,
                           tb_name: str,
                           cols: List[str]) -> int:
        if hasattr(rows, '__len__'):
            assert len(rows) <= 10000

        columns_str = ", ".join(cols)
        symbol = "?"
        val_place_holders = ",".join([symbol] * len(cols))

        try:
            await self._cursor.executemany(
                f"INSERT INTO {tb_name} ({columns_str}) VALUES ({val_place_holders})",
                rows,
            )
        except Exception as e:
            logger.exception(e)
            print('data rows:')
            for row in rows:
                print(row)
            raise

        return self._cursor.rowcount
        # This may not be the number of rows inserted.
        # The meaning depends on the MySQL client package.


class MySQL(SQLClient):
    CONNECTION_CLASS = MysqlConnection
    ASYNCCONNECTION_CLASS = MysqlAsyncConnection
    CONNECTIONPOOL_CLASS = MysqlConnectionPool

    def __init__(self,
                 *,
                 user: str,
                 password: str,
                 database: str,
                 host: str,       # MySQL server url
                 port: int = 3306,
                 ):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port

    def connect(self):
        conn = mysqlclient.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            db=self.database,
        )
        conn.autocommit(True)
        return conn

    async def a_connect(self):
        config = {
            'Driver': 'MySQL ODBC 8.0 Unicode Driver',
            'SERVER': self.host,
            'DATABASE': self.database,
            'UID': self.user,
            'PASSWORD': self.password,
            'timeout': 0,
            'autocommit': True,
        }
        dsn = ';'.join(f'{k}={v}' for k, v in config.items())
        return await aioodbc.connect(dsn=dsn, autocommit=True)
