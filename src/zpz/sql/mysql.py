import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pprint import pprint
from typing import List, Iterable, ContextManager

import aioodbc
import MySQLdb as mysqlclient
from boltons.iterutils import chunked_iter

from .sql import SQLClient, Connection, AsyncConnection
from ..mp import MAX_THREADS

logger = logging.getLogger(__name__)


class MysqlConnection(Connection):
    def get_tables(self) -> List[str]:
        z = self.read('SHOW TABLES').fetchall()
        return [v[0] for v in z]

    def describe_table(self, tb_name: str) -> None:
        z = self.read(f'DESCRIBE {tb_name}').fetchall_pandas()
        pprint(z)

    def table_rowcount(self, tb_name: str, exact: bool = True) -> int:
        if exact:
            sql = f"""SELECT COUNT(*) FROM {tb_name}"""
        else:
            sql = f"""SELECT table_rows FROM information_schema.tables WHERE table_name='{tb_name}' """
        z = self.read(sql).fetchall()
        return int(z[0][0])

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


class MysqlConnectionPool:
    def __init__(self,
                 mysql_obj,
                 maxsize: int,
                 ):
        self._mysql_obj = mysql_obj
        self._maxsize = maxsize or MAX_THREADS
        self._pool = []
        self.size = 0  # number of active connections

    @property
    def vacancy(self) -> int:
        # Number of currently usable connections.
        # Value is 0 to self._maxsize, inclusive.
        return self._maxsize - self.size + len(self._pool)

    @contextmanager
    def get_connection(self) -> ContextManager[MysqlConnection]:
        if self.size < self._maxsize:
            if self._pool:
                conn = self._pool.pop()
            else:
                conn = self._mysql_obj.connect()
                cursor = conn.cursor()
                self.size += 1
        else:
            while not self._pool:
                time.sleep(0.07)
            conn, cursor = self._pool.pop()

        try:
            yield self._mysql_obj.CONNECTION_CLASS(conn, cursor)
        finally:
            self._pool.append((conn, cursor))

    def close(self) -> None:
        for conn, cursor in self._pool:
            cursor.close()
            conn.close()

    def insert_stream(self,
                      rows: Iterable,
                      *,
                      tb_name: str,
                      cols: List[str],
                      batch_size: int,
                      log_every_n_batches: int = 1,
                      ) -> int:
        assert batch_size <= 10000
        futures = {}
        n_inserted = 0

        def callback(t):
            if t.cancelled():
                raise RuntimeError('Future object has been cancelled')
            e = t.exception()
            if e is not None:
                raise e
            del futures[id(t)]

        def insert_one_batch(data, ibatch):
            if log_every_n_batches and (ibatch + 1) % log_every_n_batches == 0:
                verbose = True
                logger.info('  inserting batch #%d', ibatch + 1)
            else:
                verbose = False
            with self.get_connection() as conn:
                n = conn.insert_batch(data, tb_name=tb_name, cols=cols)
            if verbose:
                logger.info('  inserted batch #%d', ibatch + 1)
            nonlocal n_inserted
            n_inserted += n

        with ThreadPoolExecutor(self._maxsize + 1) as executor:
            for ibatch, batch in enumerate(chunked_iter(rows, batch_size)):
                while not self.vacancy:
                    time.sleep(0.017)
                t = executor.submit(insert_one_batch, batch, ibatch)
                futures[id(t)] = t
                t.add_done_callback(callback)

        while futures:
            time.sleep(0.012)
        return n_inserted


class MysqlAsyncConnection(AsyncConnection):
    async def get_tables(self) -> List[str]:
        await self.read('SHOW TABLES')
        z = await self.fetchall()
        return [v[0] for v in z]

    async def describe_table(self, tb_name: str) -> None:
        await self.read(f'DESCRIBE {tb_name}')
        z = await self.fetchall_pandas()
        pprint(z)

    async def table_rowcount(self, tb_name: str, exact: bool = True) -> int:
        if exact:
            sql = f"""SELECT COUNT(*) FROM {tb_name}"""
        else:
            sql = f"""SELECT table_rows FROM information_schema.tables WHERE table_name='{tb_name}' """
        await self.read(sql)
        z = await self.fetchall()
        return int(z[0][0])

    def insert_batch(self,
                     rows: Iterable[Iterable[str]],
                     *,
                     tb_name: str,
                     cols: List[str]) -> int:
        # We do not enforce `rows` tobe a Sequence.
        # But, don't make it too long.
        if hasattr(rows, '__len__'):
            assert len(rows) <= 10000

        columns_str = ", ".join(cols)
        val_place_holders = ",".join(["?"] * (len(cols)))

        try:
            await self._cursor.executemany(
                f"INSERT INTO {tb_name} ({columns_str}) VALUES ({val_place_holders})",
                rows,
            )
        except Exception as e:
            logger.exception(e)
            print('data rows:')
            for row in rows:  # This may be useless if `rows` is not a list.
                print(row)
            raise

        return self._cursor.rowcount
        # This may not be the number of rows inserted.
        # The meaning depends on the MySQL client package.


class MySQL(SQLClient):
    CONNECTION_CLASS = MysqlConnection
    ASYNCCONNECTION_CLASS = MysqlAsyncConnection

    def __init__(self,
                 *,
                 user: str,
                 password: str,
                 database: str,
                 host: str,       # MySQL server url
                 port: int = 3306,
                 autocommit: bool = True,
                 ):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.autocommit = autocommit

    def connect(self):
        conn = mysqlclient.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            db=self.database,
        )
        conn.autocommit(self.autocommit)
        return conn

    async def a_connect(self):
        config = {
            'Driver': 'MySQL ODBC 8.0 Unicode Driver',
            'SERVER': self.host,
            'DATABASE': self.database,
            'UID': self.user,
            'PASSWORD': self.password,
            'timeout': 0,
            'autocommit': self.autocommit,
        }
        dsn = ';'.join(f'{k}={v}' for k, v in config.items())
        return await aioodbc.connect(dsn=dsn, autocommit=self.autocommit)

    @contextmanager
    def get_connection_pool(self, maxsize: int) -> ContextManager[MysqlConnectionPool]:
        pool = MysqlConnectionPool(self, maxsize)
        try:
            yield pool
        finally:
            pool.close()
