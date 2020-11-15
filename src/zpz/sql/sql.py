import logging
import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, asynccontextmanager
from typing import Union, Sequence, List, Type, Iterable

from boltons.iterutils import chunked_iter
import pandas as pd

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def _execute_(self, sql, **kwargs):
        logger.debug('executing SQL statement\n%s', sql)
        self._cursor.execute(sql, **kwargs)

    def _execute(self, sql: str, **kwargs) -> None:
        try:
            self._execute_(sql, **kwargs)
        except:
            msg = f'Failed to execute SQL\n{sql}'
            logger.exception(msg)
            raise

    def read(self, sql: str, **kwargs):
        self._execute(sql, **kwargs)
        return self

    def iterrows(self):
        """
        Iterator over rows in result after calling ``read``,
        one row at a time.
        """
        return iter(self._cursor)

    def iterbatches(self, batch_size: int):
        """
        This method is called after ``read`` to iter over results, one batch at a time.
        """
        while True:
            rows = self.fetchmany(batch_size)
            if rows:
                yield rows
            else:
                break

    @property
    def headers(self) -> List[str]:
        """
        Return column headers after calling ``read``.

        This can be used to augment the returns of `fetchone`, `fetchmany`, `fetchall`,
        which return values only, i.e. they do not return column headers.
        """
        return [x[0] for x in self._cursor.description]

    def fetchone(self) -> Union[Sequence[str], None]:
        """
        Fetch the next row of a query result set, returning a single sequence, 
        or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to 
        ``read`` did not produce any result set or no call to ``read`` was issued yet.
        """
        return self._cursor.fetchone()

    def fetchone_pandas(self):
        row = self.fetchone()
        if not row:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records([row], columns=self.headers)

    def fetchmany(self, n: int) -> Sequence[Sequence[str]]:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences
        (e.g. a list of tuples). 
        An empty sequence is returned when no more rows are available.

        An Error (or subclass) exception is raised if the previous call to 
        ``read`` did not produce any result set or no call to ``read`` was issued yet.
        """
        return self._cursor.fetchmany(n)

    def fetchmany_pandas(self, n: int):
        rows = self.fetchmany(n)
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(rows, columns=self.headers)

    def fetchall(self) -> Sequence[Sequence[str]]:
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a tuple of tuples). 
        Note that the cursor's arraysize attribute can affect the performance of this operation.

        An Error (or subclass) exception is raised if the previous call to 
        ``read`` did not produce any result set or no call to ``read`` was issued yet.
        """
        return self._cursor.fetchall()

    def fetchall_pandas(self):
        """
        This method is called after ``read`` to fetch the results as a ``pandas.DataFrame``.

        Warning: do not use this if the result contains a large number of rows.
        """
        rows = self.fetchall()
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(list(rows), columns=self.headers)

    def write(self, sql: str):
        self._execute(sql)


class ConnectionPool:
    def __init__(self,
                 connect_func,
                 maxsize: int,
                 connection_cls: Type[Connection]):
        self._connect_func = connect_func
        self._maxsize = maxsize
        self._connection_cls = connection_cls
        self._pool = []
        self.size = 0  # number of active connections

    @property
    def vacancy(self):
        # Number of currently usable connections.
        # Value is 0 to self._maxsize, inclusive.
        return self._maxsize - self.size + len(self._pool)

    @contextmanager
    def get_connection(self) -> Connection:
        if self.size < self._maxsize:
            if self._pool:
                conn = self._pool.pop()
            else:
                conn = self._connect_func()
                self.size += 1
        else:
            while not self._pool:
                time.sleep(0.07)
            conn = self._pool.pop()

        cursor = conn.cusor()
        try:
            yield self._connection_cls(cursor)
        finally:
            cursor.close()
            self._pool.append(conn)

    def close(self):
        for conn in self._pool:
            conn.close()

    def execute_stream(self,
                       x: Iterable,
                       func,
                       *,
                       batch_size,
                       log_every_n_batches: int = 1,
                       **kwargs):
        assert batch_size <= 10000
        futures = {}

        def callback(t):
            if t.cancelled():
                raise RuntimeError('Future object has been cancelled')
            e = t.exception()
            if e is not None:
                raise e
            del futures[id(t)]

        def do_one_batch(data, ibatch):
            if log_every_n_batches and (ibatch + 1) % log_every_n_batches == 0:
                verbose = True
                logger.info('  doing batch #%d', ibatch + 1)
            else:
                verbose = False
            with self.get_connection() as conn:
                _ = func(conn, data, **kwargs)
            if verbose:
                logger.info('  done batch #%d', ibatch + 1)

        with ThreadPoolExecutor(self._maxsize + 1) as executor:
            for ibatch, batch in enumerate(chunked_iter(x, batch_size)):
                while not self.vacancy:
                    time.sleep(0.017)
                t = executor.submit(do_one_batch, batch, ibatch)
                futures[id(t)] = t
                t.add_done_callback(callback)

        while futures:
            time.sleep(0.012)


class AsyncConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    async def _execute_(self, sql, **kwargs):
        logger.debug('executing SQL statement\n%s', sql)
        await self._cursor.execute(sql, **kwargs)

    async def _execute(self, sql: str, **kwargs) -> None:
        try:
            await self._execute_(sql, **kwargs)
        except:
            msg = f'Failed to execute SQL\n{sql}'
            logger.exception(msg)
            raise

    async def read(self, sql: str, **kwargs):
        await self._execute(sql, **kwargs)
        return self

    async def iterrows(self):
        return iter(self._cursor)

    async def iterbatches(self, batch_size: int):
        while True:
            rows = await self.fetchmany(batch_size)
            if rows:
                yield rows
            else:
                break

    @property
    def headers(self) -> List[str]:
        return [x[0] for x in self._cursor.description]

    async def fetchone(self) -> Union[Sequence[str], None]:
        return await self._cursor.fetchone()

    async def fetchone_pandas(self):
        row = await self.fetchone()
        if not row:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records([row], columns=self.headers)

    async def fetchmany(self, n: int) -> Sequence[Sequence[str]]:
        return await self._cursor.fetchmany(n)

    async def fetchmany_pandas(self, n: int):
        rows = await self.fetchmany(n)
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(rows, columns=self.headers)

    async def fetchall(self) -> Sequence[Sequence[str]]:
        return await self._cursor.fetchall()

    async def fetchall_pandas(self):
        rows = await self.fetchall()
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(list(rows), columns=self.headers)

    async def write(self, sql: str):
        await self._execute(sql)


class SQLClient(metaclass=ABCMeta):
    CONNECTION_CLASS: Type[Connection] = Connection
    ASYNCCONNECTION_CLASS: Type[AsyncConnection] = AsyncConnection
    CONNECTIONPOOL_CLASS: Type[ConnectionPool] = ConnectionPool

    @abstractmethod
    def connect(self):
        # This should return a connection object
        # w/o changing state of `self`.
        raise NotImplementedError

    @contextmanager
    def get_connection(self) -> Connection:
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield self.CONNECTION_CLASS(cursor)
        finally:
            cursor.close()
            conn.close()

    @contextmanager
    def get_connection_pool(self, maxsize: int) -> ConnectionPool:
        pool = self.CONNECTIONPOOL_CLASS(
            self.connect, maxsize, self.CONNECTION_CLASS)
        try:
            yield pool
        finally:
            pool.close()

    @abstractmethod
    async def a_connect(self):
        raise NotImplementedError

    @asynccontextmanager
    async def a_get_connection(self) -> AsyncConnection:
        conn = await self.a_connect()
        cursor = await conn.cursor()
        try:
            yield self.ASYNCCONNECTION_CLASS(cursor)
        finally:
            await cursor.close()
            await conn.close()
