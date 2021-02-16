import logging
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager, asynccontextmanager
from typing import (
    Union, Sequence, List, Tuple,
    Type, ContextManager, AsyncContextManager,
    Iterator, AsyncIterator,
)

import pandas as pd
import aioodbc
import MySQLdb.connections

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, conn, cursor):
        self._conn = conn
        self._cursor = cursor
        if isinstance(conn, MySQLdb.connections.Connection):
            self._autocommit = conn.get_autocommit()
        else:
            self._autocommit = conn.autocommit

    def commit(self) -> None:
        assert not self._autocommit
        self._conn.commit()

    def rollback(self) -> None:
        assert not self._autocommit
        self._conn.rollback()

    def _execute_(self, sql, *args, **kwargs) -> None:
        logger.debug('executing SQL statement\n%s\nargs:\n%s\nkwargs:\n%s',
                     sql, str(args), str(kwargs))
        self._cursor.execute(sql, *args, **kwargs)

    def _execute(self, sql: str, *args, **kwargs) -> None:
        try:
            self._execute_(sql, *args, **kwargs)
        except:
            msg = f'Failed to execute SQL\n{sql}\nargs:\n{args}\nkwargs:\n{kwargs}'
            logger.exception(msg)
            raise

    def read(self, sql: str, *args, **kwargs) -> 'Connection':
        self._execute(sql, *args, **kwargs)
        return self

    def iterrows(self) -> Iterator[Tuple]:
        """
        Iterate over rows in result after calling ``read``,
        one row at a time.
        """
        return iter(self._cursor)

    def iterbatches(self, batch_size: int) -> Iterator[List[Tuple]]:
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

    def fetchone(self) -> Union[Tuple, None]:
        """
        Fetch the next row of a query result set, returning a single sequence, 
        or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to 
        ``read`` did not produce any result set or no call to ``read`` was issued yet.
        """
        return self._cursor.fetchone()

    def fetchone_pandas(self) -> pd.DataFrame:
        row = self.fetchone()
        if not row:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records([row], columns=self.headers)

    def fetchmany(self, n: int) -> Sequence[Tuple]:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences
        (e.g. a list of tuples). 
        An empty sequence is returned when no more rows are available.

        An Error (or subclass) exception is raised if the previous call to 
        ``read`` did not produce any result set or no call to ``read`` was issued yet.
        """
        return self._cursor.fetchmany(n)

    def fetchmany_pandas(self, n: int) -> pd.DataFrame:
        rows = self.fetchmany(n)
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(rows, columns=self.headers)

    def fetchall(self) -> List[Tuple]:
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a tuple of tuples). 
        Note that the cursor's arraysize attribute can affect the performance of this operation.

        An Error (or subclass) exception is raised if the previous call to 
        ``read`` did not produce any result set or no call to ``read`` was issued yet.
        """
        return self._cursor.fetchall()

    def fetchall_pandas(self) -> pd.DataFrame:
        """
        This method is called after ``read`` to fetch the results as a ``pandas.DataFrame``.

        Warning: do not use this if the result contains a large number of rows.
        """
        rows = self.fetchall()
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(list(rows), columns=self.headers)

    def write(self, sql: str, *args, **kwargs) -> None:
        self._execute(sql, *args, **kwargs)


class AsyncConnection:
    def __init__(self,
                 conn: aioodbc.connection.Connection,
                 cursor: aioodbc.cursor.Cursor):
        self._conn = conn
        self._cursor = cursor
        self._autocommit = conn.autocommit

    async def commit(self) -> None:
        assert not self._autocommit
        await self._conn.commit()

    async def rollback(self) -> None:
        assert not self._autocommit
        await self._conn.rollback()

    async def _execute_(self, sql, *args, **kwargs):
        logger.debug('executing SQL statement:\n%s\nargs:\n%s\nkwargs:\n%s',
                     sql, args, kwargs)
        await self._cursor.execute(sql, *args, **kwargs)

    async def _execute(self, sql: str, *args, **kwargs) -> None:
        try:
            await self._execute_(sql, *args, **kwargs)
        except:
            msg = f'Failed to execute SQL:\n{sql}\nargs:\n{args}\nkwargs:\n{kwargs}'
            logger.exception(msg)
            raise

    async def read(self, sql: str, *args, **kwargs) -> 'AsyncConnection':
        await self._execute(sql, *args, **kwargs)
        return self

    async def iterrows(self) -> AsyncIterator[Tuple]:
        return iter(self._cursor)

    async def iterbatches(self, batch_size: int) -> AsyncIterator[List[Tuple]]:
        while True:
            rows = await self.fetchmany(batch_size)
            if rows:
                yield rows
            else:
                break

    @property
    def headers(self) -> List[str]:
        return [x[0] for x in self._cursor.description]

    async def fetchone(self) -> Union[Tuple, None]:
        return await self._cursor.fetchone()

    async def fetchone_pandas(self) -> pd.DataFrame:
        row = await self.fetchone()
        if not row:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records([row], columns=self.headers)

    async def fetchmany(self, n: int) -> List[Tuple]:
        return await self._cursor.fetchmany(n)

    async def fetchmany_pandas(self, n: int) -> pd.DataFrame:
        rows = await self.fetchmany(n)
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(rows, columns=self.headers)

    async def fetchall(self) -> List[Tuple]:
        return await self._cursor.fetchall()

    async def fetchall_pandas(self) -> pd.DataFrame:
        rows = await self.fetchall()
        if not rows:
            return pd.DataFrame(columns=self.headers)
        return pd.DataFrame.from_records(list(rows), columns=self.headers)

    async def write(self, sql: str, *args, **kwargs) -> None:
        await self._execute(sql, *args, **kwargs)


class SQLClient(metaclass=ABCMeta):
    CONNECTION_CLASS: Type[Connection] = Connection
    ASYNCCONNECTION_CLASS: Type[AsyncConnection] = AsyncConnection

    @abstractmethod
    def connect(self):
        # This should return a connection object
        # w/o changing state of `self`.
        #
        # The connection type is either `MySQLdb.connections.Connection`
        # or a `pyodbc` connection class (I couldn't find its doc).
        raise NotImplementedError

    @contextmanager
    def get_connection(self) -> ContextManager[Connection]:
        # Typical usage:
        #
        #   obj = Sql(...)
        #   with obj.get_connection() as conn:
        #     conn.read(...)
        #     x = conn.fetchall()
        #     conn.write(...)
        #     ...
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield self.CONNECTION_CLASS(conn, cursor)
        finally:
            # if conn.is_connected(): # ?
            cursor.close()
            conn.close()

    @abstractmethod
    async def a_connect(self) -> aioodbc.Connection:
        raise NotImplementedError

    @asynccontextmanager
    async def a_get_connection(self) -> AsyncContextManager[AsyncConnection]:
        conn = await self.a_connect()
        cursor = await conn.cursor()
        try:
            yield self.ASYNCCONNECTION_CLASS(conn, cursor)
        finally:
            # if conn.is_connected(): # ?
            await cursor.close()
            await conn.close()
