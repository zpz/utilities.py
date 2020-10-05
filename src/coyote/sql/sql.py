import logging
import time
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import Union, Sequence, List, Callable, Type

import pandas as pd

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def _execute_(self, sql):
        logger.debug('executing SQL statement\n%s', sql)
        self._cursor.execute(sql)

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
        return self._cursor.fetchmany(n or self._cursor.arraysize)

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
                 connect_func: Callable,
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


class SQLClient(metaclass=ABCMeta):
    CONNECTION_CLASS: Type[Connection] = Connection

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
        pool = ConnectionPool(self.connect, maxsize, self.CONNECTION_CLASS)
        try:
            yield pool
        finally:
            pool.close()

