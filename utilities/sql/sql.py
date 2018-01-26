'''
The main point of this module
is to split common SQL executions into two modes: *read* and *write*,
and provide abstract base classes for *reader* and *writer* objects
in order to regulate the interface.

The main differences between *read* and *write* include

1. Reader executes a single SQL statement whereas writer may execute a series of SQL statements.

2. Reader needs to get the output, and in certain useful formats,
   whereas writer in general does not care about the output.
'''

import abc
import logging
from typing import Callable, Tuple, Sequence

import pandas as pd

from .util import split_sql

logger = logging.getLogger(__name__)


class SQLExecutor(metaclass=abc.ABCMeta):
    def __init__(self,
                 *,
                 conn_func: Callable,
                 cursor_args: dict = None,
                 **conn_args) -> None:
        """
        Args:
            conn_func: function 'connect' of the appropriate class.

            cursor_args: keyword args to 'connection.cursor()'.
                In most cases this should be left as `None`.

            **conn_args: connection parameters, typically include
                'host', 'port', 'user', 'passwd', and so on.
                See specific subclasses for specifics.
        """
        if 'port' in conn_args:
            conn_args['port'] = int(conn_args['port'])
        self._conn_func = conn_func
        self._conn_args = conn_args
        self._user = conn_args['user']
        if cursor_args is None:
            cursor_args = {}
        self._cursor_args = cursor_args
        self._connect()

    @property
    def user(self) -> str:
        return self._user

    def _connect(self) -> None:
        try:
            self._conn = self._conn_func(**self._conn_args)
        except:
            logger.error('Failed to connect to server with arguments %s' % str(
                self._conn_args))
            raise
        try:
            self._cursor = self._conn.cursor(**self._cursor_args)
        except:
            logger.error(
                'Failed to create cursor on connection with arguments %s' %
                str(self._cursor_args))
            raise

        self._cursor.arraysize = 100000

    def _execute(self, sql) -> None:
        """Execute a single SQL statement."""
        try:
            self._cursor.execute(sql)
        except:
            logger.error('Failed to execute:\n%s', sql)
            raise

    def close(self) -> None:
        try:
            self._cursor.close()
            self._conn.close()
        except:
            pass

    def __del__(self) -> None:
        self.close()


class SQLReader(SQLExecutor, metaclass=abc.ABCMeta):
    def execute(self, sql: str):
        assert len(split_sql(sql)) == 1
        self._execute(sql)
        return self

    @property
    def has_result(self):
        """
        There is no reliable way to determine this.
        """
        return (self._cursor.rowcount > 0 or self._cursor.rowcount is None
                or self._cursor.rowcount == -1)

    def fetchall(self) -> Tuple[Sequence[str], Sequence[Sequence]]:
        """
        This method is called after ``execute`` to fetch the results.

        Returns:
            A tuple with ``headers`` and ``rows``, where
            ``headers`` is a list of column names, and
            ``rows`` is a tuple of tuples (rows).
        """
        if self.has_result:
            rows = self._cursor.fetchall()
            headers = [x[0] for x in self._cursor.description]
        else:
            rows = []
            headers = []
        return headers, rows

    def fetchall_pandas(self):
        """
        This method is called after ``execute`` to fetch the results as a ``pandas.DataFrame``.
        """
        if self.has_result:
            names = [metadata[0] for metadata in self._cursor.description]
            rows = []
            while True:
                r = self._cursor.fetchmany()
                if r:
                    rows.extend(r)
                else:
                    break
            nrows = len(rows)
            if nrows > self._cursor.arraysize:
                logger.debug('downloaded %d rows', nrows)
            return pd.DataFrame.from_records(rows, columns=names)
        return pd.DataFrame()

    def rowcount(self, sql: str = None) -> int:
        """
        Args:
            sql: if ``None``, return the row count of the last SQL statement executed.
                if not ``None``, a table name or a ``SELECT`` statement that would return
                table-like content; a ``SELECT COUNT(*)`` statement is constructed and
                executed.
        """
        if sql:
            if ' ' in sql or '\n' in sql:
                # A 'SELECT' statement
                sql = '(' + sql + ')'
            return self.__call__('SELECT COUNT(*) FROM {} AS t'.format(sql))
        else:
            return self._cursor.rowcount

    def read(self, sql: str) -> Tuple[Sequence[str], Sequence[Sequence]]:
        return self.execute(sql).fetchall()

    def read_pandas(self, sql: str):
        return self.execute(sql).fetchall_pandas()

    def __call__(self, sql: str):
        """
        ``obj(sql)`` is a shortcut to ``obj.read_pandas(sql)``
        with a little more clean-up, that is, if the resultant ``pandas.DataFrame``
        contains only one element, the single element is returned as a scalar
        (as opposed to a ``1 X 1 pandas.DataFrame``).

        This usage is meant primarily for interactive explorations.
        """
        v = self.read_pandas(sql)
        if v.size == 1:
            return v.iloc[0, 0]
        return v

    def __iter__(self):
        """
        Iterator over rows in result after calling ``execute``.

        Prior to ``execute``, the iterator is empty but not an error.
        """

        return self._cursor


class SQLWriter(SQLExecutor, metaclass=abc.ABCMeta):
    def execute(self, sql: str):
        """
        Args:
            sql: a single SQL statement or a list thereof.
        """
        if isinstance(sql, str):
            sql = split_sql(sql)
        sql = list(sql)
        for s in sql:
            self._execute(s)
        return self

    def write(self, sql: str) -> None:
        """
        TODO: return some info about the write operation.
        """
        self.execute(sql)

    def __call__(self, sql) -> None:
        return self.write(sql)
