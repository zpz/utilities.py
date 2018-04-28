import logging
import textwrap
from typing import Union, Sequence, List, Tuple, Callable

import sqlparse
import pandas as pd

logger = logging.getLogger(__name__)


def cleanse_sql(x):
    return x.strip().strip('\n').strip().strip(';').strip()


def split_sql(sql: str) -> Sequence[str]:
    """
    Split a sequence of SQL statements into individual statements.

    The input is a block of text formatted like ::

        CREATE TABLE ...
        ;
        INSERT INTO ...
        SELECT ...
        ;
        DROP TABLE ...


    ``;\\n`` is treated as the separator between SQL statements.

    For each single SQL statement in the resultant list,
    trailing ';', line break, and spaces are removed;
    leading ';' and line break are removed;
    leading spaces are not removed b/c that may be part of intentional formatting
    for alignment.
    """
    sql.replace('\n\n', '\n')
    z = [cleanse_sql(x) for x in cleanse_sql(sql).split(';\n') if x]
    return [x for x in z if x]


def pretty_sql(sql: str, lpad: str = '        ', keyword_case=None) -> str:
    """
    Convert a SQL statement(s) into readable format (nicely line-split, indented, and aligned)
    suitable for printing and inspection.
    """
    assert keyword_case in (None, 'upper', 'lower')
    sql = sqlparse.format(
        cleanse_sql(sql),
        reindent=True,
        keyword_case=keyword_case,
        strip_whitespace=True,
    )
    sql = sql.lstrip('\n').rstrip(' \n').replace('\n\n\n', '\n\n')
    if lpad:
        sql = textwrap.indent(sql, lpad)
    return sql


class SQLClient:
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
                'port', 'user', 'password', and so on.
                See specific subclasses for specifics.
        """
        if 'port' in conn_args:
            conn_args['port'] = int(conn_args['port'])
        self._conn_func = conn_func
        self._conn_args = conn_args
        if cursor_args is None:
            cursor_args = {}
        self._cursor_args = cursor_args
        self._cursor_arraysize = 100000

        self._connect()

        self._user = conn_args['user']

    @property
    def user(self) -> str:
        return self._user

    def _create_cursor(self):
        try:
            self._cursor = self._conn.cursor(**self._cursor_args)
            self._cursor.arraysize = self._cursor_arraysize
        except:
            logger.error(
                'Failed to create cursor on connection with arguments %s',
                str(self._cursor_args))
            raise

    def _connect(self) -> None:
        try:
            self._conn = self._conn_func(**self._conn_args)
        except:
            logger.error('Failed to connect to server with arguments %s' % str(
                self._conn_args))
            raise
        self._create_cursor()

    def close(self) -> None:
        try:
            self._cursor.close()
            self._conn.close()
        except:
            pass

    def __del__(self) -> None:
        self.close()

    def _execute(self, sql: str, **kwargs) -> None:
        '''Execute a single SQL statement.'''
        assert isinstance(sql, str)
        try:
            logger.debug('Executing SQL statement:\n%s\nwith args: %s',
                         pretty_sql(sql), kwargs)
            self._cursor.execute(sql, **kwargs)
        except:
            logger.error(
                'Failed to execute:\n%s\nwith configuration: %s\nraw statement:\n%s',
                pretty_sql(sql), kwargs, sql)
            raise

    def read(self, sql: str, **kwargs):
        self._execute(sql, **kwargs)
        return self

    def __iter__(self):
        """
        Iterator over rows in result after calling ``read``.

        Before ``_execute`` is ever run, using the iterator is an error.
        """
        return self._cursor

    def fetchall(self) -> Tuple[Sequence[str], Sequence[Sequence]]:
        """
        This method is called after ``read`` to fetch the results.

        Returns:
            A tuple with ``headers`` and ``rows``, where
            ``headers`` is a list of column names, and
            ``rows`` is a tuple of tuples (rows).
        """
        try:
            rows = self._cursor.fetchall()
            headers = [x[0] for x in self._cursor.description]
            return headers, rows
        except:
            # TODO: log some error message?
            return [], []

    def fetchall_pandas(self, simplify=True):
        """
        This method is called after ``read`` to fetch the results as a ``pandas.DataFrame``.
         
        `simplify`: if `True`, then if the resultant ``pandas.DataFrame``
            contains only one element, the single element is returned as a scalar
            (as opposed to a ``1 X 1 pandas.DataFrame``).
        """
        try:
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
            v = pd.DataFrame.from_records(rows, columns=names)
            if simplify:
                if v.size == 1:
                    v = v.iloc[0, 0]
            return v
        except:
            # TODO: log some error message?
            return pd.DataFrame()

    def write(self, sql: Union[str, List[str]], **kwargs):
        if isinstance(sql, str):
            sql = split_sql(sql)
        for s in sql:
            self._execute(s, **kwargs)
        return self
