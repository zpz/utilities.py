import logging
import textwrap
from typing import Union, Sequence, List, Tuple, Callable, Optional

from retrying import retry
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


class SQLReader:
    '''
    SQL client for a read-only engine.
    '''

    def __init__(self,
                 *,
                 conn_func: Callable,
                 cursor_args: dict = None,
                 cursor_arraysize: int = 10000,
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
        self._cursor_arraysize = cursor_arraysize

        self._connect()

    @property
    def user(self) -> Optional[str]:
        return self._conn_args.get('user')

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
        except Exception as e:
            args = {**self._conn_args}
            if 'password' in args:
                args['password'] = '[hidden]'
            host = self._conn_args.get('host', '<host>')
            port = self._conn_args.get('port', '<port>')
            logger.error(f'Failed to connect to server {host}:{port} with arguments {args}')
            logger.error(str(e))
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
            self._cursor.execute(sql, **kwargs)
        except:
            logger.error('Failed to execute:\n%s\nwith configuration: %s', sql, kwargs)
            raise

    def read(self, sql: str, **kwargs):
        self._execute(sql, **kwargs)
        return self

    def iterrows(self):
        """
        Iterator over rows in result after calling ``read``.

        Before ``_execute`` is ever run, using the iterator is an error.
        """
        return self._cursor

    def iterbatches(self, size=None):
        """
        This method is called after ``read`` to iter over results, one batch at a time.
        """
        while True:
            rows = self.fetchmany(size)
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

    def fetchone_pandas(self, simplify=False):
        row = self.fetchone()
        if not row:
            return pd.DataFrame(columns=self.headers)
        if simplify and len(row) == 1:
            return row[0]

        return pd.DataFrame.from_records([row], columns=self.headers)

    def fetchmany(self, n=None) -> Sequence[Sequence[str]]:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences
        (e.g. a list of tuples). 
        An empty sequence is returned when no more rows are available.

        An Error (or subclass) exception is raised if the previous call to 
        ``read`` did not produce any result set or no call to ``read`` was issued yet.
        """
        return self._cursor.fetchmany(n or self._cursor.arraysize)

    def fetchmany_pandas(self, n=None, simplify=False):
        rows = self.fetchmany(n)
        if not rows:
            return pd.DataFrame(columns=self.headers)
        if simplify and len(rows) == 1 and len(rows[0]) == 1:
            return rows[0][0]

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

    def fetchall_pandas(self, simplify=False):
        """
        This method is called after ``read`` to fetch the results as a ``pandas.DataFrame``.
         
        `simplify`: if `True`, then if the resultant ``pandas.DataFrame``
            contains only one element, the single element is returned as a scalar
            (as opposed to a ``1 X 1 pandas.DataFrame``).

        Warning: do not use this if the result contains a large number of rows.
        """
        rows = self.fetchall()
        if not rows:
            return pd.DataFrame(columns=self.headers)
        if simplify and len(rows) == 1 and len(rows[0]) == 1:
            return rows[0][0]

        return pd.DataFrame.from_records(list(rows), columns=self.headers)


class SQLClient(SQLReader):
    '''
    SQL client for both read and write.
    '''

    def write(self, sql: Union[str, List[str]], **kwargs):
        if isinstance(sql, str):
            sql = [sql]
        for ss in sql:
            self._execute(ss, **kwargs)
        return self
