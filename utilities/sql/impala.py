import pandas as pd
from impala.util import as_pandas
from impala import dbapi

from .sql import SQLReader, SQLWriter
from .hive import HiveReaderMixin, HiveWriterMixin
from .util import pretty_sql


def _refresh(self, table_name=None):
    if table_name:
        self.execute('REFRESH ' + table_name)
    else:
        self.execute('INVALIDATE METADATA')


class ImpalaReader(SQLReader, HiveReaderMixin):
    def __init__(self, *, host, port, user, **kwargs):
        sper().__init__(
            conn_func=dbapi.connect,
            cursor_args={'user': user},
            host=host,
            port=port,
            user=user,
            **kwargs)

    def fetchall_pandas(self):
        if self.has_result:
            return as_pandas(self._cursor)
        return pd.DataFrame()

    refresh = _refresh


class ImpalaWriter(SQLWriter, HiveWriterMixin):
    def __init__(self, *, host, port, user, **kwargs):
        super().__init__(
            conn_func=dbapi.connect,
            cursor_args={'user': user},
            host=host,
            port=port,
            user=user,
            **kwargs)
        self._cursor.execute('set sync_ddl=true')

    refresh = _refresh
