import logging
from typing import Sequence, Tuple

from impala import dbapi

from .sql import SQLReader, SQLWriter

logging.getLogger('impala.hiveserver2').setLevel(logging.WARNING)
logging.getLogger('impala._thrift_api').setLevel(logging.WARNING)


class HiveReaderMixin:
    @property
    def has_result(self) -> bool:
        return self._cursor.has_result_set

    def get_databases(self) -> Sequence[str]:
        self._cursor.get_databases()
        return [v[0] for v in self._cursor.fetchall()]

    def has_database(self, db_name: str) -> bool:
        return self._cursor.database_exists(db_name)

    def get_tables(self, db_name: str) -> Sequence[str]:
        self._cursor.get_tables(db_name)
        return [v[2] for v in self._cursor.fetchall()]

    def has_table(self, db_name: str, tb_name: str) -> bool:
        return self._cursor.table_exists(tb_name, db_name)

    def get_table_schema(self, db_name: str,
                         tb_name: str) -> Sequence[Tuple[str, str]]:
        return self._cursor.get_table_schema(tb_name, db_name)


class HiveReader(SQLReader, HiveReaderMixin):
    def __init__(self, *, host, port, user, auth_mechanism, **kwargs):
        super().__init__(
            conn_func=dbapi.connect,
            cursor_args={'user': user},
            host=host,
            port=port,
            user=user,
            auth_mechanism=auth_mechanism,
            **kwargs)


class HiveWriterMixin:
    pass


class HiveWriter(SQLWriter, HiveWriterMixin):
    def __init__(self, *, host, port, user, auth_mechanism, **kwargs):
        super().__init__(
            conn_func=dbapi.connect,
            cursor_args={'user': user},
            host=host,
            port=port,
            user=user,
            auth_mechanism=auth_mechanism,
            **kwargs)
