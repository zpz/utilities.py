import logging
from typing import List, Tuple

import psycopg2

from .sql import SQLClient

logger = logging.getLogger(__name__)


class Postgres(SQLClient):
    def __init__(self, *, host, user, password, dbname, port=5432, **kwargs):
        super().__init__(
            conn_func=psycopg2.connect,
            host=host,
            user=user,
            password=password,
            dbname=dbname,
            port=int(port),
            **kwargs)

    @property
    def closed(self):
        return self._conn.closed

    @property
    def connected(self):
        return not self.closed

    def reconnect(self):
        if self.closed:
            self._connect()

    def rollback(self):
        '''Call this after `write` but before `commit` to cancel the write.'''
        self._conn.rollback()

    def commit(self):
        '''Call this after `write`.'''
        self._conn.commit()

    def get_databases(self) -> List[str]:
        sql = "SELECT datname FROM pg_database WHERE datistemplate = false"
        headers, rows = self.read(sql).fetchall()
        return [v[0] for v in rows]

    def get_tables(self) -> List[str]:
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        rows = self.read(sql).fetchall()
        return [v[0] for v in rows]

    def has_table(self, tb_name):
        # A 'lazy' solution
        # return tb_name in self.get_tables()

        sql = "SELECT exists(SELECT relname FROM pg_class WHERE relname = '{}')".format(
            tb_name)
        return self.read(sql).fetchall()

    def get_table_schema(
            self, tb_name) -> Tuple[List[str], List[Tuple[str, str, str]]]:
        '''
        Returns: tuple with
            ['column_name', 'data_type', 'is_nullable'],
            [[column_name, data_type, is_nullable] for each field]
        '''
        sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{tb_name}'
        """.format(tb_name=tb_name)
        return self.read(sql).fetchall()

    def get_table_columns(self, tb_name) -> List[str]:
        sql = "SELECT column_name from information_schema.columns WHERE table_name = '{}'".format(
            tb_name)
        rows = self.read(sql).fetchall()
        return [v[0] for v in rows]