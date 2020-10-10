import logging
from typing import List, Tuple

import psycopg2

from .sql import SQLClient, Connection

logger = logging.getLogger(__name__)


class PgConnection(Connection):
    def get_databases(self) -> List[str]:
        sql = "SELECT datname FROM pg_database WHERE datistemplate = false"
        headers, rows = self.read(sql).fetchall()
        return [v[0] for v in rows]

    def get_tables(self) -> List[str]:
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        rows = self.read(sql).fetchall()
        return [v[0] for v in rows]

    def has_table(self, tb_name: str):
        # A 'lazy' solution
        # return tb_name in self.get_tables()

        sql = "SELECT exists(SELECT relname FROM pg_class WHERE relname = '{}')".format(
            tb_name)
        return self.read(sql).fetchall()

    def get_table_schema(
            self, tb_name: str) -> Tuple[List[str], List[Tuple[str, str, str]]]:
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

    def get_table_columns(self, tb_name: str) -> List[str]:
        sql = "SELECT column_name from information_schema.columns WHERE table_name = '{}'".format(
            tb_name)
        rows = self.read(sql).fetchall()
        return [v[0] for v in rows]


class Postgres(SQLClient):
    CONNECTION_CLASS = PgConnection

    def __init__(self,
                 *,
                 user: str,
                 password: str,
                 db: str,
                 host: str,
                 port: int = 5432,
                 ):
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.port = port

    def connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.db,
        )
