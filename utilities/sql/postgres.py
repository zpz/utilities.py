import psycopg2 as pg2

from .sql import SQLReader, SQLWriter


class PostgresMixin:
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
        self._conn.rollback()

    def execute(self, sql):
        try:
            return super().execute(sql)
        except:
            self.rollback()
            raise


class PostgresReader(SQLReader, PostgresMixin):
    def __init__(self, *, dbname, host, port, user, password, **kwargs):
        super().__init__(
            conn_func=pg2.connect,
            dbname=dbname, host=host, port=port, user=user, password=password,
            **kwargs)

    def get_databases(self):
        sql = "SELECT datname FROM pg_database WHERE datistemplate = false"
        headers, rows = self.execute(sql).fetchall()
        return [v[0] for v in rows]

    def get_tables(self):
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        headers, rows = self.execute(sql).fetchall()
        return [v[0] for v in rows]

    def has_table(self, tb_name):
        # A 'lazy' solution
        # return tb_name in self.get_tables()

        sql = "SELECT exists(SELECT relname FROM pg_class WHERE relname = '{}')".format(tb_name)
        return self.read(sql)

    def get_table_schema(self, tb_name):
        sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{tb_name}'
        """.format(tb_name=tb_name)
        return self.read(sql)

    def get_table_columns(self, tb_name):
        sql = "SELECT column_name from information_schema.columns WHERE table_name = '{}'".format(tb_name)
        headers, rows = self.execute(sql).fetchall()
        return [v[0] for v in rows]


class PostgresWriter(SQLWriter):
    def __init__(self, *, dbname, host, port, user, password, **kwargs):
        super().__init__(
            conn_func=pg2.connect,
            dbname=dbname, host=host, port=port, user=user, password=password,
            **kwargs)

    def commit(self):
        self._conn.commit()

