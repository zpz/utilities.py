import pymysql

from .sql import SQLReader, SQLWriter


class MySQLReader(SQLReader):
    def __init__(self, *, host, port, user, password, **kwargs):
        super().__init__(
            conn_func=pymysql.connect,
            host=host,
            port=port,
            user=user,
            passwd=password,
            **kwargs)

    def get_databases(self):
        sql = 'SHOW DATABASES'
        self.execute(sql)
        return [v[0] for v in self.fetchall()[1]]

    def has_database(self, db_name):
        return db_name in self.get_databases()

    def get_tables(self, db_name):
        sql = 'SHOW TABLES IN ' + db_name
        self.execute(sql)
        return [v[0] for v in self.fetchall()[1]]

    def has_table(self, db_name, tb_name):
        return tb_name in self.get_tables(db_name)

    def get_table_schema(self, db_name, tb_name):
        sql = 'DESCRIBE {}.{}'.format(db_name, tb_name)
        return self.read(sql)


class MySQLWriter(SQLWriter):
    def __init__(self, *, host, port, user, password, **kwargs):
        super().__init__(
            conn_func=pymysql.connect,
            host=host,
            port=port,
            user=user,
            passwd=password,
            **kwargs)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()
