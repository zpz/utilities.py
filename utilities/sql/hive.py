import logging
from typing import Sequence, Tuple, List, Dict

from impala import dbapi

from .sql import split_sql, SQLClient

logging.getLogger('impala.hiveserver2').setLevel(logging.WARNING)
logging.getLogger('impala._thrift_api').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class Hive(SQLClient):
    def __init__(self,
                 *,
                 host,
                 user,
                 port=10000,
                 auth_mechanism='PLAIN',
                 configuration=None):
        super().__init__(
            conn_func=dbapi.connect,
            cursor_args={'user': user},
            host=host,
            port=int(port),
            user=user,
            auth_mechanism=auth_mechanism)
        self._configuration = configuration or {}
        # You may want to pass in via `configuration`:
        #   {'hive.execution.engine': 'tez'}

    def _parse_sql(self, sql: str) -> Tuple[List[str], Dict[str, str]]:
        assert isinstance(sql, str)
        sql = split_sql(sql)
        configuration = {}
        ss = []
        for s in sql:
            if s.strip().upper().startswith('SET '):
                a, b = s[4:].split('=')
                configuration[a.strip()] = b.strip()
            else:
                ss.append(s)
        return ss, configuration

    def read(self, sql: str):
        sqls, config = self._parse_sql(sql)
        assert len(sqls) == 1
        sql = sqls[0]
        config = {**self._configuration, **config}
        return super().read(sql, configuration=config)

    def write(self, sql: str):
        sqls, config = self._parse_sql(sql)
        config = {**self._configuration, **config}
        return super().write(sqls, configuration=config)

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

    def has_partition(self, db_name: str, tb_name: str,
                      partition_criteria: List[Tuple[str, str]]) -> bool:
        '''
        `partition_criteria` is a list of `(partition_name, partition_value)` tuples.
        '''
        sql = 'SHOW PARTITIONS {}.{} PARTITION({})'.format(
            db_name, tb_name,
            ','.join('{}="{}"'.format(a, b) for a, b in partition_criteria))
        _, rows = self.read(sql).fetchall()
        value = ','.join('{}={}'.format(a, b) for a, b in partition_criteria)
        for row in rows:
            if value in row:
                return True
        return False

    def drop_partition(self, db_name, tb_name, partition_criteria):
        sql = 'ALTER TABLE {}.{} DROP IF EXISTS PARTITION({})'.format(
            db_name, tb_name,
            ','.join('{}="{}"'.format(a, b) for a, b in partition_criteria))
        self.write(sql)
