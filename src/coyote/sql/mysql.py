import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
from typing import List, Iterable

import MySQLdb as mysqlclient
from boltons.iterutils import chunked_iter

from .sql import SQLClient, Connection
from ..a_sync import IO_WORKERS

logger = logging.getLogger(__name__)


class MysqlConnection(Connection):
    def get_tables(self) -> List[str]:
        z = self.read('SHOW TABLES').fetchall()
        return [v[0] for v in z]

    def describe_table(self, tb_name: str) -> None:
        z = self.read(f'DESCRIBE {tb_name}').fetchall_pandas()
        pprint(z)

    def insert_batch(self,
                     rows: Iterable[Iterable[str]],
                     *,
                     tb_name: str,
                     cols: List[str]) -> int:
        columns_str = ", ".join(cols)
        symbol = "%s"
        val_place_holders = ", ".join([symbol] * (len(cols)))

        try:
            self._cursor.executemany(
                f"INSERT INTO {tb_name} ({columns_str}) VALUES ({val_place_holders})",
                rows,
            )
        except Exception as e:
            logger.exception(e)
            print('data rows:')
            for row in rows:
                print(row)
            raise

        return self._cursor.rowcount
        # This may not be the number of rows inserted.
        # The meaning depends on the MySQL client package.

    def table_rowcount(self, tb_name: str, exact: bool = True) -> int:
        if exact:
            sql = f"""SELECT COUNT(*) FROM {tb_name}"""
        else:
            sql = f"""SELECT table_rows FROM information_schema.tables WHERE table_name='{tb_name}' """
        z = self.read(sql).fetchall()
        return int(z[0][0])


class MySQL(SQLClient):
    CONNECTION_CLASS = MysqlConnection

    def __init__(self,
                 *,
                 user: str,
                 password: str,
                 db: str,
                 host: str,       # MySQL server url
                 port: int = 3306,
                 ):
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.port = port

    def connect(self):
        conn = mysqlclient.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            db=self.db,
        )
        conn.autocommit(True)
        return conn

    def insert_stream(self,
                      rows: Iterable,
                      *,
                      tb_name: str,
                      cols: List[str],
                      batch_size: int,
                      log_every_n_batches: int = 1,
                      max_concurrency: int = None,
                      ) -> int:
        max_conn = max_concurrency or IO_WORKERS

        with self.get_connection_pool(max_conn) as pool:
            futures = {}
            n_inserted = 0

            def callback(t):
                if t.cancelled():
                    raise RuntimeError('Future object has been cancelled')
                e = t.exception()
                if e is not None:
                    raise e
                del futures[id(t)]

            def insert_one_batch(rows, ibatch):
                if log_every_n_batches and (ibatch + 1) % log_every_n_batches == 0:
                    verbose = True
                    logger.info('  inserting batch #%d', ibatch + 1)
                else:
                    verbose = False
                with pool.get_connection() as conn:
                    n = conn.insert_batch(rows, tb_name=tb_name, cols=cols)
                if verbose:
                    logger.info('  inserted batch #%d', ibatch + 1)
                nonlocal n_inserted
                n_inserted += n

            with ThreadPoolExecutor(max_conn + 1) as executor:
                for ibatch, batch in enumerate(chunked_iter(rows, batch_size)):
                    while not pool.vacancy:
                        time.sleep(0.017)
                    t = executor.submit(insert_one_batch, batch, ibatch)
                    futures[id(t)] = t
                    t.add_done_callback(callable)

            while futures:
                time.sleep(0.012)
            return n_inserted
