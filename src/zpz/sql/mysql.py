import logging
import os
from typing import List, Tuple

import pymysql

from .sql import SQLClient

logger = logging.getLogger(__name__)


class MySQL(SQLClient):
    def __init__(self,
                 *,
                 user: str,
                 password: str,
                 database: str,
                 host: str,       # MySQL server url
                 port: int=3306,  # We use a non-standard port, so this is mandatory
                 **kwargs):
        super().__init__(
            conn_func=pymysql.connect,
            user=user,
            password=password,
            database=database,
            port=port,
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
