from contextlib import contextmanager

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


class ManagedScopedSession(object):
    def __init__(self, database, host, port, user, password, **conn_kwargs):
        self._dbsession = scoped_session(
            sessionmaker(
                autocommit=False, autoflush=False, expire_on_commit=False))
        self._database = database

        connection_url = "mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}".format(
            database=database,
            host=host,
            port=port,
            user=user,
            passwd=password)
        for name, value in conn_kwargs.items():
            connection_url += '?{}={}'.format(name, value)

        self._engine = create_engine(connection_url, pool_recycle=3600)
        self._dbsession.configure(bind=self._engine)

    def get_session(self):
        return self._dbsession

    def get_engine(self):
        return self._engine

    @contextmanager
    def session(self):
        try:
            yield self._dbsession
        finally:
            self._dbsession.rollback()
            # self._dbsession.remove()
            # This will rollback un-committed transactions.
