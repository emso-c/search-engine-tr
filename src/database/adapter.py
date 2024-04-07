from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from src.models import Base
import pymssql
from data.credentials import *

class DBAdapter:
    def __init__(self, **engine_kwargs,):
        """Initialize the DBAdapter with a database URL."""
        self.engine = create_engine(**engine_kwargs, pool_size=0, pool_pre_ping=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.persistent_session = None
    
    def __enter__(self) -> 'DBAdapter':
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.engine.dispose()

    def get_session(self, persistent=True) -> Session:
        """Get a new Session."""
        if persistent:
            if self.persistent_session is None:
                self.persistent_session = self.Session(expire_on_commit=False)
                self.persistent_session.begin()
            return self.persistent_session
        return self.Session(expire_on_commit=False)

    def delete_db(self):
        """Delete the database."""
        print("WARNING: Deleting database!")
        Base.metadata.drop_all(self.engine)
    
    def get_connection_type(self):
        """Return the connection type."""
        return self.engine.url.get_dialect().name


def load_db_adapter(echo=False):
    try:
        db_adapter = DBAdapter(
            url=f'mssql+pymssql://{user}:{password}@{server}:{port}/{database}?charset=utf8',
            echo=echo,
        )
        # if not db_adapter.engine.connect():
        #     raise ConnectionError("Could not connect to the database")
        # TODO implement sync with remote database
        # print("Syncing with remote database")
        # DBAdapter(url="sqlite:///data/ip.db", echo=True).sync_with_remote(db_adapter.engine.url)
        print("Connected to the remote database")
        return db_adapter
    except (ConnectionError, OperationalError):
        # raise ConnectionError("Could not connect to the remote database")
        print("Warning: Could not connect to the database, falling back to local sqlite database")
        return DBAdapter(url="sqlite:///data/ip.db", echo=echo)
