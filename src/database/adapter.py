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
    
    # def sync_with_remote(self, remote_db_url: str):
    #     """Sync the current database with a remote database."""
    
    #     remote_engine = create_engine(remote_db_url)
    #     remote_metadata = MetaData()
    #     remote_metadata.reflect(remote_engine)
    #     remote_tables = remote_metadata.tables.keys()
        
    #     local_metadata = MetaData()
    #     local_metadata.reflect(self.engine)
    #     local_tables = local_metadata.tables.keys()
        
    #     for table in remote_tables:
    #         if table not in local_tables:
    #             remote_metadata.tables[table].create(self.engine)
    #             print(f"Created table {table}")
    #         else:
    #             print(f"Table {table} already exists")
        
    #     # copying data
    #     for table in remote_tables:
    #         remote_table = remote_metadata.tables[table]
    #         local_table = local_metadata.tables[table]
            
    #         remote_data = remote_engine.execute(remote_table.select()).fetchall()
    #         local_data = self.engine.execute(local_table.select()).fetchall()
            
    #         for row in remote_data:
    #             if row not in local_data:
    #                 self.engine.execute(local_table.insert().values(row))
    #                 print(f"Inserted row {row} into {table}")
    #             else:
    #                 print(f"Row {row} already exists in {table}")        


def load_db_adapter(echo=False):
    return DBAdapter(url="sqlite:///data/ip.db", echo=echo)
    try:
        db_adapter = DBAdapter(
            url='mssql+pymssql://',
            creator=lambda: pymssql.connect(
                server=server,
                user=user,
                password=password,
                database=database,
                port=port
            ),
            echo=echo
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
