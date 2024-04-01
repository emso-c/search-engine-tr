from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from src.models import Base


class DBAdapter:
    def __init__(self, **engine_kwargs,):
        """Initialize the DBAdapter with a database URL."""
        self.engine = create_engine(**engine_kwargs)
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
            return self.persistent_session
        return self.Session(expire_on_commit=False)

    def delete_db(self):
        """Delete the database."""
        print("WARNING: Deleting database!")
        Base.metadata.drop_all(self.engine)
