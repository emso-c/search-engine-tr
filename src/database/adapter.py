from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Session

Base = declarative_base()

class DBAdapter:
    def __init__(self, db_url: str):
        """Initialize the DBAdapter with a database URL."""
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
    
    def __enter__(self) -> 'DBAdapter':
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.engine.dispose()
        self.Session

    def get_session(self) -> Session:
        """Get a new Session."""
        return self.Session(expire_on_commit=False)

    def delete_db(self):
        """Delete the database."""
        print("WARNING: Deleting database!")
        Base.metadata.drop_all(self.engine)
