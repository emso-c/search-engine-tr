import time

from sqlalchemy.exc import OperationalError as saOperationalError
from sqlite3 import OperationalError as slOperationalError
from src.database.adapter import DBAdapter
from src.models import Base

class BaseService:
    def __init__(self, db_adapter: DBAdapter):
        """Initialize the BaseService with a DBAdapter."""
        self.db_adapter = db_adapter
        Base.metadata.create_all(self.db_adapter.engine)
        self.base_type = None # Set this in the child class

    def _commit(self) -> bool:
        session = self.db_adapter.get_session()        
        max_retries = 5
        retries = 5
        wait = 5
        while retries > 0:
            try:
                session.commit()
                return True
            except (saOperationalError, slOperationalError):
                print("Database locked, waiting...")
                session.rollback()
                print("Session rolled back.")
                time.sleep(wait)
            except Exception as e:
                print("Error committing changes, retrying:", e.__class__.__name__, e)
                retries -= 1
                session.rollback()
                print("Session rolled back.")
                time.sleep(wait)

        print(f"Failed to commit changes after {max_retries} retries.")
        session.rollback()
        return False
    
    def count(self):
        """Return the number of items in the database."""
        return self.db_adapter.get_session().query(self.base_type).count()

    def commit(self, verbose=False):
        """Commit the current transaction."""

        if not verbose:
            self._commit()
            return

        print("Commiting Service:", self.__class__.__name__)
        self._commit()
