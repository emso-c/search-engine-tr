from src.database.adapter import DBAdapter
from src.models import Base

class BaseService:
    def __init__(self, db_adapter: DBAdapter):
        """Initialize the BaseService with a DBAdapter."""
        self.db_adapter = db_adapter
        Base.metadata.create_all(self.db_adapter.engine)

    def commit(self, verbose=True):
        """Commit the current transaction."""
        session = self.db_adapter.get_session()
        if not verbose:
            session.commit()
            return

        print("Commiting Service:", self.__class__.__name__)
        if session.dirty or session.deleted or session.new:
            session.commit()
            print("Changes committed:")
            print("New:", len(self.db_adapter.persistent_session.new))
            print("Updated:", len(self.db_adapter.persistent_session.dirty))
            print("Deleted:", len(self.db_adapter.persistent_session.deleted))
        else:
            print("No changes to commit.")