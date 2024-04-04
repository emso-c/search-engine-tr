from src.database.adapter import DBAdapter
from src.models import Base

class BaseService:
    def __init__(self, db_adapter: DBAdapter):
        """Initialize the BaseService with a DBAdapter."""
        self.db_adapter = db_adapter
        Base.metadata.create_all(self.db_adapter.engine)

    def commit(self, verbose=True):
        """Commit the current transaction."""
        if not verbose:
            self.db_adapter.get_session().commit()
            return

        print("Commiting Service:", self.__class__.__name__)
        if self.db_adapter.get_session().dirty \
            or self.db_adapter.get_session().deleted \
            or self.db_adapter.get_session().new:
            self.db_adapter.get_session().commit()
            print("Changes committed:")
            print("New:", len(self.db_adapter.persistent_session.new))
            print("Updated:", len(self.db_adapter.persistent_session.dirty))
            print("Deleted:", len(self.db_adapter.persistent_session.deleted))
        else:
            print("No changes to commit.")