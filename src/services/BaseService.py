from src.database.adapter import DBAdapter
from src.models import Base

class BaseService:
    def __init__(self, db_adapter: DBAdapter):
        """Initialize the BaseService with a DBAdapter."""
        self.db_adapter = db_adapter
        Base.metadata.create_all(self.db_adapter.engine)
