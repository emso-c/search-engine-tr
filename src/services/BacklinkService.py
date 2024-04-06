from typing import List, Optional
from datetime import datetime
from src.database.adapter import DBAdapter
from src.models import BacklinkTable
from src.services import BaseService

class BacklinkService(BaseService):
    def __init__(self, db_adapter: DBAdapter):
        super().__init__(db_adapter)
        self.model = BacklinkTable
    
    def add_backlink(self, backlink_obj: BacklinkTable) -> BacklinkTable:
        """Add a new backlink to the database."""
        session = self.db_adapter.get_session()
        session.add(backlink_obj)
        return backlink_obj

    def get_backlinks(self) -> List[BacklinkTable]:
        """Get all backlinks from the database."""
        session = self.db_adapter.get_session()
        return session.query(BacklinkTable).all()
    
    def get_backlink(self, backlink_url: str) -> Optional[BacklinkTable]:
        """Get a specific backlink from the database."""
        session = self.db_adapter.get_session()
        return session.query(BacklinkTable).filter_by(backlink_url=backlink_url).first()
    
    def get_backlinks_by_target_url(self, target_url: str) -> List[BacklinkTable]:
        """Get all backlinks by target url from the database."""
        session = self.db_adapter.get_session()
        return session.query(BacklinkTable).filter_by(target_url=target_url).all()
    
    def get_backlinks_by_source_url(self, source_url: str) -> List[BacklinkTable]:
        """Get all backlinks by source url from the database."""
        session = self.db_adapter.get_session()
        return session.query(BacklinkTable).filter_by(source_url=source_url).all()

    def get_backlinks_by_source_and_target_url(self, source_url: str, target_url: str) -> List[BacklinkTable]:
        """Get all backlinks by source and target url from the database."""
        session = self.db_adapter.get_session()
        return session.query(BacklinkTable).filter_by(source_url=source_url, target_url=target_url).all()
    
    def update_backlink(self, new_obj: BacklinkTable) -> BacklinkTable:
        """Update an existing backlink in the database."""
        session = self.db_adapter.get_session()
        updated_obj = session.query(BacklinkTable).filter_by(id=new_obj.id).first()
        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_")  and attr not in ["created_at"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        return updated_obj
    
    def delete_backlinks_by_source_to_target_url(self, source_url: str, target_url: str) -> List[BacklinkTable]:
        """Delete all backlinks by source to target url from the database."""
        session = self.db_adapter.get_session()
        backlink_objs = session.query(BacklinkTable).filter_by(source_url=source_url, target_url=target_url).all()
        for backlink_obj in backlink_objs:
            session.delete(backlink_obj)
        return backlink_objs
