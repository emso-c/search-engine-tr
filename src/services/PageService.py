from typing import List, Optional
from datetime import datetime

from sqlalchemy import func

from src.models import PageTable
from src.services import BaseService


class PageService(BaseService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self.model = PageTable
    
    def get_pages(self) -> List[PageTable]:
        """Get all pages from the database."""
        session = self.db_adapter.get_session()
        return session.query(PageTable).all()
    
    def remove_duplicates(self, column=PageTable.page_url) -> bool:
        """Remove duplicate IPs from the database."""
        session = self.db_adapter.get_session()
        session.query(self.model).filter(column.in_(
            session.query(column).group_by(column).having(func.count(column) > 1)
        )).delete(synchronize_session=False)
        session.commit()
        return True
    
    def count_unscanned_pages(self) -> int:
        """Count the number of unscanned pages."""
        session = self.db_adapter.get_session()
        val = session.query(PageTable).filter(PageTable.last_crawled == None).count()
        return val if val else 0
    
    def get_page(self, page_url: str) -> Optional[PageTable]:
        """Get a specific page from the database."""
        session = self.db_adapter.get_session()
        page = session.query(PageTable).filter(PageTable.page_url == page_url).first()
        return page
    
    def add_page(self, page: PageTable) -> PageTable:
        """Add a new page to the database."""
        session = self.db_adapter.get_session()
        session.add(page)
        return page

    def update_page(self, new_obj: PageTable) -> PageTable:
        """Update an existing page in the database."""
        session = self.db_adapter.get_session() 
        updated_obj = session.query(PageTable).filter_by(page_url=new_obj.page_url).first()
        if not updated_obj:
            raise ValueError(f"Cant find page with url: {new_obj.page_url} in the database.")

        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_")  and attr not in ["created_at", "updated_at"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        setattr(updated_obj, "updated_at", datetime.now())
        return updated_obj
    
    def delete_page(self, page_url: str) -> PageTable:
        """Delete a specific page from the database."""
        session = self.db_adapter.get_session()
        page = session.query(PageTable).filter(PageTable.page_url == page_url).first()
        session.delete(page)
        return page
    
    def upsert_page(self, new_page: PageTable) -> PageTable:
        """Update or insert a page in the database."""
        session = self.db_adapter.get_session()
        page = session.query(PageTable).filter(PageTable.page_url == new_page.page_url).first()
        if page:
            return self.update_page(new_page)
        return self.add_page(new_page)