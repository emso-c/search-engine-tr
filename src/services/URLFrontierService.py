from typing import List, Optional
from datetime import datetime
from src.database.adapter import DBAdapter
from src.models import URLFrontierTable
from src.services import BaseService

class URLFrontierService(BaseService):
    def __init__(self, db_adapter: DBAdapter):
        self.db_adapter = db_adapter
    
    def get_urls(self) -> List[URLFrontierTable]:
        """Get all urls from the database."""
        session = self.db_adapter.get_session()
        return session.query(URLFrontierTable).all()
    
    def safe_add_url(self, url: str) -> Optional[URLFrontierTable]:
        """Add a new url to the database if it does not already exist."""
        if not url:
            return None
        print("Adding url to url frontier:", url)
        session = self.db_adapter.get_session()
        searched_url = session.query(URLFrontierTable).filter(URLFrontierTable.url == url).first()
        if not searched_url:
            session.add(URLFrontierTable(url=url))
        else:
            searched_url.score += 1.0  # TODO the increment value should be configurable
        return url
    
    def get_url(self, url: str) -> Optional[URLFrontierTable]:
        """Get a specific url from the database."""
        session = self.db_adapter.get_session()
        url = session.query(URLFrontierTable).filter(URLFrontierTable.url == url).first()
        return url
    
    def add_url(self, url: str) -> URLFrontierTable:
        """Add a new url to the database."""
        session = self.db_adapter.get_session()
        session.add(URLFrontierTable(url=url))
        return url

    def update_url(self, new_obj: URLFrontierTable) -> URLFrontierTable:
        """Update an existing url in the database."""
        session = self.db_adapter.get_session() 
        updated_obj = session.query(URLFrontierTable).filter_by(url=new_obj.url).first()
        if not updated_obj:
            raise ValueError(f"Cant find url with url: {new_obj.url} in the database.")

        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_")  and attr not in ["created_at", "updated_at", "score"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        setattr(updated_obj, "updated_at", datetime.now())
        setattr(updated_obj, "score", new_obj.score)

        return updated_obj
    
    def delete_url(self, url_str: str) -> URLFrontierTable:
        """Delete a specific url from the database."""
        if not url_str:
            return None
        session = self.db_adapter.get_session()
        url = session.query(URLFrontierTable).filter(URLFrontierTable.url == url_str).first()
        if not url:
            return None
        session.delete(url)
        return url

    def delete_all_urls(self) -> List[URLFrontierTable]:
        """Delete all urls from the database."""
        session = self.db_adapter.get_session()
        urls = session.query(URLFrontierTable).all()
        for url in urls:
            session.delete(url)
        return urls
