from typing import List, Optional
from datetime import datetime
from src.models import SearchResultTable
from src.services import BaseService


class SearchResultService(BaseService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self.model = SearchResultTable
    
    def add_search_result(self, search_result_obj: SearchResultTable) -> SearchResultTable:
        """Add a new search result to the database."""
        session = self.db_adapter.get_session()
        session.add(search_result_obj)
        return search_result_obj
    
    def safe_add_search_result(self, obj: SearchResultTable, commit: bool = False) -> SearchResultTable:
        """Add a new search result to the database if it does not already exist."""
        session = self.db_adapter.get_session()
        searched_search_result = (session.query(SearchResultTable)
                                  .filter_by(query=obj.query)
                                  .first())
        if not searched_search_result:
            session.add(obj)
            if commit:
                session.commit()
        return obj
    
    def upsert_search_result(self, obj: SearchResultTable, commit: bool = False) -> SearchResultTable:
        """Add a new search result to the database if it does not already exist, 
        or update it if it does."""
        session = self.db_adapter.get_session()
        
        existing_search_result = (session.query(SearchResultTable)
                                .filter_by(query=obj.query)
                                .first())
        if existing_search_result:
            for attr, value in vars(obj).items():
                setattr(existing_search_result, attr, value)
            result = existing_search_result
        else:
            session.add(obj)
            result = obj
        
        if commit:
            session.commit()
        
        return result

    
    def get_search_results(self) -> List[SearchResultTable]:
        """Get all search results from the database."""
        session = self.db_adapter.get_session()
        return session.query(SearchResultTable).all()
    
    def get_search_result_by_query(self, query: str) -> Optional[SearchResultTable]:
        """Get a specific search result by query from the database."""
        session = self.db_adapter.get_session()
        return session.query(SearchResultTable).filter_by(query=query).first()
    
    def update_search_result(self, new_obj: SearchResultTable) -> SearchResultTable:
        """Update an existing search result in the database."""
        session = self.db_adapter.get_session()
        updated_obj = session.query(SearchResultTable).filter_by(id=new_obj.id).first()
        for attr in [attr for attr in dir(new_obj)]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        session.commit()
        return updated_obj
    
    def delete_search_result(self, result_id: int) -> SearchResultTable:
        """Delete a specific search result from the database."""
        session = self.db_adapter.get_session()
        search_result_obj = session.query(SearchResultTable).filter_by(id=result_id).first()
        session.delete(search_result_obj)
        session.commit()
        return search_result_obj
    
    def delete_all_search_results(self, commit: bool = False) -> bool:
        """Delete all search results from the database."""
        session = self.db_adapter.get_session()
        session.query(SearchResultTable).delete()
        if commit:
            session.commit()
        return True
