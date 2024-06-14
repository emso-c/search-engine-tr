from typing import List, Optional
from datetime import datetime
from src.models import DocumentIndexTable
from src.services import BaseService


class DocumentIndexService(BaseService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self.model = DocumentIndexTable

    def add_document_index(self, document_index_obj: DocumentIndexTable) -> DocumentIndexTable:
        """Add a new document index to the database."""
        session = self.db_adapter.get_session()
        session.add(document_index_obj)
        return document_index_obj
    
    def safe_add_document_index(self, obj: DocumentIndexTable, commit:bool=False) -> DocumentIndexTable:
        """Add a new document index to the database if it does not already exist."""
        session = self.db_adapter.get_session()
        searched_document_index = (session.query(DocumentIndexTable)
                                   .filter_by(
                                       document_url=obj.document_url,
                                       word=obj.word)
                                   .first())
        if not searched_document_index:
            session.add(obj)
            if commit:
                session.commit()
        return obj
    
    def get_document_indices(self) -> List[DocumentIndexTable]:
        """Get all document indices from the database."""
        session = self.db_adapter.get_session()
        return session.query(DocumentIndexTable).all()
    
    def get_document_index(self, document_url: str, word: str) -> Optional[DocumentIndexTable]:
        """Get a specific document index from the database."""
        session = self.db_adapter.get_session()
        return session.query(DocumentIndexTable).filter_by(document_url=document_url, word=word).first()
    
    def get_document_indices_by_word(self, word: str, starting_with=False) -> List[DocumentIndexTable]:
        """Get all document indices by word from the database."""
        session = self.db_adapter.get_session()
        if starting_with:
            return session.query(DocumentIndexTable).filter(DocumentIndexTable.word.startswith(word)).all()
        return session.query(DocumentIndexTable).filter_by(word=word).all()
    
    def get_document_indices_by_multiple_words(self, words: List[str]) -> List[DocumentIndexTable]:
        """Get all document indices by multiple words from the database."""
        session = self.db_adapter.get_session()
        return session.query(DocumentIndexTable).filter(DocumentIndexTable.word.in_(words)).all()
    
    def update_document_index(self, new_obj: DocumentIndexTable) -> DocumentIndexTable:
        """Update an existing document index in the database."""
        session = self.db_adapter.get_session()
        updated_obj = session.query(DocumentIndexTable).filter_by(document_url=new_obj.document_url, word=new_obj.word).first()
        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_")  and attr not in ["created_at", "updated_at"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        setattr(updated_obj, "updated_at", datetime.now())
        return updated_obj
    
    def delete_document_index(self, document_url: str, word: str) -> DocumentIndexTable:
        """Delete a specific document index from the database."""
        session = self.db_adapter.get_session()
        document_index_obj = session.query(DocumentIndexTable).filter_by(document_url=document_url, word=word).first()
        session.delete(document_index_obj)
        return document_index_obj
    
    def delete_document_indices_by_document_url(self, document_url: str) -> List[DocumentIndexTable]:
        """Delete all document indices by document_url from the database."""
        session = self.db_adapter.get_session()
        document_index_objs = session.query(DocumentIndexTable).filter_by(document_url=document_url).all()
        for document_index_obj in document_index_objs:
            session.delete(document_index_obj)
        return document_index_objs
    
    def delete_all_document_indices(self, commit) -> bool:
        """Delete all document indices from the database."""
        session = self.db_adapter.get_session()
        session.query(DocumentIndexTable).delete()
        if commit:
            session.commit()
        return True

    def get_document_indices_by_document_url(self, document_url: int) -> List[DocumentIndexTable]:
        """Get all document indices by document_url from the database."""
        session = self.db_adapter.get_session()
        return session.query(DocumentIndexTable).filter_by(document_url=document_url).all()
