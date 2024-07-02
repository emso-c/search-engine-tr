from sqlalchemy import union_all
from src.models import DocumentIndexTableBase
from src.services import PartitionedService 


class DocumentIndexService(PartitionedService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self.base_type = DocumentIndexTableBase

    def add_document_index(self, document_index_obj: DocumentIndexTableBase) -> DocumentIndexTableBase:
        """Add a new document index to the database."""
        session = self.db_adapter.get_session()
        session.add(document_index_obj)
        return document_index_obj
    
    def safe_add_document_index(self, obj: DocumentIndexTableBase, commit:bool=False) -> DocumentIndexTableBase:
        """Add a new document index to the database if it does not already exist."""
        session = self.db_adapter.get_session()
        table = self.base_type.get_partition_tablename(obj.word)
        DynamicModel = self.get_model(table)
        searched_document_index = (session.query(DynamicModel)
                                   .filter_by(
                                       document_url=obj.document_url,
                                       word=obj.word)
                                   .first())
        if not searched_document_index:
            session.add(obj)
            if commit:
                session.commit()
        return obj
    
    def get_document_indices(self) -> list[DocumentIndexTableBase]:
        """Get all document indices from the database."""
        return self.get_all()
    
    def get_document_indices_by_word(self, word: str, starting_with=False) -> list[DocumentIndexTableBase]:
        """Get all document indices by word from the database."""
        session = self.db_adapter.get_session()
        table = self.base_type.get_partition_tablename(word)
        DynamicModel = self.get_model(table)
        if starting_with:
            return session.query(DynamicModel).filter(DynamicModel.word.startswith(word)).all()
        return session.query(DynamicModel).filter_by(word=word).all()
    
    def get_document_indices_by_multiple_words(self, words: list[str]) -> list[DocumentIndexTableBase]:
        queries = []
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            query = self.db_adapter.get_session().query(DynamicTable).filter(
                DynamicTable.word.in_(words)
            )
            queries.append(query)
        
        fetch_all_query = union_all(*queries)
        result = self.db_adapter.get_session().execute(fetch_all_query).fetchall()
        return self.rows_to_objects(result)

    def update_document_index(self, new_obj: DocumentIndexTableBase) -> DocumentIndexTableBase:
        """Update an existing document index in the database."""
        session = self.db_adapter.get_session()
        table = self.base_type.get_partition_tablename(new_obj.word)
        DynamicModel = self.get_model(table)
        updated_obj = session.query(DynamicModel).filter_by(document_url=new_obj.document_url, word=new_obj.word).first()
        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_")  and attr not in ["created_at", "updated_at"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        return updated_obj
    
    def delete_document_index(self, document_url: str, word: str) -> DocumentIndexTableBase:
        """Delete a specific document index from the database."""
        session = self.db_adapter.get_session()
        table = self.base_type.get_partition_tablename(word)
        DynamicModel = self.get_model(table)
        document_index_obj = session.query(DynamicModel).filter_by(document_url=document_url, word=word).first()
        session.delete(document_index_obj)
        return document_index_obj
    
    def delete_all_document_indices(self, commit) -> bool:
        """Delete all document indices from the database."""
        queries = []
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            query = self.db_adapter.get_session().query(DynamicTable).delete()
            queries.append(query)
        
        return True

    def get_document_indices_by_document_url(self, document_url: int) -> list[DocumentIndexTableBase]:
        """Get all document indices by document_url from the database."""
    
        queries = []
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            query = self.db_adapter.get_session().query(DynamicTable).filter_by(
                document_url=document_url
            )
            queries.append(query)
        
        fetch_all_query = union_all(*queries)
        result = self.db_adapter.get_session().execute(fetch_all_query).fetchall()
        return self.rows_to_objects(result)
