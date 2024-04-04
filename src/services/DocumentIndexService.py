from collections import defaultdict
from typing import List, Optional
from datetime import datetime
import math
from src.models import DocumentIndexTable, WordFrequency, Document
from src.services import BaseService


def convert_indices_to_document(words: List[str], indices: List[DocumentIndexTable]) -> List[Document]:
    """Convert a list of document indices to a list of Document objects."""
    document_map = defaultdict(list)

    for index in indices:
        document_map[index.document_id].append(index)

    documents = []
    for document_id, indices in document_map.items():
        word_frequencies = []
        for word in words:
            frequency = 0
            for index in indices:
                if index.word == word:
                    frequency = index.frequency
                    break
            if frequency > 0:
                word_frequencies.append(WordFrequency(word=word, frequency=frequency))
        documents.append(Document(word_frequencies=word_frequencies, document_id=document_id))

    return documents

def calculate_inverse_document_frequency(words: List[str], documents: List[Document]) -> list[tuple[Document, float]]:
    """Calculate the inverse document frequency of each word.
    Returns a list of tuples containing the document and the inverse document frequency score.
    """
    document_word_containment_counts = {word: 0 for word in words}
    total_documents = len(documents)
    for word in words:
        for document in documents:
            for word_freq in document.word_frequencies:
                if word_freq.word == word:
                    document_word_containment_counts[word] = document_word_containment_counts[word]+1
    
    inverse_document_frequencies = []
    for document in documents:
        idf_score = 0
        for word in words:
            dfx = document_word_containment_counts[word]
            f = 0
            for wf in document.word_frequencies:
                if wf.word == word:
                    f = wf.frequency
                    break
            
            score = f * math.log(total_documents / dfx)
            idf_score += score
        inverse_document_frequencies.append((document, idf_score))
    
    return inverse_document_frequencies


class DocumentIndexService(BaseService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)

    def add_document_index(self, document_index_obj: DocumentIndexTable) -> DocumentIndexTable:
        """Add a new document index to the database."""
        session = self.db_adapter.get_session()
        session.add(document_index_obj)
        return document_index_obj
    
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
        obj = session.query(DocumentIndexTable).filter_by(document_url=new_obj.document_url, word=new_obj.word).first()
        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_")]:
            setattr(obj, attr, getattr(new_obj, attr))
        return obj
    
    def delete_document_index(self, document_url: str, word: str) -> DocumentIndexTable:
        """Delete a specific document index from the database."""
        session = self.db_adapter.get_session()
        document_index_obj = session.query(DocumentIndexTable).filter_by(document_url=document_url, word=word).first()
        session.delete(document_index_obj)
        return document_index_obj
    
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
