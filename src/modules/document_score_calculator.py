from collections import defaultdict
from typing import List
import math
from src.models import DocumentIndexTableBase, WordFrequency, Document

class DocumentScoreCalculator:
    @staticmethod
    def _preprocess_words(words: List[str]) -> List[str]:
        """Preprocess a list of words by removing any non-alphanumeric characters and converting to lowercase."""
        return [word.lower().strip() for word in words if word.isalnum()]

    @staticmethod
    def convert_indices_to_document(words: List[str], indices: List[DocumentIndexTableBase]) -> List[Document]:
        """Convert a list of document indices to a list of Document objects."""
        words = DocumentScoreCalculator._preprocess_words(words)
        document_map = defaultdict(list)

        for index in indices:
            document_map[index.document_url].append(index)

        documents = []
        for document_url, indices in document_map.items():
            word_frequencies = []
            for word in words:
                for index in indices:
                    if word.lower() == index.word.lower():
                        word_frequencies.append(WordFrequency(
                            word=word, 
                            frequency=index.frequency, 
                            location_index=index.location, 
                            tag=index.tag
                        ))
                        break
            documents.append(Document(url=document_url, word_frequencies=word_frequencies))
        return documents

    @staticmethod
    def calculate_inverse_document_frequency(words: List[str], documents: List[Document]) -> list[tuple[Document, float]]:
        """Calculate the inverse document frequency of each word.
        Returns a list of tuples containing the document and the inverse document frequency score.
        """
        words = DocumentScoreCalculator._preprocess_words(words)
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
                if dfx == 0:
                    continue
                f = 0
                for wf in document.word_frequencies:
                    if wf.word == word:
                        f = wf.frequency
                        break

                score = f * math.log10(total_documents / dfx)
                # score = f * (total_documents / dfx)
                # print(f"f*ln(N/dfx) = {f}*log({total_documents}/{dfx}) = {score}")
                idf_score += score
            inverse_document_frequencies.append((document, idf_score))
        return inverse_document_frequencies
