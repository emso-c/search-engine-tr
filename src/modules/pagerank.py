from collections import defaultdict
import json
from urllib.parse import urlparse

from src.database.adapter import load_db_adapter
from src.models import Config, Document, PageScore
from src.modules.crawler import Crawler
from src.modules.document_score_calculator import DocumentScoreCalculator
from src.services import IPService
from src.services.DocumentIndexService import DocumentIndexService
from src.utils import tag_weights

def _get_base_url(url: str) -> str:
    parsed_uri = urlparse(url)
    result = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri).strip()
    return result

with open("config.json") as f:
    config = Config(**json.load(f))

crawler = Crawler(config.crawler)
adapter = load_db_adapter()
document_index_service = DocumentIndexService(adapter)
ip_service = IPService(adapter)


class PageRank:
    def _to_page_score(self, tuples: list[tuple[Document, float]]) -> list[PageScore]:
        return [PageScore(document=doc, score=score) for doc, score in tuples]
    
    def _get_tf_idf_scores(self, words: list[str]) -> list[PageScore]:
        indices = document_index_service.get_document_indices_by_multiple_words(words)
        
        documents = DocumentScoreCalculator.convert_indices_to_document(words, indices)
        idf_scores = DocumentScoreCalculator.calculate_inverse_document_frequency(words, documents)
        if not idf_scores:
            print("No documents found.")
            return []
        return self._to_page_score(idf_scores)

    def _update_idf_scores_by_domain_authority(self, idf_scores: list[PageScore]) -> list[PageScore]:
        final_scores = []

        # update the idf scores by domain authority
        for idf_score in idf_scores:
            document = idf_score.document
            idf_score = idf_score.score
            
            ip_obj = ip_service.get_ip_by_domain(_get_base_url(document.url))
            domain_score = 0
            if ip_obj:
                domain_score = ip_obj.score
                
            if not idf_score and not domain_score:
                score = 0
            elif not idf_score and domain_score:
                score = domain_score
            elif idf_score and not domain_score:
                score = idf_score
            elif idf_score and domain_score:
                score = idf_score * domain_score
                
            final_scores.append(PageScore(document=document, score=score))
        
        return final_scores
    
    def _update_idf_scores_by_tag_weights(self, idf_scores: list[PageScore]) -> list[PageScore]:
        for page_score in idf_scores:
            tag_weight_total = 0
            for wf in page_score.document.word_frequencies:
                tag_weight_total += tag_weights.get(wf.tag, 1.0)
            page_score.score *= tag_weight_total / len(page_score.document.word_frequencies)
        return idf_scores
    
    def _update_idf_scores_by_word_proximity(self, idf_scores: list[PageScore]) -> list[PageScore]:
        for page_score in idf_scores:
            word_locations = defaultdict(list)
            for wf in page_score.document.word_frequencies:
                word_locations[wf.word].append(wf.location_index)
            proximity_score = self._calculate_proximity_score(word_locations)
            page_score.score *= proximity_score
        return idf_scores
    
    def _calculate_proximity_score(self, word_locations):
        min_distance = float('inf')
        words = list(word_locations.keys())
        
        for i in range(len(words)):
            for j in range(i + 1, len(words)):
                distances = [abs(loc1 - loc2) for loc1 in word_locations[words[i]] for loc2 in word_locations[words[j]]]
                if distances:
                    min_distance = min(min_distance, min(distances))
                    
        if min_distance == float('inf'):
            return 1.0  # No valid distances found

        return 1 / (1 + min_distance)  # The smaller the distance, the higher the score
    
    def get_pageranks(self, words, top=10) -> tuple[list[PageScore], int]:
        idf_scores = self._get_tf_idf_scores(words)
        
        # Find the document with the highest frequency
        most_frequent_document = max(idf_scores, key=lambda x: x.document.word_frequencies[0].frequency)
        idf_scores.remove(most_frequent_document)

        idf_scores = self._update_idf_scores_by_domain_authority(idf_scores)
        idf_scores = self._update_idf_scores_by_tag_weights(idf_scores)
        idf_scores = self._update_idf_scores_by_word_proximity(idf_scores)
        
        # Sort the remaining documents by score in descending order
        idf_scores.sort(key=lambda x: x.score, reverse=True)
        
        # Insert the most frequent document at the top
        idf_scores.insert(0, most_frequent_document)
        
        return idf_scores[:top], len(idf_scores)
