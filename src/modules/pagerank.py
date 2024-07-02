from collections import defaultdict
import json
from urllib.parse import urlparse

from src.database.adapter import load_db_adapter
from src.models import Config, Document, PageScore
from src.modules.crawler import Crawler
from src.modules.document_score_calculator import DocumentScoreCalculator
from src.modules.normalizer import Normalizer
from src.services import IPService, PageService
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
page_service = PageService(adapter)


class PageRank:
    _default_weights = {
        'idf': 0.8,
        'proximity': 0.5,
        'weights': 0.3,
        'authority': 0.1
    }
    
    def __init__(self, weights:dict={}, norm_method=None):
        self.weights = weights or self._default_weights
        self.normalization_method = norm_method or Normalizer.z_score
    
    def normalize(self, scores):
        if len(scores) == 0:
            return scores
        return self.normalization_method(scores)

    def _to_page_score(self, tuples: list[tuple[Document, float]]) -> list[PageScore]:
        return [PageScore(document=doc, idf_score=score) for doc, score in tuples]
    
    def _get_tf_idf_scores(self, words: list[str]) -> list[PageScore]:
        indices = document_index_service.get_document_indices_by_multiple_words(words)
        
        documents = DocumentScoreCalculator.convert_indices_to_document(words, indices)
        idf_scores = DocumentScoreCalculator.calculate_inverse_document_frequency(words, documents)
        if not idf_scores:
            print("No documents found.")
            return []
        return self._to_page_score(idf_scores)

    def _update_idf_scores_by_domain_authority(self, page_scores: list[PageScore]) -> list[PageScore]:
        idf_scores = [ps.idf_score for ps in page_scores]
        idf_scores = self.normalize(idf_scores)
        
        domain_scores = []
        for page_score in page_scores:
            document = page_score.document
            ip_obj = ip_service.get_ip_by_domain(_get_base_url(document.url))
            domain_score = ip_obj.score if ip_obj else 0
            domain_scores.append(domain_score)
        domain_scores = self.normalize(domain_scores)
        
        for idx, page_score in enumerate(page_scores):
            page_score.idf_score = (
                self.weights['idf'] * idf_scores[idx] +
                self.weights['authority'] * domain_scores[idx]
            )
        
        return page_scores
    
    def _update_idf_scores_by_tag_weights(self, page_scores: list[PageScore]) -> list[PageScore]:
        tag_weight_scores = []
        for page_score in page_scores:
            tag_weight_total = sum(tag_weights.get(wf.tag, 1.0) for wf in page_score.document.word_frequencies)
            tag_weight_scores.append(tag_weight_total / len(page_score.document.word_frequencies))
        
        tag_weight_scores = self.normalize(tag_weight_scores)
        
        for idx, page_score in enumerate(page_scores):
            page_score.idf_score += self.weights['weights'] * tag_weight_scores[idx]
        
        return page_scores
    
    def _update_idf_scores_by_word_proximity(self, page_scores: list[PageScore]) -> list[PageScore]:
        proximity_scores = []
        for page_score in page_scores:
            word_locations = defaultdict(list)
            for wf in page_score.document.word_frequencies:
                word_locations[wf.word].append(wf.location_index)
            proximity_scores.append(self._calculate_proximity_score(word_locations))
        
        proximity_scores = self.normalize(proximity_scores)
        
        for idx, page_score in enumerate(page_scores):
            page_score.idf_score += self.weights['proximity'] * proximity_scores[idx]
        
        return page_scores
    
    def _calculate_proximity_score(self, word_locations):
        min_distance = float('inf')
        words = list(word_locations.keys())
        
        for i in range(len(words)):
            for j in range(i + 1, len(words)):
                distances = [abs(loc1 - loc2) for loc1 in word_locations[words[i]] for loc2 in word_locations[words[j]]]
                if distances:
                    min_distance = min(min_distance, min(distances))
        
        if (min_distance == float('inf')):
            return 1.0  # No valid distances found

        return 1 / (1 + min_distance)  # The smaller the distance, the higher the score
    
    def _attach_document_metadata(self, page_scores: list[PageScore]):
        for page_score in page_scores:
            document = page_service.get_page(page_score.document.url)
            if not document:
                continue
            page_score.document.title = document.title
            page_score.document.description = document.description
        return page_scores

    def get_pageranks(self, words, top=10) -> tuple[list[PageScore], int]:
        page_scores = self._get_tf_idf_scores(words)
        
        if not page_scores:
            return [], 0
        
        # Find the document with the highest frequency
        most_frequent_document = max(page_scores, key=lambda x: x.document.word_frequencies[0].frequency)
        page_scores.remove(most_frequent_document)

        page_scores = self._update_idf_scores_by_domain_authority(page_scores)
        page_scores = self._update_idf_scores_by_tag_weights(page_scores)
        page_scores = self._update_idf_scores_by_word_proximity(page_scores)
        
        # Sort the remaining documents by score in descending order
        page_scores.sort(key=lambda x: x.idf_score, reverse=True)
        
        # Insert the most frequent document at the top
        page_scores.insert(0, most_frequent_document)
        page_scores = self._attach_document_metadata(page_scores)
        
        return page_scores[:top], len(page_scores)
