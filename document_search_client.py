import json
import random
import socket
import sys
import os
from threading import Thread, Event
import threading
import time
from urllib.parse import urlparse

from src.database.adapter import load_db_adapter
from src.models import Config, DocumentIndexTable, Document, PageScore, WordFrequency
from src.modules.crawler import Crawler
from src.services import IPService, PageService
from src.services.DocumentIndexService import (
    DocumentIndexService,
    convert_indices_to_document,
    calculate_inverse_document_frequency
)
from timeit import timeit

def _get_base_url(url: str) -> str:
    parsed_uri = urlparse(url)
    result = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri).strip()
    return result

with open("config.json") as f:
    config = Config(**json.load(f))

crawler = Crawler(config.crawler)
adapter = load_db_adapter()
page_service = PageService(adapter)
document_index_service = DocumentIndexService(adapter)
ip_service = IPService(adapter)

print("Initial document index count:", document_index_service.count())


class PageRank:
    def _to_page_score(self, tuples: list[tuple[Document, float]]) -> list[PageScore]:
        return [PageScore(document=doc, score=score) for doc, score in tuples]
    
    def _get_tf_idf_scores(self, words: list[str]) -> list[PageScore]:
        indices = document_index_service.get_document_indices_by_multiple_words(words)
        
        documents = convert_indices_to_document(words, indices)
        idf_scores = calculate_inverse_document_frequency(words, documents)
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
            #     print("Domain score found for", _get_base_url(document.document_id), ":", ip_obj.score)
            # else:
            #     print(f"Could not find IP for domain {_get_base_url(document.document_id)} from document {document.document_id}. Skipping.")
                
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

    def get_pageranks(self, words, top=10) -> tuple[list[PageScore], int]:
        idf_scores = self._get_tf_idf_scores(words)
        idf_scores = self._update_idf_scores_by_domain_authority(idf_scores)

        idf_scores.sort(key=lambda x: x.score, reverse=True)
        return idf_scores[:top], len(idf_scores)

pr = PageRank()
while True:
    words = input("Enter words to search (space separated): ")
    words = crawler._preprocess_document(words)
    words = words.split(" ")
    if not words:
        print("Please provide a valid search query.")
        break
    
    start = timeit()
    print("Searching for documents containing:", words)
    
    ranks, doc_count = pr.get_pageranks(words, top=10)
    
    if not ranks:
        print("No results found.")
        continue

    print(f"\nSearch results (searched {doc_count} documents in {timeit() - start:.3f}s):")
    for rank in ranks:
        print(f"{rank.document.url} (score: {rank.score:.3f})")
    print()
    print()