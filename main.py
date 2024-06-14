import json
from typing import List, Optional
from datetime import datetime
import math

import requests
from src.database.adapter import DBAdapter
from src.models import Config, DocumentIndexTable, IPTable, WordFrequency, Document
from src.services import IPService, DocumentIndexService, PageService
from collections import Counter, defaultdict
from src.modules.crawler import Crawler, CrawlerConfig
from src.utils import ResponseConverter


with open("config.json") as f:
    config = Config(**json.load(f))

db_adapter = DBAdapter(url="sqlite:///data/ip.db")
crawler = Crawler(config.crawler)
page_service = PageService(db_adapter)


# for page in page_service.get_pages()[:10]:
#     if page.robotstxt:
#         print(page.page_url)
# exit()
url = "http://c2673.cloudnet.cloud"
robotstxt_bytes:bytes = page_service.get_page(url).robotstxt

from urllib.robotparser import RobotFileParser

parser = RobotFileParser()
parser.parse(robotstxt_bytes.decode("utf-8").splitlines())
print(parser.can_fetch(crawler.config.user_agent, url))
exit()


db_adapter = DBAdapter(url="sqlite:///data/ip.db")
# db_adapter.delete_db()
ip_service = IPService(db_adapter)

ip_service.upsert_ip(IPTable(ip="0.0.0.0", domain="example.com", port=80, score=0, status=200))
ip_service.upsert_ip(IPTable(ip="0.0.0.0", domain="example2.com", port=61, score=0, status=429))
ip_service.upsert_ip(IPTable(ip="0.0.0.0", domain="example3.com", port=8080, score=0, status=403))
ip_service.upsert_ip(IPTable(ip="0.0.0.1", domain="example.com", port=8081, score=0, status=403))
ip_service.upsert_ip(IPTable(ip="0.0.0.1", domain="example.com", port=82, score=0, status=500))

ip_service.commit()

from pprint import pprint
pprint(ip_service.get_ips())


exit()

# favicon = requests.get("http://85.1.163.91/favicon.ico")
# #to binary
# from PIL import Image
# import io
# image = Image.open(io.BytesIO(favicon.content))
# print(image)


# with open("config.json") as f:
#     config = Config(**json.load(f))

# crawler = Crawler(config.crawler)
# response = requests.get("http://201.194.192.66/doc/page/login.asp?_1712224784469")  # case study 1 (SPA, angular)
# # response = requests.get("http://85.1.163.91/")
# response = ResponseConverter.from_requests(response)
# df = crawler.get_document_frequency(response.body)
# print(df.most_common(10))
# exit()

def convert_indices_to_document(words: List[str], indices: List[DocumentIndexTable]) -> List[Document]:
    """Convert a list of document indices to a list of Document objects."""
    document_map = defaultdict(list)

    # Group indices by document_id
    for index in indices:
        document_map[index.document_id].append(index)

    # Create Document objects
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
        documents.append(Document(word_frequencies=word_frequencies, url=document_id))

    return documents

indices = [
    DocumentIndexTable(index_id=1, word='foo', frequency=3, document_id="doc1"),
    DocumentIndexTable(index_id=2, word='bar', frequency=1, document_id="doc1"),
    DocumentIndexTable(index_id=3, word='baz', frequency=1, document_id="doc1"),
    
    DocumentIndexTable(index_id=4, word='foo', frequency=2, document_id="doc2"),
    DocumentIndexTable(index_id=5, word='bar', frequency=2, document_id="doc2"),
    
    DocumentIndexTable(index_id=6, word='foo', frequency=4, document_id="doc3"),
]
words = ['foo', 'bar']

document_list = convert_indices_to_document(words, indices)
for document in document_list:
    print(f"Document {document.document_id}:", document.word_frequencies)


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


inverse_document_frequencies = calculate_inverse_document_frequency(words, document_list)
for document, idf_score in inverse_document_frequencies:
    print(f"Document {document.document_id} IDF score: {idf_score}")
