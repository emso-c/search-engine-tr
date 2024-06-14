import json

from src.models import Config
from src.modules.crawler import Crawler
from src.modules.pagerank import PageRank
from timeit import default_timer as timer


with open("config.json") as f:
    config = Config(**json.load(f))

crawler = Crawler(config.crawler)


pr = PageRank()
while True:
    words = input("Enter words to search (space separated): ")
    words = crawler._preprocess_document(words)
    words = words.split(" ")
    if not words:
        print("Please provide a valid search query.")
        break
    
    start = timer()
    print("Searching for documents containing:", words)
    
    ranks, doc_count = pr.get_pageranks(words, top=10)
    
    if not ranks:
        print("No results found.\n\n")
        continue

    end = timer()
    final_time = end - start
    final_time = final_time if final_time >= 0 else 0
    print(f"\nSearch results (searched {doc_count} documents in {final_time:.3f} seconds):")
    for rank in ranks:
        print(f"{rank.document.url} (score: {rank.score:.3f})")
    print()
    print()
