import json
import pickle
import threading

from src.database.adapter import load_db_adapter
from src.models import Config, SearchResultTable
from src.modules.crawler import Crawler
from src.modules.pagerank import PageRank, adapter
from timeit import default_timer as timer
from src.services.SearchResultService import SearchResultService


def update_db_in_background(raw_query, ranks, doc_count):
    search_result_service.safe_add_search_result(
        SearchResultTable(
            query=raw_query,
            results=pickle.dumps((ranks, doc_count))
        ),
        commit=True
    )



with open("config.json") as f:
    config = Config(**json.load(f))
# adapter = load_db_adapter()
search_result_service = SearchResultService(adapter)
crawler = Crawler(config.crawler)
pr = PageRank()


while True:
    raw_query = input("Enter query to search (space separated): ")
    raw_query = crawler._preprocess_document(raw_query)
    query = raw_query.split(" ")
    if not query:
        print("Please provide a valid search query.")
        break
    
    start = timer()
    print("Searching for documents containing:", query)
    
    sr_service_results = search_result_service.get_search_result_by_query(raw_query)
    if sr_service_results:
        ranks, doc_count = pickle.loads(sr_service_results.results)
    else:
        ranks, doc_count = pr.get_pageranks(query, top=10)
    
    if not ranks:
        print("No results found.\n\n")
        continue

    end = timer()
    final_time = end - start
    final_time = final_time if final_time >= 0 else 0
    print(f"\nSearch results (searched {doc_count} documents in {final_time:.3f} seconds):")
    for i, rank in enumerate(ranks):
        if i == 0:
            print("[pinned]->", end=" ")
        print(f"{rank.document.url} (score: {rank.score:.3f})")
    
    print()
    print()
    
    if sr_service_results:
        ranks, doc_count = pr.get_pageranks(query, top=10)
    db_update_thread = threading.Thread(target=update_db_in_background, args=(raw_query, ranks, doc_count))
    db_update_thread.start()

