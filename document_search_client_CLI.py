import json
import pickle
import threading

from src.models import Config, SearchResultTable
from src.modules.crawler import Crawler
from src.modules.pagerank import PageRank, adapter
from timeit import default_timer as timer
from src.services.SearchResultService import SearchResultService


def update_search_results(raw_query, ranks, doc_count, cache_hit):
    if cache_hit:
        query = crawler._preprocess_document(raw_query).split(" ")
        ranks, doc_count = pr.get_pageranks(query, top=10)
    search_result_service.upsert_search_result(
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
    
    cache_hit = False
    try:
        sr_service_results = search_result_service.get_search_result_by_query(raw_query)
        if not sr_service_results:
            raise Exception("Cache miss")
        cache_hit = True
        ranks, doc_count = pickle.loads(sr_service_results.results)
    except:
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
        print(f"{rank.document.url} (score: {rank.idf_score:.3f})")
    
    print()
    print()
    
    threading.Thread(
        target=update_search_results,
        args=(raw_query, ranks, doc_count, cache_hit)
    ).start()

