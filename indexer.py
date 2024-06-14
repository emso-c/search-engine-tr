import json
from src.database.adapter import load_db_adapter
from src.models import Config, DocumentIndexTable
from src.modules.crawler import Crawler
from src.services import IPService, PageService
from src.services.DocumentIndexService import DocumentIndexService


with open("config.json") as f:
    config = Config(**json.load(f))

adapter = load_db_adapter()
page_service = PageService(adapter)
document_index_service = DocumentIndexService(adapter)

print("Initial document index count:", document_index_service.count())

crawler = Crawler(config.crawler)

# Clear the document index table
document_index_service.delete_all_document_indices(commit=True)

for page in page_service.get_pages():
    if not page.body:
        continue
    if not isinstance(page.body, bytes):
        raise ValueError("Page body is not bytes")
    
    content = page.body.decode("utf-8", errors="ignore")
    document_frequency, word_details = crawler.get_document_frequency(content)
    
    if document_frequency:
        # clear the document index table for page_url to not nuke entire index because of a single page
        # document_index_service.delete_document_indices_by_document_url(page.page_url)

        for word, freq in document_frequency.items():
            for location, tag in word_details[word]:
                document_index = DocumentIndexTable(
                    document_url=page.page_url,
                    word=word,
                    frequency=freq,
                    location=location,
                    tag=tag
                )
                document_index_service.add_document_index(document_index)
        print(f"Indexed {page.page_url}")
        document_index_service.commit()

print("Indexing complete. Total indices:", document_index_service.count())
