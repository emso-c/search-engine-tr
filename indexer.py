from src.database.adapter import load_db_adapter
from src.modules.crawler import Crawler
from src.services import PageService
from src.services.DocumentIndexService import DocumentIndexService
from src.utils import config

adapter = load_db_adapter()
page_service = PageService(adapter)
document_index_service = DocumentIndexService(adapter)

print("Initial document index count:", document_index_service.count())

crawler = Crawler(config.crawler)

# clear the document index table
document_index_service.delete_all_document_indices(commit=True)

for page in page_service.get_pages():
    if not page.body:
        continue
    if not isinstance(page.body, bytes):
        raise ValueError("Page body is not bytes")
    
    content = page.body.decode("utf-8", errors="ignore")
    document_frequency, word_details = crawler.get_document_frequency(content)
    
    if document_frequency:
        for word, freq in document_frequency.items():
            for location, tag in word_details[word]:
                document_index = document_index_service.generate_obj(
                    "word",
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
