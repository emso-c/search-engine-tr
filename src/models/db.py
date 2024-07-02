import string
from sqlalchemy import Column, DateTime, Float, Index, Integer, LargeBinary, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class RepresentableTable:
    def __repr__(self) -> str:
        dict = self.__dict__.copy()
        dict.pop("_sa_instance_state", None)
        pretty = ", ".join([f"{k}={v}" for k, v in dict.items()])
        return f"<{self.__class__.__name__}({pretty})>"


class URLFrontierTable(Base, RepresentableTable):
    __tablename__ = "url_frontier"

    url = Column(String(255), primary_key=True)

class IPTableBase(object):
    __basename__ = "ip_table"
    partition_keys = list(string.ascii_lowercase)
    index_prefixes = [
        ("idx_ip", "ip"),
        ("idx_ip_last_crawled", "last_crawled")
    ]

    domain = Column(String(255), primary_key=True)
    ip = Column(String(15), nullable=True)
    port = Column(Integer, nullable=True)
    status = Column(Integer)
    score = Column(Float, default=0.0, nullable=False)
    last_crawled = Column(DateTime, nullable=True, default=None)
    
    @staticmethod
    def _get_partition_key(url: str):
        if url.startswith("http"):
            url = url.split("//")[1]
        elif url.startswith("www."):
            url = url[4:]
        key = url.lower()[0]
        if key not in IPTableBase.partition_keys:
            key = "default"
        return key

    @staticmethod
    def get_partition_tablename(url):
        key = IPTableBase._get_partition_key(url)
        return f"{IPTableBase.__basename__}_{key}"


class PageTableBase(object):
    __basename__ = "page_table"
    partition_keys = list(string.ascii_lowercase)
    index_prefixes = [
        ("idx_page_url", "page_url"),
        ("idx_page_table_last_crawled", "last_crawled")
    ]
    
    page_url = Column(String(255), primary_key=True)
    status_code = Column(Integer)
    title = Column(String, nullable=True)
    keywords = Column(String, nullable=True)
    description = Column(String, nullable=True)
    body = Column(LargeBinary, nullable=True)
    favicon = Column(LargeBinary, nullable=True)
    robotstxt = Column(LargeBinary, nullable=True)
    sitemap = Column(LargeBinary, nullable=True)
    last_crawled = Column(DateTime, nullable=True, default=None)

    @staticmethod
    def _get_partition_key(url: str):
        if url.startswith("http"):
            url = url.split("//")[1]
        elif url.startswith("www."):
            url = url[4:]
        key = url.lower()[0]
        if key not in PageTableBase.partition_keys:
            key = "default"
        return key

    @staticmethod
    def get_partition_tablename(url):
        key = PageTableBase._get_partition_key(url)
        return f"{PageTableBase.__basename__}_{key}"



class DocumentIndexTableBase(object):
    __basename__ = "document_index"
    partition_keys = list(string.ascii_lowercase)
    index_prefixes = [
        ("idx_document_url", "document_url"),
        ("idx_word", "word")
    ]

    document_url = Column(String(255), primary_key=True)  # pages.page_url
    word = Column(String(255), primary_key=True)
    frequency = Column(Integer)
    location = Column(Integer, primary_key=True)
    tag = Column(String(50))  # e.g., 'p', 'h1', 'title'
    
    @staticmethod
    def _get_partition_key(word: str):
        key = word.lower()[0]
        if key not in DocumentIndexTableBase.partition_keys:
            key = "default"
        return key

    @staticmethod
    def get_partition_tablename(url):
        key = DocumentIndexTableBase._get_partition_key(url)
        return f"{DocumentIndexTableBase.__basename__}_{key}"
    

class BacklinkTable(Base, RepresentableTable):
    __tablename__ = "backlinks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_url = Column(String(255), nullable=False)
    target_url = Column(String(255), nullable=False)
    anchor_text = Column(String, nullable=True)
    
    __table_args__ = (
        Index('idx_source_url', 'source_url'),
    )


class SearchResultTable(Base, RepresentableTable):
    __tablename__ = "search_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query = Column(String(1000), nullable=False)
    results = Column(LargeBinary, nullable=False)
    
    __table_args__ = (
        Index('idx_query', 'query'),
    )