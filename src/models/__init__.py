from datetime import datetime
from sqlalchemy import JSON, Column, DateTime, Float, Index, Integer, LargeBinary, String, Text
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
    # created_at = Column(DateTime, default=datetime.now)

class IPTable(Base, RepresentableTable):
    __tablename__ = "ips"

    domain = Column(String(255), primary_key=True)
    ip = Column(String(15), nullable=True)
    port = Column(Integer, nullable=True)
    status = Column(Integer)
    score = Column(Float, default=0.0, nullable=False)
    last_crawled = Column(DateTime, nullable=True, default=None)
    # created_at = Column(DateTime, default=datetime.now)
    # updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    __table_args__ = (
        Index('idx_ip', 'ip'),
        Index('idx_ip_last_crawled', 'last_crawled'),
    )


class PageTable(Base, RepresentableTable):
    __tablename__ = "pages"
    
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
    
    __table_args__ = (
        Index('idx_page_url', 'page_url'),
        Index('idx_page_table_last_crawled', 'last_crawled'),
    )


class DocumentIndexTable(Base, RepresentableTable):
    __tablename__ = "document_index"

    document_url = Column(String(255), primary_key=True)  # pages.page_url
    word = Column(String(255), primary_key=True)
    frequency = Column(Integer)
    location = Column(Integer, primary_key=True)
    tag = Column(String(50))  # e.g., 'p', 'h1', 'title'
    
    __table_args__ = (
        Index('idx_document_url', 'document_url'),
        Index('idx_word', 'word'),
    )


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

###########################################################################################################

from enum import Enum
from typing import List, Union

from pydantic import BaseModel

class WordFrequency(BaseModel):
    word: str
    frequency: int
    location_index: int
    tag: str

class Document(BaseModel):
    url: str|int
    word_frequencies: list[WordFrequency]

class PageScore(BaseModel):
    document: Document
    score: float



class FailEnum(Enum):
    INVALID_STATUS_CODE = 0  # 404, 500, etc.
    NOT_AVAILABLE = 1  # DNS resolution, connection timeout, etc.
    NOT_TURKISH = 2  # Content-Language, meta tags, etc.
    NO_CONTENT = 3  # empty response body
    INVALID_CONTENT_TYPE = 4  # not text/html


class FailReasonWeight(BaseModel):
    INVALID_STATUS_CODE: float
    NOT_AVAILABLE: float
    NOT_TURKISH: float

class MaxWorkerConfig(BaseModel):
    ip_search: int
    url_frontier: int
    page_search: int

class CrawlerConfig(BaseModel):
    parallelism: int  # number of programs running at the same time
    max_workers: MaxWorkerConfig  # number of threads in the pool
    chunk_size: int
    req_timeout: int
    user_agent: str
    allowed_protocols: list[str]
    retry_after_minutes: int
    fail_reason_weights: FailReasonWeight
    max_document_length: int
    ports: List[int]
    shuffle_chunks: bool

class SystemConfig(BaseModel):
    machine_id: int
    total_machines: int

class Config(BaseModel):
    crawler: CrawlerConfig
    system: SystemConfig


class LinkType(Enum):
    # example: https://www.example.com
    INTERNAL = 0  # /abc or https://www.example.com/abc
    EXTERNAL = 1  # https://www.anotherexample.com
    INVALID = 2  # mailto:info@example, tel:123456789, #title, etc.


class Link(BaseModel):
    type: LinkType
    base_url: str
    href: str
    anchor_text: Union[str, None]

    @property
    def full_url(self):
        if self.type == LinkType.INTERNAL and self.href.startswith("/"):
            return f"{self.base_url}{self.href}"
        return self.href


class MetaTags(BaseModel):
    title: Union[str, None]
    description: Union[str, None]
    keywords: Union[str, None]
