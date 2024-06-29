from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel

class WordFrequency(BaseModel):
    word: str
    frequency: int
    location_index: int
    tag: str

class Document(BaseModel):
    url: str
    title: str = None
    description: str = None
    word_frequencies: list[WordFrequency]

class PageScore(BaseModel):
    document: Document
    idf_score: float



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

class UniformResponse(BaseModel):
    """
    A single source of truth for all kinds of responses from different libraries
    """
    url: str
    status_code: int
    headers: dict
    body: Optional[str]
    content_bytes: Optional[bytes]
