from datetime import datetime
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class RepresentableTable:
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.__dict__})>"


class IPTable(Base, RepresentableTable):
    __tablename__ = "ips"

    ip = Column(String, primary_key=True)
    domain = Column(String, nullable=True)
    port = Column(Integer)
    status = Column(Integer)
    title = Column(String, nullable=True)
    keywords = Column(String, nullable=True)
    description = Column(String, nullable=True)
    body = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


###########################################################################################################

from enum import Enum
from typing import List, Union

from pydantic import BaseModel


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


class CrawlerConfig(BaseModel):
    max_workers: int
    chunk_size: int
    req_timeout: int
    user_agent: str
    allowed_protocols: list[str]
    retry_after_minutes: int
    fail_reason_weights: FailReasonWeight
    ports: tuple[int]


class Config(BaseModel):
    crawler: CrawlerConfig


class LinkType(Enum):
    # example: https://www.example.com
    INTERNAL = 0  # /abc or https://www.example.com/abc
    EXTERNAL = 1  # https://www.anotherexample.com
    INVALID = 2  # mailto:info@example, tel:123456789, #title, etc.


class Link(BaseModel):
    type: LinkType
    base_url: str
    href: str

    @property
    def full_url(self):
        if self.type == LinkType.INTERNAL and self.href.startswith("/"):
            return f"{self.base_url}{self.href}"
        return self.href


class MetaTags(BaseModel):
    title: Union[str, None]
    description: Union[str, None]
    keywords: Union[List[str], None]
