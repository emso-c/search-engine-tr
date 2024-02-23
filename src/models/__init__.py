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
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)