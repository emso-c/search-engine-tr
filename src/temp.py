from typing import List, Optional, Union
from datetime import datetime
from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Session

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


class DBAdapter:
    def __init__(self, db_url: str):
        """Initialize the DBAdapter with a database URL."""
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
    
    def __enter__(self) -> 'DBAdapter':
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.engine.dispose()
        self.Session

    def get_session(self) -> Session:
        """Get a new Session."""
        return self.Session(expire_on_commit=False)

    def delete_db(self):
        """Delete the database."""
        print("WARNING: Deleting database!")
        Base.metadata.drop_all(self.engine)


class BaseService:
    def __init__(self, db_adapter: DBAdapter):
        """Initialize the BaseService with a DBAdapter."""
        self.db_adapter = db_adapter
        Base.metadata.create_all(self.db_adapter.engine)


class IPService(BaseService):
    def add_ip(self, ip: str, domain: str, port: int, status: int) -> IPTable:
        """Add a new IP to the database."""
        with self.db_adapter.get_session() as session:
            ip_obj = IPTable(ip=ip, domain=domain, port=port, status=status)
            session.add(ip_obj)
            session.commit()
            return ip_obj

    def get_ips(self) -> List[IPTable]:
        """Get all IPs from the database."""
        with self.db_adapter.get_session() as session:
            return session.query(IPTable).all()
    
    def get_ip(self, ip: str) -> Optional[IPTable]:
        """Get a specific IP from the database."""
        with self.db_adapter.get_session() as session:
            return session.query(IPTable).filter_by(ip=ip).first()

    def update_ip(self, ip: str, domain: str, port: int, status: int) -> IPTable:
        """Update an existing IP in the database."""
        with self.db_adapter.get_session() as session:
            ip_obj = session.query(IPTable).filter_by(ip=ip).first()
            ip_obj.domain = domain
            ip_obj.port = port
            ip_obj.status = status
            ip_obj.updated_at = datetime.now()
            session.commit()
            return ip_obj

    def delete_ip(self, ip: str) -> IPTable:
        """Delete a specific IP from the database."""
        with self.db_adapter.get_session() as session:
            ip_obj = session.query(IPTable).filter_by(ip=ip).first()
            session.delete(ip_obj)
            session.commit()
            return ip_obj

    def delete_all_ips(self) -> bool:
        """Delete all IPs from the database."""
        with self.db_adapter.get_session() as session:
            session.query(IPTable).delete()
            session.commit()
            return True
    
    def get_valid_ips(self) -> List[IPTable]:
        """Get all valid IPs from the database."""
        with self.db_adapter.get_session() as session:
            return session.query(IPTable).filter_by(status=200).all()

    def upsert_ip(self, ip: str, domain: str, port: int, status: int) -> IPTable:
        """Add a new IP or update an existing one in the database."""
        session = self.db_adapter.get_session()
        ip_obj = session.query(IPTable).filter_by(ip=ip).first()
        if ip_obj:
            ip_obj = self.update_ip(ip, domain, port, status)
        else:
            ip_obj = self.add_ip(ip, domain, port, status)
        session.commit()
        return ip_obj



# example usage

# temporary in-memory database to test the service
# db_adapter = DBAdapter("sqlite:///:memory:")

# with db_adapter as adap:
#     ip_service = IPService(adap)
#     print("initial ips:", len(ip_service.get_ips()))

#     ip_service.upsert_ip("123.123.123.123", "http://example.com", 80, 200)
#     ip_service.upsert_ip("1.1.1.1", "https://example2.com", 443, 200)
#     ip_service.upsert_ip("69.69.69.69", "https://example3.com", 443, 400)

#     print(len(ip_service.get_ips()))
    
#     ip_service.delete_ip("69.69.69.69")
#     print(len(ip_service.get_ips()))

#     ip_service.delete_all_ips()
#     print(len(ip_service.get_ips()))

#     adap.delete_db()
#     try:
#         print(len(ip_service.get_ips()))
#     except Exception as e:
#         print("table not found")