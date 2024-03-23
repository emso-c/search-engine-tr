from typing import List, Optional
from datetime import datetime

from src.models import IPTable
from src.services import BaseService


class IPService(BaseService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self._queue = []
    
    def add_ip(self, ip: str, domain: str, port: int, status: int,
                keywords: str = None, title: str = None,
                description: str = None, body: str = None) -> IPTable:
        """Add a new IP to the database."""
        with self.db_adapter.get_session() as session:
            ip_obj = IPTable(
                ip=ip, domain=domain, port=port, status=status,
                keywords=keywords, title=title, description=description,
                body=body
            )
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

    def update_ip(self, ip: str, domain: str, port: int, status: int,
                  keywords: str = None, title: str = None,
                  description: str = None, body: str = None) -> IPTable:
        """Update an existing IP in the database."""
        with self.db_adapter.get_session() as session:
            ip_obj = session.query(IPTable).filter_by(ip=ip).first()
            ip_obj.domain = domain
            ip_obj.port = port
            ip_obj.status = status
            ip_obj.updated_at = datetime.now()
            ip_obj.keywords = keywords or ip_obj.keywords
            ip_obj.title = title or ip_obj.title
            ip_obj.description = description or ip_obj.description
            ip_obj.body = body or ip_obj.body
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

    def upsert_ip(self, ip: str, domain: str, port: int, status: int,
                  keywords: str = None, title: str = None,
                  description: str = None, body: str = None) -> IPTable:
        """Add a new IP or update an existing one in the database."""
        session = self.db_adapter.get_session()
        ip_obj = session.query(IPTable).filter_by(ip=ip).first()
        if ip_obj:
            ip_obj = self.update_ip(ip, domain, port, status, keywords, title, description, body)
        else:
            ip_obj = self.add_ip(ip, domain, port, status, keywords, title, description, body)
        session.commit()
        return ip_obj
    
    def queue_operation(self, func, *args, **kwargs):
        """Queue an operation to be executed later."""
        self._queue.append((func, args, kwargs))
    
    def execute_queue(self):
        """Execute all queued operations in a single transaction."""
        if not self._queue:
            return
        with self.db_adapter.get_session() as session:
            for func, args, kwargs in self._queue:
                func(session, *args, **kwargs)
            session.commit()
        self._queue = []



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