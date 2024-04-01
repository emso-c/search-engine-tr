from typing import List, Optional
from datetime import datetime

from src.models import IPTable
from src.services import BaseService


class IPService(BaseService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
    
    def add_ip(self, ip_obj: IPTable) -> IPTable:
        """Add a new IP to the database."""
        with self.db_adapter.get_session() as session:
            session.add(ip_obj)
            # session.commit()
            return ip_obj

    def get_ips(self) -> List[IPTable]:
        """Get all IPs from the database."""
        with self.db_adapter.get_session() as session:
            return session.query(IPTable).all()
    
    def get_ip(self, ip: str) -> Optional[IPTable]:
        """Get a specific IP from the database."""
        with self.db_adapter.get_session() as session:
            return session.query(IPTable).filter_by(ip=ip).first()

    def update_ip(self, new_ip_obj: IPTable) -> IPTable:
        """Update an existing IP in the database."""
        with self.db_adapter.get_session() as session:
            existing_ip_obj = session.query(IPTable).filter_by(ip=new_ip_obj.ip).first()
            existing_ip_obj.domain = new_ip_obj.domain
            existing_ip_obj.port = new_ip_obj.port
            existing_ip_obj.status = new_ip_obj.status
            existing_ip_obj.updated_at = datetime.now()
            existing_ip_obj.keywords = new_ip_obj.keywords or existing_ip_obj.keywords
            existing_ip_obj.title = new_ip_obj.title or existing_ip_obj.title
            existing_ip_obj.description = new_ip_obj.description or existing_ip_obj.description
            existing_ip_obj.body = new_ip_obj.body or existing_ip_obj.body
            # session.commit()
            return existing_ip_obj

    def delete_ip(self, ip: str) -> IPTable:
        """Delete a specific IP from the database."""
        with self.db_adapter.get_session() as session:
            ip_obj = session.query(IPTable).filter_by(ip=ip).first()
            session.delete(ip_obj)
            # session.commit()
            return ip_obj

    def delete_all_ips(self, commit) -> bool:
        """Delete all IPs from the database."""
        with self.db_adapter.get_session() as session:
            session.query(IPTable).delete()
            if commit:
                session.commit()
            return True
    
    def get_valid_ips(self) -> List[IPTable]:
        """Get all valid IPs from the database."""
        with self.db_adapter.get_session() as session:
            return session.query(IPTable).filter_by(status=200).all()

    def upsert_ip(self, new_ip_obj:IPTable) -> IPTable:
        """Add a new IP or update an existing one in the database."""
        session = self.db_adapter.get_session()
        ip_obj = session.query(IPTable).filter_by(ip=new_ip_obj.ip).first()
        if ip_obj:
            self.update_ip(new_ip_obj)
        else:
            self.add_ip(new_ip_obj)
        # session.commit()
        return ip_obj
    
    def commit(self):
        """Commit the current transaction."""
        if self.db_adapter.get_session().dirty \
            or self.db_adapter.get_session().deleted \
            or self.db_adapter.get_session().new:
            self.db_adapter.get_session().commit()
        print("Warning: No changes to commit.")
    
    # def queue_operation(self, func:str, *args, **kwargs):
    #     """Queue an operation to be executed later."""
    #     if func not in dir(self):
    #         raise ValueError(f"Invalid function: {func}")
    #     self._queue.append((func, args, kwargs))

    # def execute_queue(self):
    #     """Execute all queued operations in a single transaction."""
    #     if not self._queue:
    #         return
    #     with self.db_adapter.get_session() as session:
    #         for func, args, kwargs in self._queue:
    #             func = str(func)
    #             self.__getattribute__(func)(*args, **kwargs)
    #         session.commit()
    #     self._queue = []



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