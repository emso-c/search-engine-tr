from typing import List, Optional
from datetime import datetime

from sqlalchemy import func

from src.models import IPTable
from src.services import BaseService


class IPService(BaseService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self.model = IPTable
    
    def add_ip(self, ip_obj: IPTable) -> IPTable:
        """Add a new IP to the database."""
        session = self.db_adapter.get_session()
        session.add(ip_obj)
        return ip_obj

    def safe_add_url(self, ip_obj: IPTable) -> bool:
        """Add a new IP to the database if it does not already exist."""
        session = self.db_adapter.get_session()
        searched_ip = session.query(IPTable).filter(IPTable.domain == ip_obj.domain).first()
        if not searched_ip:
            session.add(ip_obj)
            return True
        return False
    
    def increment_ip_score(self, ip: str, score: float) -> IPTable:
        """Increment the score of an IP in the database."""
        session = self.db_adapter.get_session()
        ip_obj = session.query(IPTable).filter_by(ip=ip).first()
        if not ip_obj:
            raise ValueError(f"Cant find IP with ip: {ip} in the database.")
        ip_obj.score += score
        return ip_obj

    def get_ips(self) -> List[IPTable]:
        """Get all IPs from the database."""
        return self.db_adapter.get_session().query(IPTable).all()
    
    def get_ip_by_domain(self, domain: str) -> IPTable:
        """Get all IPs with a specific domain from the database."""
        session = self.db_adapter.get_session()
        return session.query(IPTable).filter_by(domain=domain).first() # TODO could there be multiple domains with different ip's? 
    
    def get_ip(self, ip: str) -> Optional[IPTable]:
        """Get a specific IP from the database."""
        self.db_adapter.get_session().query(IPTable).filter_by(ip=ip).first()

    def update_ip(self, new_obj: IPTable) -> IPTable:
        """Update an existing IP in the database."""
        session = self.db_adapter.get_session() 
        updated_obj = session.query(IPTable).filter_by(ip=new_obj.ip, domain=new_obj.domain).first()
        if not updated_obj:
            raise ValueError(f"Cant find IP with ip: {new_obj.ip} and domain: {new_obj.domain} in the database.")

        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_") and attr not in ["created_at", "updated_at", "score"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        setattr(updated_obj, "updated_at", datetime.now())
        setattr(updated_obj, "score", new_obj.score)
        
        return updated_obj

    def delete_ip(self, ip: str) -> IPTable:
        """Delete a specific IP from the database."""
        session = self.db_adapter.get_session()
        ip_obj = session.query(IPTable).filter_by(ip=ip).first()
        session.delete(ip_obj)
        session.flush()
        return ip_obj

    def delete_all_ips(self, commit) -> bool:
        """Delete all IPs from the database."""
        session = self.db_adapter.get_session()
        session.query(IPTable).delete()
        if commit:
            session.commit()
        session.flush()
        return True
    
    def get_valid_ips(self) -> List[IPTable]:
        """Get all valid IPs from the database."""
        session = self.db_adapter.get_session()
        return session.query(IPTable).filter_by(status=200).all()

    def upsert_ip(self, new_ip_obj:IPTable) -> IPTable:
        """Add a new IP or update an existing one in the database."""
        session = self.db_adapter.get_session()
        ip_obj = session.query(IPTable).filter_by(ip=new_ip_obj.ip, domain=new_ip_obj.domain).first()
        if ip_obj:
            self.update_ip(new_ip_obj)
        else:
            self.add_ip(new_ip_obj)
        return ip_obj
    
    def remove_duplicates(self, column=IPTable.domain) -> bool:
        """Remove duplicate IPs from the database."""
        session = self.db_adapter.get_session()
        session.query(IPTable).filter(column.in_(
            session.query(column).group_by(column).having(func.count(column) > 1)
        )).delete(synchronize_session=False)
        session.commit()
        return True
    
    
    # def queue_operation(self, func:str, *args, **kwargs):
    #     """Queue an operation to be executed later."""
    #     if func not in dir(self):
    #         raise ValueError(f"Invalid function: {func}")
    #     self._queue.append((func, args, kwargs))

    # def execute_queue(self):
    #     """Execute all queued operations in a single transaction."""
    #     if not self._queue:
    #         return
    #     session = self.db_adapter.get_session()
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