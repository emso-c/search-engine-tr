from typing import List, Optional
from datetime import datetime

from sqlalchemy import func, select, union_all

from src.models import IPTableBase
from src.services import PartitionedService


class IPService(PartitionedService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self.base_type = IPTableBase
    
    def add_ip(self, ip_obj: IPTableBase) -> IPTableBase:
        """Add a new IP to the database."""
        session = self.db_adapter.get_session()
        
        table = IPTableBase.get_partition_tablename(ip_obj.domain)
        DynamicModel = self.get_model(table)
        session.add(ip_obj)
        return DynamicModel
    
    def get_unscanned_ips(self):
        queries = []
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            query = select(DynamicTable).where(DynamicTable.last_crawled == None)
            queries.append(query)

        fetch_all_query = union_all(*queries)
        result = self.db_adapter.get_session().execute(fetch_all_query).all()
        rows = self.rows_to_objects(result)
        return rows

    def safe_add_url(self, ip_obj: IPTableBase) -> bool:
        """Add a new IP to the database if it does not already exist."""
        session = self.db_adapter.get_session()
        table = IPTableBase.get_partition_tablename(ip_obj.domain)
        DynamicModel = self.get_model(table)
        searched_ip = session.query(DynamicModel).filter(DynamicModel.domain == ip_obj.domain).first()
        if not searched_ip:
            session.add(ip_obj)
            return True
        return False

    def get_ips(self) -> List[IPTableBase]:
        """Get all IPs from the database."""
        return self.get_all()
    
    def get_ip_by_domain(self, domain: str) -> IPTableBase:
        """Get all IPs with a specific domain from the database."""
        session = self.db_adapter.get_session()
        table = IPTableBase.get_partition_tablename(domain)
        DynamicTable = self.get_model(table)
        return session.query(DynamicTable).filter(DynamicTable.domain==domain).first() # TODO could there be multiple domains with different ip's? 

    def update_ip(self, new_obj: IPTableBase) -> IPTableBase:
        """Update an existing IP in the database."""
        session = self.db_adapter.get_session()
        
        table = IPTableBase.get_partition_tablename(new_obj.domain)
        DynamicModel = self.get_model(table)
        
        updated_obj = session.query(DynamicModel).filter_by(ip=new_obj.ip, domain=new_obj.domain).first()
        if not updated_obj:
            raise ValueError(f"Cant find IP with ip: {new_obj.ip} and domain: {new_obj.domain} in the database.")

        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_") and attr not in ["created_at", "updated_at", "score"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        setattr(updated_obj, "score", new_obj.score)
        
        return updated_obj

    def get_valid_ips(self):
        queries = []
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            query = self.db_adapter.get_session().query(DynamicTable).filter_by(status=200)
            queries.append(query)
        
        fetch_all_query = union_all(*queries)
        result = self.db_adapter.get_session().execute(fetch_all_query).fetchall()

        return self.rows_to_objects(result)
    
    def upsert_ip(self, new_ip_obj:IPTableBase) -> IPTableBase:
        """Add a new IP or update an existing one in the database."""
        session = self.db_adapter.get_session()
        
        table = IPTableBase.get_partition_tablename(new_ip_obj.domain)
        DynamicModel = self.get_model(table)
        
        ip_obj = session.query(DynamicModel).filter_by(ip=new_ip_obj.ip, domain=new_ip_obj.domain).first()
        if ip_obj:
            self.update_ip(new_ip_obj)
        else:
            self.add_ip(new_ip_obj)
        return ip_obj
    
    def remove_duplicates(self) -> bool:
        """Remove duplicate IPs from the database."""
        
        session = self.db_adapter.get_session()
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            column = DynamicTable.domain
            session.query(DynamicTable).filter(column.in_(
                session.query(column).group_by(column).having(func.count(column) > 1)
            )).delete(synchronize_session=False)
            session.commit()
        return True



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