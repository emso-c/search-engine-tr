from typing import List, Optional
from datetime import datetime
from sqlalchemy import func, select, union_all

from src.models import PageTableBase
from src.services import PartitionedService


class PageService(PartitionedService):
    def __init__(self, db_adapter):
        super().__init__(db_adapter)
        self.base_type = PageTableBase
    
    def get_pages(self) -> List[PageTableBase]:
        """Get all pages from the database."""
        return self.get_all()
    
    def get_unscanned_pages(self):
        queries = []
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            query = select(DynamicTable).where(DynamicTable.last_crawled == None)
            queries.append(query)

        fetch_all_query = union_all(*queries)
        rows = self.db_adapter.get_session().execute(fetch_all_query).all()
        rows = self.rows_to_objects(rows)
        return rows
    
    def generate_page_obj(self, page_url, title, status_code, keywords, description, body, favicon, robotstxt, sitemap, last_crawled):
        tablename = self.base_type.get_partition_tablename(page_url)
        PageTable = self.get_model(tablename)
        return PageTable(
            page_url=page_url,
            title=title,
            status_code=status_code,
            keywords=keywords,
            description=description,
            body=body,
            favicon=favicon,
            robotstxt=robotstxt,
            sitemap=sitemap,
            last_crawled=last_crawled,
        )
    
    
    def count_unscanned_pages(self):
        """Count the number of unscanned pages."""
        total = 0
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.db_adapter.get_model(table_name, self.base_type)
            stmt = self.db_adapter.get_session().query(func.count()).select_from(DynamicTable).filter(DynamicTable.last_crawled == None)
            total += stmt.scalar()
        return total

    def get_page(self, page_url: str) -> Optional[PageTableBase]:
        """Get a specific page from the database."""
        session = self.db_adapter.get_session()
        table = PageTableBase.get_partition_tablename(page_url)
        model = self.get_model(table)
        page = session.query(model).filter(model.page_url == page_url).first()
        return page
    
    def add_page(self, new_obj: PageTableBase) -> PageTableBase:
        """Add a new page to the database."""
        session = self.db_adapter.get_session()
        table = PageTableBase.get_partition_tablename(new_obj.page_url)
        DynamicModel = self.get_model(table)
        session.add(new_obj)
        return DynamicModel

    def update_page(self, new_obj: PageTableBase) -> PageTableBase:
        """Update an existing page in the database."""
        session = self.db_adapter.get_session()
        
        table = PageTableBase.get_partition_tablename(new_obj.page_url)
        DynamicModel = self.get_model(table)
        
        updated_obj = session.query(DynamicModel).filter_by(page_url=new_obj.page_url).first()
        if not updated_obj:
            raise ValueError(f"Cant find page with url: {new_obj.page_url} in the database.")

        for attr in [attr for attr in dir(new_obj) if not attr.startswith("_")  and attr not in ["created_at", "updated_at"]]:
            setattr(updated_obj, attr, getattr(new_obj, attr))
        setattr(updated_obj, "updated_at", datetime.now())
        return updated_obj
    
    def delete_page(self, page_url: str) -> PageTableBase:
        """Delete a specific page from the database."""
        session = self.db_adapter.get_session()
        table = PageTableBase.get_partition_tablename(page_url)
        DynamicModel = self.get_model(table)
        
        page = session.query(DynamicModel).filter(DynamicModel.page_url == page_url).first()
        session.delete(page)
        return page
    
    def upsert_page(self, new_page: PageTableBase) -> PageTableBase:
        """Update or insert a page in the database."""
        session = self.db_adapter.get_session()
        table = PageTableBase.get_partition_tablename(new_page.page_url)
        DynamicModel = self.get_model(table)
        
        page = session.query(DynamicModel).filter(DynamicModel.page_url == new_page.page_url).first()
        if page:
            return self.update_page(new_page)
        return self.add_page(new_page)