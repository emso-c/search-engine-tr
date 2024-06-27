from src.services import BaseService
from sqlalchemy import func, union_all

class PartitionedService(BaseService):
    def get_model(self, table_name):
        return self.db_adapter.get_model(table_name, self.base_type)
    
    def count(self):
        """Return the number of items in the database across all partitioned tables."""
        total = 0
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.db_adapter.get_model(table_name, self.base_type)
            stmt = self.db_adapter.get_session().query(func.count()).select_from(DynamicTable)
            total += stmt.scalar()
        return total
    
    def get_all(self):
        queries = []
        for key in self.base_type.partition_keys + ["default"]:
            table_name = f"{self.base_type.__basename__}_{key}"
            DynamicTable = self.get_model(table_name)
            query = self.db_adapter.get_session().query(DynamicTable)
            queries.append(query)
        
        fetch_all_query = union_all(*queries)
        result = self.db_adapter.get_session().execute(fetch_all_query).fetchall()

        return self.rows_to_objects(result)

    def rows_to_objects(self, result):
        objects = []
        for row in result:
            for key in self.base_type.partition_keys + ["default"]:
                table_name = f"{self.base_type.__basename__}_{key}"
                DynamicTable = self.get_model(table_name)
                obj = DynamicTable()
                for i, column in enumerate(DynamicTable.__table__.columns):
                    setattr(obj, column.name, row[i])
                objects.append(obj)
                break
        
        return objects

    def generate_obj(self, partition_key_name, **kwargs):
        tablename = self.base_type.get_partition_tablename(kwargs[partition_key_name])
        DynamicTable = self.get_model(tablename)
        return DynamicTable(**kwargs)