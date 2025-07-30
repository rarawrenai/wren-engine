from ibis import util
from ibis.backends.clickhouse import Backend

def custom_get_schema_using_query(self, query):
    print("Running patched_get_schema_using_query")
    name = util.gen_name("clickhouse_metadata") 
    self.raw_sql(f"CREATE VIEW {name} ON CLUSTER 'ch_cluster' AS {query}")
    try:
        return self.get_schema(name)
    finally:
        self.raw_sql(f"DROP VIEW {name} ON CLUSTER 'ch_cluster'")
    
Backend._get_schema_using_query = custom_get_schema_using_query

