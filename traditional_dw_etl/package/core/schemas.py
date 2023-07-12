import asyncio
from glob import glob
from datetime import datetime
from package.core.stagings import RunStagings
from package.utils.deco_functions import mem_profile

class SetupSchemas(object):
    """
    Initial Class for Checkout and parse schemas (dims and facts) queries from folder.
    """
    def __init__(
            self, 
            schemas_path
        ):
        self.schemas_path = schemas_path

        if type(schemas_path) != str:
            raise ValueError("Schemas Paths need to be a String for Query Paths.")

    def get_schemas_query(self):
        schema_paths = {}
        for dm in glob(self.schemas_path + '/*'):
            dm_name = dm.split('/')[-1]
            schema_paths[dm_name] = {}

            facts = glob(dm + '/facts/*.sql')
            dims = glob(dm + '/dims/*.sql')

            dims_tables = list(set(["/".join(k.split('/')[-1:]).replace('_update.sql', '').replace('_insert.sql', '') for k in dims]))
            facts_tables = list(set(["/".join(k.split('/')[-1:]).replace('_update.sql', '').replace('_insert.sql', '') for k in facts]))

            for table_name in dims_tables + facts_tables:
                schema_paths[dm_name][table_name] = {}

            for query_path in dims + facts:
                table_name = query_path.split('/')[-1].replace('_update', '').replace('_insert', '').replace('.sql', '').strip()
                table_query_method = query_path.split('/')[-1].split('_')[-1].replace('.sql', '').strip()

                with open(query_path) as f:
                    query = " ".join(f.readlines()).replace('\n', ' ').replace('\t', ' ').strip()

                f.close()
            
                schema_paths[dm_name][table_name][table_query_method] = query
        
        return schema_paths
    
class RunSchemas(RunStagings):
    """
    Auxiliar Class for Run insert and update query.
    """
    def __init__(
            self, 
            schemas_path,
            credentials_dict,
            logs_path
        ):
        self._logs_path = logs_path
        self._credentials_dict = credentials_dict
        
        schemas = SetupSchemas(schemas_path)
        self.schemas_queries = schemas.get_schemas_query()

    async def get_connections(self):
        return await super().get_connections(only_ssms=True)
    
    def execute_schema_query(self, engine):
        for dm in self.schemas_queries.keys():
            datamart = self.schemas_queries[dm]

            for table in datamart.keys():
                print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | START STAGING: {table.upper()}', file=open(self._logs_path, 'a'))

                table_query = datamart[table]

                c = engine.raw_connection()

                c.execute(table_query['insert'])
                c.commit()

                c.execute(table_query['update'])
                c.commit()
                c.close()
    
    @mem_profile
    async def run_schemas(self):
        connections = await self.get_connections()

        self.execute_schema_query(connections['sqlserver'])
