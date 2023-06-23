import asyncio
from glob import glob
from pandas import DataFrame
from databases import Database
from datetime import datetime, timedelta

from package.utils.deco_functions import retry_get_stagging_data, mem_profile
from package.database.connections import (
    get_backend_uri,
    get_ssms_connection,
    get_database_connection,
)

class SetupStages(object):
    """
    Initial class for setup and checkout all staggings from mysql and postgresql.
    """
    def __init__(
            self, 
            staggings_path: str | list,
            credentials_dict: dict,
            lookback_process_days: int = None,
            config_backend_dict: dict = None
        ):
        self.staggings_query = None
        self.staggings_names = None
        self.staggings_path = staggings_path
        self._process_date = lookback_process_days

        if type(staggings_path) not in [str, list]:
            raise TypeError('Staggings Path need to be a String path or List of paths.')

        if isinstance(staggings_path, list):
            self.backend_query_setup = []

            for query_path in staggings_path:
                backend = query_path.split('_')[-1]
                if backend not in ['mysql', 'sqlserver', 'postgresql']:
                    raise ValueError(f"Backend: {backend} Not Supported. Supported Only: 'mysql', 'sqlserver', 'postgresql'")

                stg_names, stg_query = self.checkout_stages_inputs(query_path)

                self.backend_query_setup.append({backend: [stg_names, stg_query]})

        if isinstance(staggings_path, str):
            stg_name, stg_query = self.checkout_stages_inputs(staggings_path)
            self.backend_query_setup = [{staggings_path.split('_')[-1]: [stg_name, stg_query]}]

    def __len__(self):
        return len(self.backend_query_setup)
        
    def __iter__(self):
        return iter(self.backend_query_setup)

    def checkout_stages_inputs(self, staggings_path):
        all_stages_paths = glob(staggings_path + '/*.sql')
        all_stages_names = [j.split('/')[-1].replace('.sql', '') for j in all_stages_paths]
        
        all_stages_query = []
        for stg_path in all_stages_paths:
            with open(stg_path) as f:
                try:
                    stage_query = " ".join(f.readlines()).replace('\n', ' ').replace('\t', ' ').strip()

                    all_stages_query.append(stage_query)
                
                except Exception as e:
                    print('Error on Load Query File')

            f.close()
        
        if self._process_date:
            process_date = (datetime.now() - timedelta(days=self._process_date)).strftime("%Y-%m-%d")
            all_stages_query = list(map(lambda j: j.format(process_date), all_stages_query))

        return all_stages_names, all_stages_query
    
    
class RunStaggings(object):
    """
    Auxiliar class for extract and load process.
    """
    def __init__(
            self, 
            staggings_path: str | list,
            credentials_dict: dict,
            schema: str = None,
            logs_path: str = None,
            lookback_process_days: int = None,
            staggings_backend_config: dict = None
        ):
        self._logs_path = logs_path
        self._credentials_dict = credentials_dict
        self._schema=schema
        self.staggings_config = SetupStages(
            staggings_path=staggings_path,
            credentials_dict=credentials_dict,
            lookback_process_days=lookback_process_days
        )

    def get_backends(self, staggings_config):
        backends = ["".join(k.keys()) for k in list(staggings_config)]

        return {backend: get_backend_uri(backend, self._credentials_dict) for backend in backends}
    

    async def get_connections(self, only_ssms=False):
        if only_ssms == True:
            connections = {'sqlserver': get_ssms_connection(get_backend_uri('sqlserver', self._credentials_dict))}
            
            return connections

        else:
            connections = {}
            for backend, uri in self.get_backends(self.staggings_config).items():
                con = await get_database_connection(uri)
                connections[backend] = con
            
            for backend, con in connections.items():
                if not type(con) == Database:
                    raise ValueError(f"Error on '{backend}' Connection, '{con}'")

            connections['sqlserver'] = get_ssms_connection(get_backend_uri('sqlserver', self._credentials_dict))

            return connections
    

    async def stagging_raw_data(self, connections):
        raw_stg_data = []
        for itters in list(self.staggings_config):
            backend = "".join(itters.keys())
            connection = connections[backend]

            stg_names, stg_queries = list(itters.values())[0]

            for stg_name, stg_query in zip(stg_names, stg_queries):
                print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | START STAGGING: {stg_name.upper()}', file=open(self._logs_path, 'a'))

                raw_data = await self.get_stagging_data(connection, stg_query, stg_name)
                raw_stg_data.append(raw_data)

        return raw_stg_data
    

    @retry_get_stagging_data()
    async def get_stagging_data(self, connection, stg_query, stg_name) -> dict:
        rows = await connection.fetch_all(query=stg_query)
        rows = [dict(k._mapping) | {'DATA_PROCESSAMENTO': datetime.now()} for k in rows]
        
        return {stg_name: rows}
    
    
    async def data_storange(self, engine, stage: dict, schema: str) -> None:
        table_name, rows = list(stage.items())[0]
        
        df = DataFrame(rows)
        df.columns = [k.strip().upper() for k in df.columns]
        
        # Skip Retry Empty Stagging Collect
        if not df.empty:
            df.to_sql(
                table_name.upper(),
                con=engine,
                schema=schema,
                index=False,
                if_exists='replace'
            )

    @mem_profile
    async def run_staggings(self):
        connections = await self.get_connections()

        raw_stg_data = await self.stagging_raw_data(connections)

        tasks_data_insert = [
            asyncio.ensure_future(self.data_storange(engine=connections['sqlserver'], stage=stage, schema=self._schema)) 
            for stage in raw_stg_data
        ]
        
        await asyncio.gather(*tasks_data_insert)

        try:
            for backend, con in connections.items():
                if backend in ['postgresql', 'mysql']:
                    con.disconnect()

        except:
            raise AttributeError(f"Impossible to Disconnect From Connection, Check Logs ({self._logs_path}) for More Details.")