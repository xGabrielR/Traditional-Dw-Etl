import asyncio
from datetime import datetime
from package.utils.credentials import credentials
from package.core.stagings import RunStagings
from package.core.schemas import RunSchemas

from warnings import filterwarnings
filterwarnings('ignore')

LOGS_PATH = f"/home/grc/arep/Traditional-Dw-Etl/traditional_dw_etl/logs_{datetime.now().strftime('%Y-%m-%d')}.txt"
DW_SCHEMAS_PATH = "/home/grc/arep/Traditional-Dw-Etl/sql/dw_schema"
MYSQL_QUERY_PATH = "/home/grc/arep/Traditional-Dw-Etl/sql/stages_mysql"
POSTGRESQL_QUERY_PATH = "/home/grc/arep/Traditional-Dw-Etl/sql/stages_postgresql"

stages = RunStagings(
    stagings_path=[MYSQL_QUERY_PATH, POSTGRESQL_QUERY_PATH],
    credentials_dict=credentials,
    schema='StgOls',
    logs_path=LOGS_PATH,
    stagings_backend_config = None
)

schemas = RunSchemas(
    schemas_path=DW_SCHEMAS_PATH,
    credentials_dict=credentials,
    logs_path=LOGS_PATH
)

async def main():
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | STARTING ETL STAGING', file=open(LOGS_PATH, 'a'))
    await stages.run_stagings()

    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | STARTING ETL DMS', file=open(LOGS_PATH, 'a'))
    await schemas.run_schemas()

    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | PROCESS ENDED', file=open(LOGS_PATH, 'a'))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
