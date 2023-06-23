from databases import Database
from sqlalchemy import create_engine, event

def get_backend_uri(backend: str, credentials_dict: str = None):
    """
    Get SQLAlchemy uri given a backend engine
    """
    if backend == 'mysql':
        return f"mysql+pymysql://{credentials_dict['MYSQL_USER']}:{credentials_dict['MYSQL_PSWD']}@{credentials_dict['MYSQL_HOST']}:{credentials_dict['MYSQL_PORT']}/{credentials_dict['MYSQL_DB']}"

    elif backend == 'sqlserver':
        return f"mssql+pyodbc:///?odbc_connect=DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={credentials_dict['SSMS_HOST']};DATABASE={credentials_dict['SSMS_DB']};UID={credentials_dict['SSMS_USER']};PWD={credentials_dict['SSMS_PSWD']}"

    elif backend == 'postgresql':
        return f"postgresql://{credentials_dict['PG_USER']}:{credentials_dict['PG_PSWD']}@{credentials_dict['PG_HOST']}:{credentials_dict['PG_PORT']}/{credentials_dict['PG_DB']}"

    else:
        raise ValueError('Invalid Backend') 

async def get_database_connection(connection_string: str) -> Database:
    """
    Get async connection given connection string.
    """
    try:
        database = Database(connection_string)
        await database.connect()
        
        return database

    except Exception as e:
        return e

def get_ssms_connection(connection_string: str):
    """
    Get sql server engine with fast execute many
    """
    engine = create_engine(connection_string)

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
            cursor.commit()

    return engine