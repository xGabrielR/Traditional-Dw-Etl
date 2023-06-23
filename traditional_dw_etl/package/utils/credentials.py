import os

credentials = dict(
    SSMS_HOST  = os.environ.get("SSMS_HOST"),
    SSMS_DB    = os.environ.get("SSMS_DB"),
    SSMS_PORT  = os.environ.get("SSMS_PORT"),
    SSMS_USER  = os.environ.get("SSMS_USER"),
    SSMS_PSWD  = os.environ.get("SSMS_PSWD"),
    MYSQL_HOST = os.environ.get("MYSQL_HOST"),
    MYSQL_DB   = os.environ.get("MYSQL_DB"),
    MYSQL_PORT = os.environ.get("MYSQL_PORT"),
    MYSQL_USER = os.environ.get("MYSQL_USER"),
    MYSQL_PSWD = os.environ.get("MYSQL_PSWD"),
    PG_HOST    = os.environ.get("PG_HOST"),
    PG_DB      = os.environ.get("PG_DB"),
    PG_PORT    = os.environ.get("PG_PORT"),
    PG_USER    = os.environ.get("PG_USER"),
    PG_PSWD    = os.environ.get("PG_PSWD"),
)