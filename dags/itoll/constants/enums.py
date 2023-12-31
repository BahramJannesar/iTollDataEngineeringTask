from enum import Enum


class StaticsVars(Enum):
    QUERY_PATH_CREATE_USER_TABLE = "QUERY_PATH_CREATE_USER_TABLE"
    QUERY_PATH_CREATE_ORDER_TABLE = "QUERY_PATH_CREATE_ORDER_TABLE"
    QUERY_PATH_CREATE_CAR_TABLE = "QUERY_PATH_CREATE_CAR_TABLE"
    QUERY_PATH_EXTRACT_USER_TABLE = "QUERY_PATH_EXTRACT_USER_TABLE"
    QUERY_PATH_EXTRACT_ORDER_TABLE = "QUERY_PATH_EXTRACT_ORDER_TABLE"
    QUERY_PATH_EXTRACT_CAR_TABLE = "QUERY_PATH_EXTRACT_CAR_TABLE"
    CREATE_TABLE = "CREATE_TABLES"
    ETL_QUERY = "ETL_QUERY"

    CLICKHOUSE_HTTP_CONNECTION = "clickhouse_http_connection"
    HOST = 'host'
    PORT = 'port'
    DATABASE = 'database'
    USER = 'user'
    PASSWORD = 'password'

    @property
    def str_value(self) -> str:
        return str(self.value)
