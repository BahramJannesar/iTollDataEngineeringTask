import pandas as pd
from clickhouse_driver import Client
from typing import Dict
from itoll.constants.enums import StaticsVars

def load_into_mysql(dataframe : pd.DataFrame, my_sql_connection, table_name: str):
    dataframe.to_sql(name=table_name, con=my_sql_connection.get_sqlalchemy_engine(),if_exists='append', index=False)


def load_into_clickhouse(dataframe : pd.DataFrame, table: str , clickhouse_connection: Dict):
    client = Client(host=clickhouse_connection.get(StaticsVars.HOST.str_value),
                    port=clickhouse_connection.get(StaticsVars.PORT.str_value),
                    database=clickhouse_connection.get(StaticsVars.DATABASE.str_value),
                    user=clickhouse_connection.get(StaticsVars.USER.str_value),
                    password=clickhouse_connection.get(StaticsVars.PASSWORD.str_value),
                    settings={'use_numpy': True})
    client.insert_dataframe(f"INSERT INTO {clickhouse_connection.get(StaticsVars.DATABASE.str_value)}.{table} VALUES", dataframe)

