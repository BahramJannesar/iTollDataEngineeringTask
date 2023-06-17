import sys
sys.path.append("/opt/airflow/dags")
sys.path.append("/opt/airflow/dags/itoll")


import logging
from pprint import pformat
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger(__name__)


@dag(
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2022, 7, 27, tz="Asia/Tehran"),
    catchup=False,
    tags=["etl", "batch"],
)
def etl():

    import sys
    sys.path.append("/opt/airflow/dags")
    sys.path.append("/opt/airflow/dags/itoll")

    from itoll.etl.extract import create_spark_session, extract_data_from_mysql
    from itoll.etl.transform import creating_general_mart_order
    from itoll.etl.load import load_into_clickhouse
    from itoll.utils.query_read_utils import query_reader
    from itoll.constants.enums import StaticsVars
    from airflow.hooks.mysql_hook import MySqlHook

    my_sql_connection = MySqlHook.get_connection("mysql_conn")

    _clickhouse_http_connection = BaseHook.get_connection(StaticsVars.CLICKHOUSE_HTTP_CONNECTION.str_value)
    clickhouse_connection = dict(
        host=_clickhouse_http_connection.host,
        port=_clickhouse_http_connection.port,
        database=_clickhouse_http_connection.schema,
        user=_clickhouse_http_connection.login,
        password=_clickhouse_http_connection.password
    )


    @task()
    def print_context_info():
        context = get_current_context()
        logger.info(pformat(context))

    @task()
    def creat_spark_session_task():
        ss = create_spark_session()
        return ss

    @task()
    def extract_users_table(spark_session,
                            database_host,
                            database_name,
                            query,
                            username,
                            password):
        user_dataframe = extract_data_from_mysql(spark_session=spark_session,
                                database_host=database_host,
                                database_name=database_name,
                                query=query,
                                username=username,
                                password=password)
        return user_dataframe

    @task()
    def extract_orders_table(spark_session,
                            database_host,
                            database_name,
                            query,
                            username,
                            password):
        order_dataframe = extract_data_from_mysql(spark_session=spark_session,
                                database_host=database_host,
                                database_name=database_name,
                                query=query,
                                username=username,
                                password=password)
        return order_dataframe

    @task()
    def extract_cars_table(spark_session,
                            database_host,
                            database_name,
                            query,
                            username,
                            password):
        car_dataframe = extract_data_from_mysql(spark_session=spark_session,
                                database_host=database_host,
                                database_name=database_name,
                                query=query,
                                username=username,
                                password=password)
        return car_dataframe

    @task()
    def transform_task(user_dataframe, order_dataframe, car_dataframe):
        final_general_mart_dataframe = creating_general_mart_order(user_dataframe=user_dataframe,
                                    car_dataframe=car_dataframe,
                                    order_dataframe=order_dataframe)
        return final_general_mart_dataframe

    @task()
    def load_data_task(clickhouse_connection_dict, dataframe, table):
        load_into_clickhouse(clickhouse_connection=clickhouse_connection_dict,
                             dataframe=dataframe,
                             table=table)


    ss = create_spark_session()
    user = extract_users_table(spark_session=ss,
                        database_host=my_sql_connection.host,
                        database_name=my_sql_connection.schema,
                        query=query_reader(Variable.get(StaticsVars.ETL_QUERY.str_value,deserialize_json=True)[StaticsVars.QUERY_PATH_EXTRACT_USER_TABLE.str_value]),
                        username=my_sql_connection.login,
                        password=my_sql_connection.password)
    order = extract_users_table(spark_session=ss,
                        database_host=my_sql_connection.host,
                        database_name=my_sql_connection.schema,
                        query=query_reader(Variable.get(StaticsVars.ETL_QUERY.str_value,deserialize_json=True)[StaticsVars.QUERY_PATH_EXTRACT_ORDER_TABLE.str_value]),
                        username=my_sql_connection.login,
                        password=my_sql_connection.password)
    car = extract_users_table(spark_session=ss,
                        database_host=my_sql_connection.host,
                        database_name=my_sql_connection.schema,
                        query=query_reader(Variable.get(StaticsVars.ETL_QUERY.str_value,deserialize_json=True)[StaticsVars.QUERY_PATH_EXTRACT_CAR_TABLE.str_value]),
                        username=my_sql_connection.login,
                        password=my_sql_connection.password)
    final_general_mart_order_dataframe = transform_task(user_dataframe=user,
                                                        order_dataframe=order,
                                                        car_dataframe=car)
    load_data_task(dataframe=final_general_mart_order_dataframe,
                   clickhouse_connection=clickhouse_connection,
                   table="general_mart_order")


assortment_planning = etl()