import sys
sys.path.append("/opt/airflow/dags")
sys.path.append("/opt/airflow/dags/itoll")


import logging
from pprint import pformat
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

logger = logging.getLogger(__name__)


@dag(
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2022, 7, 27, tz="Asia/Tehran"),
    catchup=False,
    tags=["batch", "manual"],
)
def creat_tables():

    import sys
    sys.path.append("/opt/airflow/dags")
    sys.path.append("/opt/airflow/dags/itoll")

    from airflow.hooks.mysql_hook import MySqlHook
    from itoll.utils.query_read_utils import query_reader
    from itoll.constants.enums import StaticsVars

    mysql_hook_prod = MySqlHook(mysql_conn_id="mysql_conn", schema="db")
    cursor_prod = mysql_hook_prod.get_conn().cursor()

    @task()
    def print_context_info():
        context = get_current_context()
        logger.info(pformat(context))

    @task()
    def create_user_table():
        cursor_prod.execute(query_reader(Variable.get(StaticsVars.CREATE_TABLE.str_value,deserialize_json=True)[StaticsVars.QUERY_PATH_CREATE_USER_TABLE.str_value]))

    @task()
    def create_order_table():
        cursor_prod.execute(query_reader(Variable.get(StaticsVars.CREATE_TABLE.str_value,deserialize_json=True)[StaticsVars.QUERY_PATH_CREATE_ORDER_TABLE.str_value]))

    @task()
    def create_car_table():
        cursor_prod.execute(query_reader(Variable.get(StaticsVars.CREATE_TABLE.str_value,deserialize_json=True)[StaticsVars.QUERY_PATH_CREATE_CAR_TABLE.str_value]))


    print_context_info() >> create_user_table() >> create_order_table() >> create_car_table()


assortment_planning = creat_tables()