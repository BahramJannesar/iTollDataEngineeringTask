import sys
sys.path.append("/opt/airflow/dags")
sys.path.append("/opt/airflow/dags/itoll")


import logging
from pprint import pformat
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)


@dag(
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2022, 7, 27, tz="Asia/Tehran"),
    catchup=False,
    tags=["init"],
)
def initial_load():

    import sys
    sys.path.append("/opt/airflow/dags")
    sys.path.append("/opt/airflow/dags/itoll")

    from airflow.hooks.mysql_hook import MySqlHook
    from itoll.initial_load.generate_data import generate_fake_users,generate_fake_cars,generate_fake_orders
    from itoll.etl.load import load_into_mysql

    mysql_hook_prod = MySqlHook(mysql_conn_id="mysql_conn", schema="db")

    @task()
    def print_context_info():
        context = get_current_context()
        logger.info(pformat(context))

    @task()
    def initial_loader():
        num_users = 4000
        num_cars = 8000
        num_orders = 100000
        fake_users = generate_fake_users(num_users)
        user_ids = [user[1][0] for user in fake_users.iterrows()]
        fake_cars = generate_fake_cars(num_cars, user_ids)

        cars_users_df = fake_users.merge(fake_cars, left_on=["id"], right_on=["user_id"], suffixes=('_users', '_cars'))
        list_of_tuples_cars_users = [tuple(x) for x in cars_users_df[["user_id", "id_cars"]].to_records(index=False)]

        fake_orders = generate_fake_orders(num_orders, list_of_tuples_cars_users)

        load_into_mysql(dataframe=fake_users,
                        my_sql_connection=mysql_hook_prod,
                        table_name="Users")

        load_into_mysql(dataframe=fake_cars,
                        my_sql_connection=mysql_hook_prod,
                        table_name="Cars")

        load_into_mysql(dataframe=fake_orders,
                        my_sql_connection=mysql_hook_prod,
                        table_name="Orders")

    print_context_info() >> initial_loader()


assortment_planning = initial_load()