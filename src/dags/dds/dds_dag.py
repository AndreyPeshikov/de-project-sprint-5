import logging

import pendulum
from airflow.decorators import dag, task
from dds.dm_couriers_loader import DM_CouriersLoader
from dds.dm_timestamps_loader import DM_TimestampsLoader
from dds.dm_restaurants_loader import DM_RestaurantsLoader
from dds.dm_users_loader import DM_UsersLoader
from dds.dm_orders_loader import DM_OrdersLoader
from dds.dm_deliveries_loader import DM_DeliveriesLoader
from dds.dm_products_loader import DM_ProductsLoader
from dds.dm_productsales_loader import DM_ProductsalesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_couriers")
    def load_dm_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_CouriersLoader(dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_timestamps")
    def load_dm_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_TimestampsLoader(dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_restaurants")
    def load_dm_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_RestaurantsLoader(dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_users")
    def load_dm_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_UsersLoader(dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_oders")
    def load_dm_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_OrdersLoader(dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_deliveries")
    def load_dm_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_DeliveriesLoader(dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_products")
    def load_dm_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_ProductsLoader(dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_productsales_load")
    def load_dm_productsales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DM_ProductsalesLoader(dwh_pg_connect, log)
        rest_loader.load_productsales()  # Вызываем функцию, которая перельет данные.
    
    # Инициализируем объявленные таски.
    dm_couriers = load_dm_couriers()
    dm_timestamps = load_dm_timestamps()
    dm_restaurants = load_dm_restaurants()
    dm_users = load_dm_users()
    dm_orders = load_dm_orders()
    dm_deliveries = load_dm_deliveries()
    dm_products = load_dm_products()
    dm_productsales = load_dm_productsales()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_couriers >> dm_timestamps >> dm_restaurants >> dm_users >> dm_orders >> dm_deliveries >> dm_products >> dm_productsales


dds_dag = sprint5_dds_dag()
