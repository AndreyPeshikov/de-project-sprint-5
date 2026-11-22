import logging

import pendulum
from airflow.decorators import dag, task
from stg.pg_saver import PgSaverCourier
from stg.pg_saver import PgSaverDelivery
from stg.courier_reader import CourierReader
from stg.delivery_reader import DeliveryReader
from stg.courier_loader import CourierLoader
from stg.delivery_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_delivery_system():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaverCourier()

        # Инициализируем класс, реализующий чтение данных из источника.
        api_reader = CourierReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CourierLoader(api_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_delivery():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaverDelivery()

        # Инициализируем класс, реализующий чтение данных из источника.
        api_reader = DeliveryReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = DeliveryLoader(api_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    couriers_loader = load_couriers()
    delivery_loader = load_delivery() 

    # Задаем порядок выполнения.
    couriers_loader >> delivery_loader


stg_dag = sprint5_delivery_system()
