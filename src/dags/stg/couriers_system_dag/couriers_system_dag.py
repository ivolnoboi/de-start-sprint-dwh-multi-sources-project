from datetime import datetime, timedelta
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from stg.couriers_system_dag.pg_saver import PgSaver
from stg.couriers_system_dag.loader import Loader
from stg.couriers_system_dag.reader import Reader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def load_stg_courier_system_data():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    base_url = Variable.get("COURIERS_BASE_URL")
    headers={
        "X-API-KEY": Variable.get("X_API_KEY"),  # ключ API
        "X-Nickname": Variable.get("NICKNAME"),  # авторизационные данные
        "X-Cohort": Variable.get("COHORT")  # авторизационные данные
    }

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "limit": 50,
            "offset": 0
        }

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = Reader(base_url + 'couriers', headers, params)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, "couriers_system_origin_to_stg_workflow", 'stg.couriersystem_couriers', "_id", log)

        # Запускаем копирование данных.
        loader.run_copy()

    courier_loader = load_couriers()


    @task()
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        params = {
            "sort_field": "date",
            "sort_direction": "asc",
            "limit": 50,
            "offset": 0,
            "from": (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        }

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = Reader(base_url + 'deliveries', headers, params)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, "deliveries_system_origin_to_stg_workflow", 'stg.couriersystem_deliveries', 'delivery_id', log)

        # Запускаем копирование данных.
        loader.run_copy()

    delivery_loader = load_deliveries()


    # Задаем порядок выполнения.
    courier_loader >> delivery_loader  # type: ignore


courier_stg_dag = load_stg_courier_system_data()  # noqa
