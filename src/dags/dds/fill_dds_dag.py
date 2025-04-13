import logging

from lib.query_generator import CourierQueryGenerator, DeliveryQueryGenerator, OrderQueryGenerator, ProductQueryGenerator, ProductSaleQueryGenerator, RestaurantQueryGenerator, TimestampQueryGenerator, UserQueryGenerator
from lib.loader import Loader
from lib.reader import Reader
from lib.pg_saver import PgSaver
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def fill_dds_layer():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Инициализируем класс, в котором реализована логика сохранения.
    pg_saver = PgSaver()

    # Инициализируем класс, реализующий чтение данных из источника.
    collection_reader = Reader(dwh_pg_connect)

    schema = 'dds'


    @task()
    def load_users():

        query_gen = UserQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    user_loader = load_users()


    @task()
    def load_restaurants():

        query_gen = RestaurantQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    restaurant_loader = load_restaurants()


    @task()
    def load_timestamps():
        
        query_gen = TimestampQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    timestamp_loader = load_timestamps()


    @task()
    def load_couriers():

        query_gen = CourierQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    courier_loader = load_couriers()


    @task()
    def load_deliveries():
        
        query_gen = DeliveryQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    delivery_loader = load_deliveries()


    @task()
    def load_products():
        
        query_gen = ProductQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    product_loader = load_products()

    @task()
    def load_orders():
        
        query_gen = OrderQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    order_loader = load_orders()

    @task()
    def load_product_sales():
        
        query_gen = ProductSaleQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    product_sale_loader = load_product_sales()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    user_loader >> restaurant_loader >> timestamp_loader >> courier_loader >> delivery_loader >> product_loader >> order_loader >> product_sale_loader  # type: ignore


fill_dds_layer_dag = fill_dds_layer()
