import logging

from lib.query_generator import DMSettlementReportQueryGenerator
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
    tags=['sprint5', 'cdm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dm_settlement_report():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    schema = 'cdm'

    @task()
    def create_dm_settlement_report():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = Reader(dwh_pg_connect)

        query_gen = DMSettlementReportQueryGenerator()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, query_gen, log, schema)

        # Запускаем копирование данных.
        loader.run_copy()

    report_loader = create_dm_settlement_report()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    report_loader  # type: ignore


settlement_report_cdm_dag = dm_settlement_report()  # noqa
