import logging

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from lib.dict_util import str2json

log = logging.getLogger(__name__)
source_conn_id = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
dwh_conn_id = "PG_WAREHOUSE_CONNECTION"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1, 00, 00, 00),
    'retries': 0
}


def load_ranks():

    src_pg_hook = PostgresHook(source_conn_id)
    src_connection = src_pg_hook.get_conn()
    src_cursor = src_connection.cursor()
    src_cursor.execute("SELECT * FROM ranks;")
    sources = src_cursor.fetchall()

    dwh_pg_hook = PostgresHook(dwh_conn_id)
    dwh_connection = dwh_pg_hook.get_conn()
    dwh_cursor = dwh_connection.cursor()

    for source in sources:
        dwh_cursor.execute(
            """
                INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
                VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                ON CONFLICT (id) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    bonus_percent = EXCLUDED.bonus_percent,
                    min_payment_threshold = EXCLUDED.min_payment_threshold;
            """,
            {
                "id": source[0],
                "name": source[1],
                "bonus_percent": source[2],
                "min_payment_threshold": source[3]
            })
        dwh_connection.commit()

    dwh_cursor.close()
    dwh_connection.close()
    src_cursor.close()
    src_connection.close()


def load_users():

    src_pg_hook = PostgresHook(source_conn_id)
    src_connection = src_pg_hook.get_conn()
    src_cursor = src_connection.cursor()
    src_cursor.execute("SELECT * FROM users;")
    sources = src_cursor.fetchall()

    dwh_pg_hook = PostgresHook(dwh_conn_id)
    dwh_connection = dwh_pg_hook.get_conn()
    dwh_cursor = dwh_connection.cursor()

    for source in sources:
        dwh_cursor.execute(
            """
                INSERT INTO stg.bonussystem_users (id, order_user_id)
                VALUES (%(id)s, %(order_user_id)s)
                ON CONFLICT (id) DO UPDATE
                SET
                    order_user_id = EXCLUDED.order_user_id;
            """,
            {
                "id": source[0],
                "order_user_id": source[1]
            })
        dwh_connection.commit()

    dwh_cursor.close()
    dwh_connection.close()
    src_cursor.close()
    src_connection.close()


def events_load():
    LAST_LOADED_ID_KEY = -1
    src_pg_hook = PostgresHook(source_conn_id)
    dwh_pg_hook = PostgresHook(dwh_conn_id)

    with dwh_pg_hook.get_conn() as dwh_connection:
        dwh_cursor = dwh_connection.cursor()
        dwh_cursor.execute(
            """
                SELECT *
                FROM stg.srv_wf_settings
                WHERE workflow_key = 'events_load'
                ORDER BY id DESC
                LIMIT 1;
            """
        )
        workflow_data = dwh_cursor.fetchone()
        if (workflow_data is not None):
            LAST_LOADED_ID_KEY = int(workflow_data[2]['outbox_id'])
        log.info(LAST_LOADED_ID_KEY)

        with src_pg_hook.get_conn() as src_connection:
            src_cursor = src_connection.cursor()
            src_cursor.execute(
                f"""
                    SELECT *
                    FROM outbox
                    WHERE id > {LAST_LOADED_ID_KEY}
                    ORDER BY id;
                """
            )

            sources = src_cursor.fetchall()
            last_id = LAST_LOADED_ID_KEY
            for source in sources:
                dwh_cursor.execute(
                    """
                        INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                        VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                        ON CONFLICT (id) DO UPDATE
                        SET
                            event_ts = EXCLUDED.event_ts,
                            event_type = EXCLUDED.event_type,
                            event_value = EXCLUDED.event_value;
                    """,
                    {
                        "id": source[0],
                        "event_ts": source[1],
                        "event_type": source[2],
                        "event_value": source[3],
                    }
                )
                last_id = int(source[0])
                # dwh_connection.commit()
            
            LAST_LOADED_ID_KEY = last_id
            src_cursor.close()

        dwh_cursor.execute(
            f"""
                INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                VALUES ('events_load', '{json.dumps({"outbox_id": LAST_LOADED_ID_KEY})}')
                ON CONFLICT (workflow_key) DO UPDATE
                SET
                    workflow_settings = EXCLUDED.workflow_settings;
            """
        )
        dwh_cursor.close()


with DAG(
    'ranks_dag',
    default_args=default_args,
    schedule_interval='0/15 * * * *',
    catchup=False,
    tags=['sprint5', 'load_ranks']
) as dag:

    load_ranks_task = PythonOperator(
        task_id='load_ranks_task',
        python_callable=load_ranks
    )

    load_users_task = PythonOperator(
        task_id='load_users_task',
        python_callable=load_users
    )

    load_events_task = PythonOperator(
        task_id='load_events_task',
        python_callable=events_load
    )

    load_ranks_task >> load_users_task >> load_events_task
