from datetime import datetime
from typing import Any
import logging

from lib.dict_util import json2str
from psycopg import Connection


log = logging.getLogger(__name__)


class PgSaver:

    def save_object(self, conn: Connection, table_name: str, id: str, update_ts: datetime, val: Any):
        log.info(table_name)
        str_val = json2str(val)
        log.info(table_name)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO {table_name} (object_id, object_value, update_ts)
                    VALUES ('{id}', '{str_val}', '{update_ts}')
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """
            )
