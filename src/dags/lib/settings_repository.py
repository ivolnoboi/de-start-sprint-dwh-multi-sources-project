from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class EtlSettingsRepository:
    def __init__(self, schema: str):
        self.schema = schema

    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                f"""
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM {self.schema}.srv_wf_settings
                    WHERE workflow_key = '{etl_key}';
                """
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO {self.schema}.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES ('{workflow_key}', '{workflow_settings}')
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """
            )
