from datetime import datetime
from logging import Logger
from typing import Dict, List
from psycopg import Connection

from lib.pg_saver import PgSaver
from lib.reader import Reader
from lib.query_generator import QueryGenerator
from lib import PgConnect, EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str


class LoadSettings:
    def __init__(self, conn: Connection, schema: str, wf_key: str, setting_keys: List[str]):
        self.conn = conn
        self.settings_repository = EtlSettingsRepository(schema)
        self.WF_KEY = wf_key
        self.SETTING_KEYS = setting_keys

    def get_workflow_settings(self):
        # Прочитываем состояние загрузки
        # Если настройки еще нет, заводим ее.
        wf_setting = self.settings_repository.get_setting(self.conn, self.WF_KEY)
        if not wf_setting:
            workflow_settings = {}
            for key in self.SETTING_KEYS:
                # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                # А в БД мы сохраним именно JSON.
                workflow_settings[key] = datetime(2022, 1, 1).isoformat()

            wf_setting = EtlSetting(
                id=0,
                workflow_key=self.WF_KEY,
                workflow_settings=workflow_settings
            )

        return wf_setting

    def save_workflow_settings(self, data):
        workflow_settings_dict = {
            self.SETTING_KEYS[i]: data[i] for i in range(len(self.SETTING_KEYS))
        }

        wf_setting_json = json2str(workflow_settings_dict)
        self.settings_repository.save_setting(self.conn, self.WF_KEY, wf_setting_json)

        return wf_setting_json


class Loader:

    def __init__(self, collection_loader: Reader, pg_dest: PgConnect, pg_saver: PgSaver, query_gen: QueryGenerator, logger: Logger, schema: str) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.query_gen = query_gen
        self.schema = schema
        self.WF_KEY = query_gen.get_wf_key()
        self.WORKFLOW_SETTING_KEYS = query_gen.get_wf_settings()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            load_settings = LoadSettings(conn, self.schema, self.WF_KEY, self.WORKFLOW_SETTING_KEYS)
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = load_settings.get_workflow_settings()

            load_queue = self.collection_loader.get_data(self.query_gen.get_query(wf_setting.workflow_settings))

            self.log.info(f"Found {len(load_queue)} records to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0


            for record in load_queue:
                saving_params = self.query_gen.get_saving_params(record)

                self.pg_saver.save_object(
                    conn=conn,
                    table_name=saving_params["table_name"],
                    data=saving_params["data"],
                    conflict_cond=saving_params["conflict_cond"],
                    update_columns=saving_params["update_columns"]
                )

            if (self.schema == 'cdm'):
                setting_data = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            else:
                setting_data = load_queue[0][(len(load_queue[0]) - len(self.WORKFLOW_SETTING_KEYS)):]

            wf_setting_json = load_settings.save_workflow_settings(setting_data)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
