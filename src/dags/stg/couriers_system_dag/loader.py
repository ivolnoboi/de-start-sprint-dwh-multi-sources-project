from datetime import datetime
from logging import Logger

from stg import EtlSetting, StgEtlSettingsRepository
from stg.couriers_system_dag.pg_saver import PgSaver
from stg.couriers_system_dag.reader import Reader
from lib import PgConnect
from lib.dict_util import json2str


class Loader:

    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, reader: Reader, pg_dest: PgConnect, pg_saver: PgSaver, WF_KEY: str, table_name: str, id_key: str, logger: Logger) -> None:
        self.reader = reader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger
        self.WF_KEY = WF_KEY
        self.table_name = table_name
        self.id_key = id_key

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.reader.get_data()
            self.log.info(f"Found {len(load_queue)} records to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, self.table_name, str(d[self.id_key]), datetime.now(), d)


            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = datetime.now()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
