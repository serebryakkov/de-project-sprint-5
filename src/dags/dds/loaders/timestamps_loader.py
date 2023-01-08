from logging import Logger
from typing import List
from datetime import datetime, time, date

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TimestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class STGReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamp_threshold: int, limit: int) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id AS id,
                        ((object_value::json -> 'date')::TEXT)::timestamp AS ts,
                        EXTRACT(YEAR FROM ((object_value::json -> 'date')::TEXT)::date) AS "year",
                        EXTRACT(MONTH FROM ((object_value::json -> 'date')::TEXT)::date) AS "month",
                        EXTRACT(DAY FROM ((object_value::json -> 'date')::TEXT)::date) AS "day",
                        (((object_value::json -> 'date')::TEXT)::timestamp)::time AS "time",
                        ((object_value::json -> 'date')::TEXT)::date AS "date"
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamp_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DDSLoader:

    def insert_timestamp(self, conn: Connection, timestamp: TimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s);
                """,
                {   
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )


class DDSTimestampLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 30000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = STGReader(pg_dwh)
        self.dds = DDSLoader()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dwh.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for timestamp in load_queue:
                self.dds.insert_timestamp(conn, timestamp)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
