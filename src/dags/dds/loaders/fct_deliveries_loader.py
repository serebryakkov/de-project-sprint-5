from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FCTDeliveryObj(BaseModel):
    id: int
    delivery_id: str
    rate: int
    sum: float
    tip_sum: float


class STGReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[FCTDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(FCTDeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        delivery_id,
                        (object_value::json ->> 'rate')::int AS rate,
                        (object_value::json ->> 'sum')::numeric(14,2) AS sum,
                        (object_value::json ->> 'tip_sum')::numeric(14,2) AS tip_sum
                    FROM stg.deliverysystem_deliveries dd 
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DDSLoader:

    def insert_fct_delivery(self, conn: Connection, fctdelivery: FCTDeliveryObj) -> None:
        with conn.cursor() as cur:
            print(fctdelivery.delivery_id)
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(delivery_id, rate, sum, tip_sum)
                    VALUES (
                        (SELECT id FROM dds.dm_deliveries WHERE delivery_key = %(delivery_id)s),
                        %(rate)s,
                        %(sum)s,
                        %(tip_sum)s
                    )
                """,
                {   
                    "delivery_id": fctdelivery.delivery_id,
                    "rate": fctdelivery.rate,
                    "sum": fctdelivery.sum,
                    "tip_sum": fctdelivery.tip_sum
                },
            )


class DDSFCTDeliveryLoader:
    WF_KEY = "fct_deliveries_load_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 20000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = STGReader(pg_dwh)
        self.dds = DDSLoader()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_fct_deliveries(self):
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
            load_queue = self.stg.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct_delivery in load_queue:
                self.dds.insert_fct_delivery(conn, fct_delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
