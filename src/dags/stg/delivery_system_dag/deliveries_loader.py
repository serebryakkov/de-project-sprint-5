from logging import Logger
from typing import List
import requests

from airflow.hooks.base import BaseHook

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class DeliveryObj():
    def __init__(self, delivery_id: str, object_value: str) -> None:
         self.delivery_id = delivery_id
         self.object_value = object_value


class DeliveryOriginRepository:
    def list_deliveries(self, offset: int, limit: int) -> List[DeliveryObj]:
        conn = BaseHook.get_connection("DELIVERY_SYSTEM_GET_DELIVERIES_ENDPOINT")
        host = conn.host
        headers = {
            "X-Nickname": conn.extra_dejson.get("X-Nickname"),
            "X-Cohort": conn.extra_dejson.get("X-Cohort"),
            "X-API-KEY": conn.extra_dejson.get("X-API-KEY")
        }
        print(f'Headers: {headers}')
        get_params = {
            "sort_field": "date",
            "sort_direction": "asc",
            "offset": offset,
            "limit": limit
        }
        objs = []
        response = requests.get(host, params=get_params, headers=headers).json()
        for entry in response:
            objs.append(
                DeliveryObj(
                    entry.get("delivery_id"),
                    entry
                )
            )
        return objs


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(delivery_id, object_value)
                    VALUES (%(delivery_id)s, %(object_value)s);
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "object_value": json2str(delivery.object_value)
                },
            )


class DeliveryLoader:
    WF_KEY = "delivery_origin_to_stg_workflow"
    OFFSET = "offset"
    BATCH_LIMIT = 50

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.origin = DeliveryOriginRepository()
        self.pg_dest = pg_dest
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.OFFSET: 0})

            # Вычитываем очередную пачку объектов.
            offset = wf_setting.workflow_settings[self.OFFSET]
            load_queue = self.origin.list_deliveries(offset, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if len(load_queue) == 0:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.OFFSET] = offset + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.OFFSET]}")
