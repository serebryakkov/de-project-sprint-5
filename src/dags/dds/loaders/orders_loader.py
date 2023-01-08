from logging import Logger
from typing import List
from datetime import datetime

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    object_id: str
    user_id: str
    restaurant_id: str
    date: datetime
    order_status: str


class STGReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        object_id,
                        (object_value::json -> 'user')::json -> 'id' AS user_id,
                        (object_value::json -> 'restaurant')::json -> 'id' AS restaurant_id,
                        object_value::json -> 'date' AS date,
                        object_value::json -> 'final_status' AS order_status
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DDSLoader:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            user_id = None
            restaurant_id = None
            timestamp_id = None
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_users
                    WHERE user_id = %(user_id)s;
                """,
                {
                    "user_id": order.user_id
                }
            )
            user_id = cur.fetchone()[0]
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {
                    "restaurant_id": order.restaurant_id
                }
            )
            restaurant_id = cur.fetchone()[0]
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_timestamps
                    WHERE ts = %(timestamp)s;
                """,
                {
                    "timestamp": order.date
                }
            )
            timestamp_id = cur.fetchone()[0]
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, user_id, restaurant_id, timestamp_id, order_status)
                    VALUES (%(order_key)s, %(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_status)s);
                """,
                {   
                    "order_key": order.object_id,
                    "user_id": user_id,
                    "restaurant_id": restaurant_id,
                    "timestamp_id": timestamp_id,
                    "order_status": order.order_status
                },
            )


class DDSOrderLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 20000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = STGReader(pg_dwh)
        self.dds = DDSLoader()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_orders(self):
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
            load_queue = self.stg.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.dds.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
