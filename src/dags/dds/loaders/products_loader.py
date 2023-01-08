from logging import Logger
from typing import List, Dict

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import pendulum


class RestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    menu: List[Dict]


class STGReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, user_threshold: int, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        object_id AS restaurant_id,
                        object_value::json -> 'menu' AS menu
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DDSLoader:

    def insert_product(self, conn: Connection, restaurant: RestaurantObj) -> None:
        for product in restaurant.menu:
            with conn.cursor() as cur:
                dm_restaurants_id = None
                cur.execute(
                    """
                        SELECT id
                        FROM dds.dm_restaurants
                        WHERE restaurant_id = %(restaurant_id)s;
                    """,
                    {
                        "restaurant_id": restaurant.restaurant_id
                    }
                )
                dm_restaurants_id = cur.fetchone()
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(product_id, restaurant_id, product_name, product_price, active_from, active_to)
                        VALUES (%(product_id)s, %(restaurant_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s);
                    """,
                    {   
                        "product_id": product.get('_id'),
                        "restaurant_id": dm_restaurants_id[0],
                        "product_name": product.get('name'),
                        "product_price": product.get('price'),
                        "active_from": pendulum.now(),
                        "active_to": pendulum.datetime(2099, 12, 31)
                    },
                )


class DDSProductLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 20000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = STGReader(pg_dwh)
        self.dds = DDSLoader()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
            load_queue = self.stg.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.dds.insert_product(conn, restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
