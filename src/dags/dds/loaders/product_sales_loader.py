from logging import Logger
from typing import List, Dict
from datetime import datetime

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductSaleObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class STGReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sold_products(self, bonus_events_threshold: int, limit: int) -> List[ProductSaleObj]:
        print(f'threshold: {bonus_events_threshold}, limit: {limit}')
        with self._db.client().cursor(row_factory=class_row(ProductSaleObj)) as cur:
            cur.execute(
                """
                    SELECT
                        op.id,
                        dp.id AS product_id,
                        do2.id AS order_id,
                        op.quantity::int AS count,
                        op.price::numeric(14,2),
                        op.product_cost::numeric(14,2) AS total_sum,
                        op.bonus_payment::NUMERIC,
                        op.bonus_grant::numeric
                    FROM
                        (SELECT
                            id,
                            event_value::json ->> 'order_id' AS order_id,
                            json_array_elements(event_value::json -> 'product_payments')::json ->> 'product_id' AS product_id,
                            json_array_elements(event_value::json -> 'product_payments')::json ->> 'price' AS price,
                            json_array_elements(event_value::json -> 'product_payments')::json ->> 'quantity' AS quantity,
                            json_array_elements(event_value::json -> 'product_payments')::json ->> 'product_cost' AS product_cost,
                            json_array_elements(event_value::json -> 'product_payments')::json ->> 'bonus_payment' AS bonus_payment,
                            json_array_elements(event_value::json -> 'product_payments')::json ->> 'bonus_grant' AS bonus_grant
                        FROM stg.bonussystem_events be 
                        WHERE event_type = 'bonus_transaction'
                            AND id > %(threshold)s
                        ORDER BY id ASC
                        LIMIT %(limit)s) AS op
                    INNER JOIN dds.dm_products dp ON op.product_id = dp.product_id 
                    INNER JOIN dds.dm_orders do2 ON op.order_id = do2.order_key;
                """, 
                {
                    "threshold": bonus_events_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DDSLoader:

    def insert_product_sales(self, conn: Connection, sold_product: ProductSaleObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(order_id, product_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES(%(order_id)s, %(product_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s);
                """,
                {
                    "order_id": sold_product.order_id,
                    "product_id": sold_product.product_id,
                    "count": sold_product.count,
                    "price": sold_product.price,
                    "total_sum": sold_product.total_sum,
                    "bonus_payment": sold_product.bonus_payment,
                    "bonus_grant": sold_product.bonus_grant
                }
            )


class DDSProductSalesLoader:
    WF_KEY = "product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 20000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = STGReader(pg_dwh)
        self.dds = DDSLoader()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_product_sales(self):
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
            load_queue = self.stg.list_sold_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} product sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.dds.insert_product_sales(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
