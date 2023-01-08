from logging import Logger
from typing import List, Dict
from datetime import datetime, date
import json

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierLedgerObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: str
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_tips_sum: float


class DDSReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_courier_ledgers(self, treshold, limit: int) -> List[CourierLedgerObj]:
        print(f'threshold: {treshold}, limit: {limit}')
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            print(f'Treshold type: {type(treshold["year"])}, {type(treshold["month"])}')
            cur.execute(
                """
                    SELECT 
                        dc.courier_id,
                        dc.courier_name,
                        dt."year" AS settlement_year,
                        dt."month" AS settlement_month,
                        COUNT(DISTINCT dd.order_id) AS orders_count,
                        SUM(fd.sum) AS orders_total_sum,
                        AVG(fd.rate) AS rate_avg,
                        SUM(fd.sum) * 0.25 AS order_processing_fee,
                        SUM(fd.tip_sum) AS courier_tips_sum
                    FROM dds.fct_deliveries fd 
                    JOIN dds.dm_deliveries dd ON fd.delivery_id = dd.id 
                    JOIN dds.dm_couriers dc ON dd.courier_id = dc.id 
                    JOIN dds.dm_orders do2 ON dd.order_id = do2.id 
                    JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id 
                    WHERE dt."year" >= %(year)s AND dt."month" >= %(month)s
                    GROUP BY 
                        dc.courier_id,
                        dc.courier_name,
                        dt."year",
                        dt."month"
                    LIMIT %(limit)s;
                """, 
                {
                    "year": treshold['year'],
                    "month": treshold['month'],
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CDMLoader:

    def insert_courier_ledger(self, conn: Connection, courier_ledger: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT 
                        fd.sum
                    FROM dds.fct_deliveries fd 
                    JOIN dds.dm_deliveries dd ON fd.delivery_id = dd.id  
                    JOIN dds.dm_couriers dc ON dd.courier_id = dc.id 
                    JOIN dds.dm_orders do2 ON dd.order_id = do2.id 
                    JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id 
                    WHERE dc.courier_id = %(courier_id)s 
                        AND dt."year" = %(year)s
                        AND dt."month" = %(month)s;
                """,
                {
                    "courier_id": courier_ledger.courier_id,
                    "year": courier_ledger.settlement_year,
                    "month": courier_ledger.settlement_month
                }
            )
            c_deliveries = cur.fetchall()
            courier_order_sum = 0
            for order_sum in c_deliveries:
                if courier_ledger.rate_avg < 4:
                    courier_order_reward = float(order_sum[0]) * (5 / 100)
                    if courier_order_reward < 100:
                        courier_order_sum += 100
                    else:
                        courier_order_sum += courier_order_reward
                if 4 <= courier_ledger.rate_avg < 4.5:
                    courier_order_reward = float(order_sum[0]) * (7 / 100)
                    if courier_order_reward < 150:
                        courier_order_sum += 150
                    else:
                        courier_order_sum += courier_order_reward
                if 4.5 <= courier_ledger.rate_avg < 4.9:
                    courier_order_reward = float(order_sum[0]) * (8 / 100)
                    if courier_order_reward < 175:
                        courier_order_sum += 175
                    else:
                        courier_order_sum += courier_order_reward
                if courier_ledger.rate_avg >= 4.9:
                    courier_order_reward = float(order_sum[0]) * (10 / 100)
                    if courier_order_reward < 200:
                        courier_order_sum += 200
                    else:
                        courier_order_sum += courier_order_reward
            print(f'courier_rate: {courier_ledger.rate_avg}')
            print(f'courier_ledger.courier_order_sum: {courier_order_sum}')
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(
                        courier_id, 
                        courier_name, 
                        settlement_year, 
                        settlement_month, 
                        orders_count, 
                        orders_total_sum,
                        rate_avg, 
                        order_processing_fee, 
                        courier_order_sum, 
                        courier_tips_sum, 
                        courier_reward_sum
                    )
                    VALUES(
                        %(courier_id)s,
                        %(courier_name)s,
                        %(settlement_year)s,
                        %(settlement_month)s,
                        %(orders_count)s,
                        %(orders_total_sum)s,
                        %(rate_avg)s,
                        %(order_processing_fee)s,
                        %(courier_order_sum)s,
                        %(courier_tips_sum)s,
                        %(courier_reward_sum)s
                    )
                    ON CONFLICT (courier_id, settlement_year, settlement_month)
                    DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": courier_ledger.courier_id,
                    "courier_name": courier_ledger.courier_name,
                    "settlement_year": courier_ledger.settlement_year,
                    "settlement_month": courier_ledger.settlement_month,
                    "orders_count": courier_ledger.orders_count,
                    "orders_total_sum": courier_ledger.orders_total_sum,
                    "rate_avg": courier_ledger.rate_avg,
                    "order_processing_fee": courier_ledger.order_processing_fee,
                    "courier_order_sum": courier_order_sum,
                    "courier_tips_sum": courier_ledger.courier_tips_sum,
                    "courier_reward_sum": courier_order_sum + courier_ledger.courier_tips_sum * 0.95 
                }
            )


class CourierLedgerLoader:
    WF_KEY = "cdm_courier_ledger_workflow"
    LAST_LOADED_MONTH = "last_loaded_month"
    BATCH_LIMIT = 20000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = DDSReader(pg_dwh)
        self.dds = CDMLoader()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_courier_ledger(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dwh.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_MONTH: {"year": 2022, "month": 11}})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_MONTH]
            load_queue = self.stg.list_courier_ledgers(json.loads(last_loaded), self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} courier ledgers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier_ledger in load_queue:
                self.dds.insert_courier_ledger(conn, courier_ledger)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_MONTH] = json2str({"year": max([t.settlement_year for t in load_queue]), "month": max([t.settlement_month for t in load_queue])})
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_MONTH]}")
