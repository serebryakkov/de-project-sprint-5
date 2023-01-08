from logging import Logger
from typing import List, Dict
from datetime import datetime, date

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SettlementReportObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float
    last_order_dt: datetime


class DDSReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_settlement_reports(self, order_treshold: str, limit: int) -> List[SettlementReportObj]:
        print(f'threshold: {order_treshold}, limit: {limit}')
        with self._db.client().cursor(row_factory=class_row(SettlementReportObj)) as cur:
            cur.execute(
                """
                    SELECT
                        dr.restaurant_id,
                        dr.restaurant_name,
                        DATE_TRUNC('day', dt."date")::date AS settlement_date,
                        COUNT(DISTINCT fps.order_id) AS orders_count,
                        SUM(fps.total_sum) AS orders_total_sum,
                        SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
                        SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
                        SUM(fps.total_sum) * 0.25 AS order_processing_fee,
                        SUM(fps.total_sum) - SUM(fps.bonus_payment) - (SUM(fps.total_sum) * 0.25) AS restaurant_reward_sum,
                        MAX(dt.ts) AS last_order_dt
                    FROM dds.fct_product_sales fps 
                    INNER JOIN dds.dm_orders do2 ON fps.order_id = do2.id 
                        AND do2.order_status = 'CLOSED'
                    INNER JOIN dds.dm_restaurants dr ON do2.restaurant_id = dr.id
                    INNER JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id 
                    WHERE dt.ts > %(order_treshold)s
                    GROUP BY dr.restaurant_id,
                            dr.restaurant_name,
                            DATE_TRUNC('day', dt."date");
                """, 
                {
                    "order_treshold": order_treshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CDMLoader:

    def insert_settlement_report(self, conn: Connection, settlement_report: SettlementReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report
                    (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, 
                    orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES(%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, 
                    %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, restaurant_name, settlement_date)
                    DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": settlement_report.restaurant_id,
                    "restaurant_name": settlement_report.restaurant_name,
                    "settlement_date": settlement_report.settlement_date,
                    "orders_count": settlement_report.orders_count,
                    "orders_total_sum": settlement_report.orders_total_sum,
                    "orders_bonus_payment_sum": settlement_report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": settlement_report.orders_bonus_granted_sum,
                    "order_processing_fee": settlement_report.order_processing_fee,
                    "restaurant_reward_sum": settlement_report.restaurant_reward_sum
                }
            )


class SettlementReportLoader:
    WF_KEY = "cdm_settlement_report_workflow"
    LAST_LOADED_ORDER_DATE = "last_loaded_order_date"
    BATCH_LIMIT = 20000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = DDSReader(pg_dwh)
        self.dds = CDMLoader()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_settlement_report(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dwh.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ORDER_DATE: '2022-01-01 00:01:00'})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ORDER_DATE]
            load_queue = self.stg.list_settlement_reports(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} settlement reports to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for settlement_report in load_queue:
                self.dds.insert_settlement_report(conn, settlement_report)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ORDER_DATE] = max([t.last_order_dt for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ORDER_DATE]}")
