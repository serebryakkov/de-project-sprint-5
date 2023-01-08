import logging

import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from lib import ConnectionBuilder
from stg.delivery_system_dag.couriers_loader import CourierLoader
from stg.delivery_system_dag.deliveries_loader import DeliveryLoader
from dds.loaders.couriers_loader import DDSCourierLoader
from dds.loaders.deliveries_loader import DDSDeliveryLoader
from dds.loaders.fct_deliveries_loader import DDSFCTDeliveryLoader
from cdm.dm_courier_ledger_loader import CourierLedgerLoader


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждые 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project5'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def project5_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with TaskGroup(group_id='stg_load') as tg1:
        @task(task_id="couriers_load")
        def load_couriers():
            # создаем экземпляр класса, в котором реализована логика.
            courier_loader = CourierLoader(dwh_pg_connect, log)
            courier_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        couriers_dict = load_couriers()

        @task(task_id="deliveries_load")
        def load_deliveries():
            # создаем экземпляр класса, в котором реализована логика.
            courier_loader = DeliveryLoader(dwh_pg_connect, log)
            courier_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        deliveries_dict = load_deliveries()

        [couriers_dict, deliveries_dict]

    with TaskGroup(group_id='dds_load') as tg2:
        @task(task_id="couriers_load")
        def load_couriers():
            # создаем экземпляр класса, в котором реализована логика.
            courier_loader = DDSCourierLoader(dwh_pg_connect, log)
            courier_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        couriers_dict = load_couriers()

        @task(task_id="deliveries_load")
        def load_deliveries():
            # создаем экземпляр класса, в котором реализована логика.
            delivery_loader = DDSDeliveryLoader(dwh_pg_connect, log)
            delivery_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        deliveries_dict = load_deliveries()

        @task(task_id="fct_deliveries_load")
        def load_fct_deliveries():
            # создаем экземпляр класса, в котором реализована логика.
            fct_deliveries_loader = DDSFCTDeliveryLoader(dwh_pg_connect, log)
            fct_deliveries_loader.load_fct_deliveries()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        fct_deliveries_dict = load_fct_deliveries()

        couriers_dict >> deliveries_dict >> fct_deliveries_dict

    @task(task_id="dm_courier_ledger_load")
    def load_courier_ledgers():
        # создаем экземпляр класса, в котором реализована логика.
        courier_ledger_loader = CourierLedgerLoader(dwh_pg_connect, log)
        courier_ledger_loader.load_courier_ledger()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    courier_ledgers_dict = load_courier_ledgers()
    
    tg1 >> tg2 >> courier_ledgers_dict

project5_dag = project5_dag()