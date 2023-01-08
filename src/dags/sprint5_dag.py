import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from lib import ConnectionBuilder, MongoConnect
from stg.stg_loaders.users_loader import UserLoader
from stg.stg_loaders.ranks_loader import RankLoader
from stg.stg_loaders.events_loader import EventLoader
from stg.order_system_restaurants_dag.pg_saver import PgSaver
from stg.order_system_users.pg_saver import PgSaver as UserPgSaver
from stg.order_system_orders.pg_saver import PgSaver as OrderPgSaver
from stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader
from stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader
from stg.order_system_users.user_loader import OrderSystemUserLoader
from stg.order_system_users.user_reader import OrderSystemUserReader
from stg.order_system_orders.order_loader import OrderLoader
from stg.order_system_orders.order_reader import OrderReader
from dds.loaders.users_loader import DDSUserLoader
from dds.loaders.restaurants_loader import DDSRestaurantLoader
from dds.loaders.timestamps_loader import DDSTimestampLoader
from dds.loaders.products_loader import DDSProductLoader
from dds.loaders.orders_loader import DDSOrderLoader
from dds.loaders.product_sales_loader import DDSProductSalesLoader
from cdm.dm_settlement_report_loader import SettlementReportLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждые 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    with TaskGroup(group_id='stg_load') as tg1:
        @task(task_id="ranks_load")
        def load_ranks():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
            rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        ranks_dict = load_ranks()

        @task(task_id="users_load")
        def load_users():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
            rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        users_dict = load_users()

        @task(task_id="events_load")
        def load_events():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
            rest_loader.load_events()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        events_dict = load_events()

        @task(task_id="restaurants_load")
        def load_restaurants():
            # Инициализируем класс, в котором реализована логика сохранения.
            pg_saver = PgSaver()

            # Инициализируем подключение у MongoDB.
            mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

            # Инициализируем класс, реализующий чтение данных из источника.
            collection_reader = RestaurantReader(mongo_connect)

            # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
            loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

            # Запускаем копирование данных.
            loader.run_copy()

        restaurant_loader = load_restaurants()

        @task(task_id="users_load")
        def load_order_system_users():
            # Инициализируем класс, в котором реализована логика сохранения.
            pg_saver = UserPgSaver()

            # Инициализируем подключение у MongoDB.
            mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

            # Инициализируем класс, реализующий чтение данных из источника.
            collection_reader = OrderSystemUserReader(mongo_connect)

            # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
            loader = OrderSystemUserLoader(collection_reader, dwh_pg_connect, pg_saver, log)

            # Запускаем копирование данных.
            loader.run_copy()

        user_loader = load_order_system_users()

        @task(task_id="orders_load")
        def load_order_system_orders():
            # Инициализируем класс, в котором реализована логика сохранения.
            pg_saver = OrderPgSaver()

            # Инициализируем подключение у MongoDB.
            mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

            # Инициализируем класс, реализующий чтение данных из источника.
            collection_reader = OrderReader(mongo_connect)

            # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
            loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)

            # Запускаем копирование данных.
            loader.run_copy()

        order_loader = load_order_system_orders()

        [ranks_dict, users_dict, events_dict, restaurant_loader, user_loader, order_loader]

    with TaskGroup(group_id='dds_load') as tg2:
        @task(task_id="dds_users_load")
        def load_users():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = DDSUserLoader(dwh_pg_connect, log)
            rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        ddsusers_dict = load_users()

        @task(task_id="dds_restaurants_load")
        def load_restaurants():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = DDSRestaurantLoader(dwh_pg_connect, log)
            rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        ddsrestaurants_dict = load_restaurants()

        @task(task_id="dds_timestamps_load")
        def load_timestamps():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = DDSTimestampLoader(dwh_pg_connect, log)
            rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        ddstimestamps_dict = load_timestamps()

        [ddsusers_dict, ddsrestaurants_dict, ddstimestamps_dict]

    with TaskGroup(group_id='dds2_load') as tg3:
        @task(task_id="dds_products_load")
        def load_products():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = DDSProductLoader(dwh_pg_connect, log)
            rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        ddsproducts_dict = load_products()

        @task(task_id="dds_orders_load")
        def load_orders():
            # создаем экземпляр класса, в котором реализована логика.
            rest_loader = DDSOrderLoader(dwh_pg_connect, log)
            rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
        ddsorders_dict = load_orders()

        [ddsproducts_dict, ddsorders_dict]

    @task(task_id="dds_product_sales_load")
    def load_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DDSProductSalesLoader(dwh_pg_connect, log)
        rest_loader.load_product_sales()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    ddsproductsales_dict = load_product_sales()

    @task(task_id="cdm_settlement_reports_load")
    def load_settlement_reports():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = SettlementReportLoader(dwh_pg_connect, log)
        rest_loader.load_settlement_report()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    cdm_settlement_report_dict = load_settlement_reports()

    # Далее задаем последовательность выполнения тасков.
    # [ranks_dict, users_dict, events_dict, restaurant_loader, user_loader, order_loader]
    # [ddsusers_dict, ddsrestaurants_dict, ddstimestamps_dict]
    # [ddsproducts_dict, ddsorders_dict]
    tg1 >> tg2 >> tg3 >> ddsproductsales_dict >> cdm_settlement_report_dict # type: ignore

sprint5_dag = sprint5_dag()