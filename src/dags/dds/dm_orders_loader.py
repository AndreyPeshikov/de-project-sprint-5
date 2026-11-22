from logging import Logger
from typing import List

from dds import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DM_OrdersObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int


class DM_OrdersOrigin:

    def list_orders(self, conn: Connection, threshold: int, limit: int) -> List[DM_OrdersObj]:
        with conn.cursor(row_factory=class_row(DM_OrdersObj)) as cur:
            cur.execute(
                """
                    WITH order_raw AS 
                        (
                        SELECT id
                            , object_value::json ->> '_id' AS order_key
                            , json_array_elements(object_value::json -> 'statuses')::json ->> 'status' AS status
                            , (json_array_elements(object_value::json -> 'statuses')::json ->> 'dttm')::timestamp AS dttm
                            , object_value::json -> 'restaurant'->>'id' as restaurant_id
                            , object_value::json -> 'user'->>'id' as user_id
                        FROM stg.ordersystem_orders oo
                        )
                    ,order_raw_rn as
                        (
                        SELECT
                            *
                            , row_number() over(PARTITION BY order_key ORDER BY dttm desc) AS rn
                        from order_raw
                        )
                    SELECT t.id
                        , order_key
                        , status as order_status
                        ,r.id AS restaurant_id
                        ,dt.id AS timestamp_id
                        ,du.id AS user_id
                    FROM order_raw_rn t
                        JOIN dds.dm_restaurants r
                        ON t.restaurant_id = r.restaurant_id
                        JOIN dds.dm_timestamps dt 
                        ON t.dttm = dt.ts
                        JOIN dds.dm_users du 
                        ON du.user_id = t.user_id 
                    WHERE rn = 1
                    and t.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY t.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DM_OrdersDest:

    def insert_orders(self, conn: Connection, DM_Orders: DM_OrdersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s);
                """,
                {
                    "order_key": DM_Orders.order_key,
                    "order_status": DM_Orders.order_status,
                    "restaurant_id": DM_Orders.restaurant_id,
                    "timestamp_id": DM_Orders.timestamp_id,
                    "user_id": DM_Orders.user_id
                },
            )


class DM_OrdersLoader:
    WF_KEY = "orders_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = DM_OrdersDest()
        self.origin = DM_OrdersOrigin()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.dds.insert_orders(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
