from logging import Logger
from typing import List

from dds import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DM_ProductsalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class DM_ProductsalesOrigin:

    def list_productsales(self, conn: Connection, threshold: int, limit: int) -> List[DM_ProductsalesObj]:
        with conn.cursor(row_factory=class_row(DM_ProductsalesObj)) as cur:
            cur.execute(
                """              
                SELECT 
                        t.id
                        ,dp.id AS product_id
                        ,o.id AS order_id
                        ,count
                        ,price
                        ,count * price AS total_sum
                        , 0 AS bonus_payment
                        , 0 AS bonus_grant
                    from
                    (
                        SELECT id
                        	,items::json ->> 'id' AS product_id
                        	,order_id
                        	,(items::json ->> 'quantity')::int AS count
                        	,(items::json ->> 'price')::numeric(19, 5) AS price
                        FROM
                        (
                            SELECT id
                            	, json_array_elements(object_value::json -> 'order_items') AS items
                            	, object_value::json ->> '_id' as order_id
                            	, object_value::json -> 'restaurant'->>'id' as restaurant_id
                            FROM stg.ordersystem_orders oo
                        ) a
                    ) t
                    LEFT JOIN dds.dm_products dp 
                        ON t.product_id = dp.product_id 
                    LEFT JOIN dds.dm_orders o 
                        ON t.order_id  = o.order_key 
                    WHERE t.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DM_ProductsalesDest:

    def insert_productsales(self, conn: Connection, DM_Productsales: DM_ProductsalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s);
                """,
                {
                    "product_id": DM_Productsales.product_id,
                    "order_id": DM_Productsales.order_id,
                    "count": DM_Productsales.count,
                    "price": DM_Productsales.price,
                    "total_sum": DM_Productsales.total_sum,
                    "bonus_payment": DM_Productsales.bonus_payment,
                    "bonus_grant": DM_Productsales.bonus_grant
                },
            )


class DM_ProductsalesLoader:
    WF_KEY = "productsales_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = DM_ProductsalesDest()
        self.origin = DM_ProductsalesOrigin()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_productsales(self):
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
            load_queue = self.origin.list_productsales(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} productsales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.dds.insert_productsales(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
