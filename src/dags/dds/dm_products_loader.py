from logging import Logger
from typing import List

from dds import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DM_ProductsObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: str
    active_to: str
    restaurant_id: int


class DM_ProductsOrigin:

    def list_products(self, conn: Connection, threshold: int, limit: int) -> List[DM_ProductsObj]:
        with conn.cursor(row_factory=class_row(DM_ProductsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        t.id
                        ,product_id
                        ,product_name
                        ,product_price
                        ,t.active_from
                        ,LEAD(t.active_from, 1,'2099-12-31 00:00:00.000') over(PARTITION BY product_id ORDER BY t.active_from) AS active_to
                        ,r.id AS restaurant_id
                    from
                    (
                        SELECT min(id) AS id, products ->>'id' AS product_id, products ->>'name' AS product_name,products ->>'price' AS product_price, min(active_from) AS active_from, restaurant_id
                        FROM
                        (
                            SELECT id, json_array_elements((object_value::json -> 'order_items')) AS products, update_ts::text  as active_from, object_value::json -> 'restaurant'->>'id' as restaurant_id
                            FROM stg.ordersystem_orders oo
                        ) a
                        GROUP BY products ->>'id', products ->>'name',products ->>'price', restaurant_id
                    ) t
                    LEFT JOIN dds.dm_restaurants r
                        ON t.restaurant_id = r.restaurant_id 
                    WHERE t.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY t.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DM_ProductsDest:

    def insert_products(self, conn: Connection, DM_Products: DM_ProductsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s);
                """,
                {
                    "product_id": DM_Products.product_id,
                    "product_name": DM_Products.product_name,
                    "product_price": DM_Products.product_price,
                    "active_from": DM_Products.active_from,
                    "active_to": DM_Products.active_to,
                    "restaurant_id": DM_Products.restaurant_id
                },
            )


class DM_ProductsLoader:
    WF_KEY = "products_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = DM_ProductsDest()
        self.origin = DM_ProductsOrigin()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
            load_queue = self.origin.list_products(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.dds.insert_products(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
