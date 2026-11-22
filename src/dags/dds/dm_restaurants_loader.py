from logging import Logger
from typing import List

from dds import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DM_RestaurantsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: str
    active_to: str


class DM_RestaurantsOrigin:

    def list_restaurants(self, conn: Connection, threshold: int, limit: int) -> List[DM_RestaurantsObj]:
        with conn.cursor(row_factory=class_row(DM_RestaurantsObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_value::json ->> '_id' as restaurant_id, object_value::json ->> 'name' as restaurant_name, update_ts::text  as active_from, '2099-12-31 00:00:00.000'::text AS active_to
                    FROM stg.ordersystem_restaurants ou
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DM_RestaurantsDest:

    def insert_restaurants(self, conn: Connection, DM_Restaurants: DM_RestaurantsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s);
                """,
                {
                    "restaurant_id": DM_Restaurants.restaurant_id,
                    "restaurant_name": DM_Restaurants.restaurant_name,
                    "active_from": DM_Restaurants.active_from,
                    "active_to": DM_Restaurants.active_to
                },
            )


class DM_RestaurantsLoader:
    WF_KEY = "res_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = DM_RestaurantsDest()
        self.origin = DM_RestaurantsOrigin()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
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
            load_queue = self.origin.list_restaurants(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.dds.insert_restaurants(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
