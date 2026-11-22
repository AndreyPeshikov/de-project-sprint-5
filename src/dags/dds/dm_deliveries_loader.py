from logging import Logger
from typing import List

from dds import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DM_DeliveriesObj(BaseModel):
    id: int
    order_id: int
    courier_id: int
    rate_id: int
    tip_sum: float


class DM_DeliveriesOrigin:

    def list_deliveries(self, conn: Connection, threshold: int, limit: int) -> List[DM_DeliveriesObj]:
        with conn.cursor(row_factory=class_row(DM_DeliveriesObj)) as cur:
            cur.execute(
                """              
                SELECT dd.id, dor.id AS order_id, dc.id AS courier_id, dr.id AS rate_id, dd.object_value::json ->> 'tip_sum' AS tip_sum
                FROM stg.deliverysystem_deliveries dd
                    JOIN dds.dm_orders dor 
                        ON dor.order_key  = dd.object_value::json->> 'order_id'
                    JOIN dds.dm_couriers dc
                        ON dc.courier_id = dd.object_value::json->> 'courier_id'
                    JOIN dds.dm_rates dr
                        ON dr.rate_value = (dd.object_value::json->> 'rate')::int 
                    WHERE dd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DM_DeliveryDest:

    def insert_deliveries(self, conn: Connection, DM_Deliveries: DM_DeliveriesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(order_id, courier_id, rate_id, tip_sum)
                    VALUES (%(order_id)s, %(courier_id)s, %(rate_id)s, %(tip_sum)s);
                """,
                {
                    "order_id": DM_Deliveries.order_id,
                    "courier_id": DM_Deliveries.courier_id,
                    "rate_id": DM_Deliveries.rate_id,
                    "tip_sum": DM_Deliveries.tip_sum
                },
            )


class DM_DeliveriesLoader:
    WF_KEY = "example_deliveries_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = DM_DeliveryDest()
        self.origin = DM_DeliveriesOrigin()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
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
            load_queue = self.origin.list_deliveries(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.dds.insert_deliveries(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
