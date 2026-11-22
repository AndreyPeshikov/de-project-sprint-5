from logging import Logger
from typing import List

from dds import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DM_TimestampsObj(BaseModel):
    id: int
    ts: str
    year: int
    month: int
    day: int
    date: str
    time: str


class DM_TimestampsOrigin:

    def list_timestamps(self, conn: Connection, threshold: int, limit: int) -> List[DM_TimestampsObj]:
        with conn.cursor(row_factory=class_row(DM_TimestampsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,dt::varchar AS ts
                        ,extract('year' FROM dt) AS year
                        ,extract('month' FROM dt) AS month 
                        ,extract('day' FROM dt) AS day 
                        ,dt::date::varchar AS date 
                        ,dt::time::varchar AS time
                    from
                    (
                        SELECT id,(status ->> 'dttm')::timestamp AS dt
                        FROM
                        (
                            SELECT id, json_array_elements((object_value::json -> 'statuses')) AS status
                            FROM stg.ordersystem_orders oo 
                        ) a
                        WHERE status ->> 'status' IN ('CLOSED', 'CANCELLED')
                    ) t
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


class DM_TimestampsDest:

    def insert_timestamps(self, conn: Connection, DM_Timestamps: DM_TimestampsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s);
                """,
                {
                    "ts": DM_Timestamps.ts,
                    "year": DM_Timestamps.year,
                    "month": DM_Timestamps.month,
                    "day": DM_Timestamps.day,
                    "date": DM_Timestamps.date,
                    "time": DM_Timestamps.time
                },
            )


class DM_TimestampsLoader:
    WF_KEY = "tm_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = DM_TimestampsDest()
        self.origin = DM_TimestampsOrigin()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
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
            load_queue = self.origin.list_timestamps(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.dds.insert_timestamps(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
