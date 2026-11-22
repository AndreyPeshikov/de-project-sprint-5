from datetime import datetime
from logging import Logger

from stg.pg_saver import PgSaverCourier
from stg.courier_reader import CourierReader
from lib import PgConnect
from lib.dict_util import json2str


class CourierLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_ordersystem_restaurants_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, api_loader: CourierReader, pg_dest: PgConnect, pg_saver: PgSaverCourier, logger: Logger) -> None:
        self.collection_loader = api_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            limit = 50
            offset = 0
            load_queue = self.collection_loader.get_couriers(limit, offset)
            while len(load_queue) > 0:
                self.log.info(f"Found {len(load_queue)} couriers.")
                if not load_queue:
                    self.log.info("Quitting.")
                    return 0
                i = 0
                for d in load_queue:
                    self.pg_saver.save_object(conn, str(d["_id"]), d)

                    i += 1
                    if i % self._LOG_THRESHOLD == 0:
                        self.log.info(f"processed {i} of {len(load_queue)} while syncing couriers.")
                offset += limit
                load_queue = self.collection_loader.get_couriers(limit, offset)

            self.log.info(f"Finishing work")

            return len(load_queue)
