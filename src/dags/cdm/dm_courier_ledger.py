from logging import Logger
from typing import List
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DM_Courier_ledgerObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class DM_Courier_ledgerOrigin:

    def list_courier_ledger(self, conn: Connection) -> List[DM_Courier_ledgerObj]:
        with conn.cursor(row_factory=class_row(DM_Courier_ledgerObj)) as cur:
            cur.execute(
                """
                   WITH
                    delivery AS 
                    (
                        SELECT courier_id, dt."year" , dt."month" , avg(dr.rate_value)::numeric(3,2) AS rate_avg, sum(fd.tip_sum ) AS tip_sum
                        FROM dds.fct_deliveries fd
                            JOIN dds.dm_rates dr 
                                ON fd.rate_id = dr.id
                            JOIN dds.dm_orders dor
                                ON dor.id = fd.order_id
                            JOIN dds.dm_timestamps dt 
                                ON dt.id = dor.timestamp_id 
                        GROUP BY courier_id, dt."year" , dt."month"
                    )
                    , pre_calc AS 
                    (
                    SELECT fd.courier_id
                        ,dc.courier_name AS courier_name
                        ,dt."year" AS settlement_year
                        ,dt."month" AS settlement_month
                        ,count(DISTINCT fps.order_id ) AS orders_count
                        ,sum(fps.total_sum) AS orders_total_sum
                        ,sum(fps.total_sum) * 0.25 AS order_processing_fee
                    FROM dds.fct_product_sales fps
                        JOIN dds.fct_deliveries fd 
                            ON fps.order_id = fd.order_id 
                        JOIN dds.dm_couriers dc 
                            ON dc.id = fd.courier_id
                        JOIN dds.dm_orders dor
                            ON dor.id = fps.order_id
                        JOIN dds.dm_timestamps dt 
                            ON dt.id = dor.timestamp_id
                    GROUP BY fd.courier_id
                        ,dc.courier_name
                        ,dt."year"
                        ,dt."month"
                    )
                    SELECT *
                        ,CASE
                            WHEN d.rate_avg < 4 THEN 0.05 * pc.orders_total_sum 
                            WHEN d.rate_avg < 4.5 THEN 0.07 * pc.orders_total_sum 
                            WHEN d.rate_avg < 4.9 THEN 0.08 * pc.orders_total_sum 
                            else 0.1 * pc.orders_total_sum 
                        END	AS courier_order_sum
                        ,d.tip_sum AS courier_tips_sum
                        ,CASE
                            WHEN d.rate_avg < 4 THEN 0.05 * pc.orders_total_sum 
                            WHEN d.rate_avg < 4.5 THEN 0.07 * pc.orders_total_sum 
                            WHEN d.rate_avg < 4.9 THEN 0.08 * pc.orders_total_sum 
                            else 0.1 * pc.orders_total_sum 
                        END	+ d.tip_sum * 0.95 AS courier_reward_sum
                    FROM pre_calc pc
                        JOIN delivery d
                            ON pc.courier_id = d.courier_id 
                                AND d."month" = pc.settlement_month 
                                AND d."year" = pc.settlement_year;
                """
            )
            objs = cur.fetchall()
        return objs


class DM_Courier_ledgerDest:

    def insert_courier_ledger(self, conn: Connection, DM_Courier_ledger: DM_Courier_ledgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s);
                """,
                {
                    "courier_id": DM_Courier_ledger.courier_id,
                    "courier_name": DM_Courier_ledger.courier_name,
                    "settlement_year": DM_Courier_ledger.settlement_year,
                    "settlement_month": DM_Courier_ledger.settlement_month,
                    "orders_count": DM_Courier_ledger.orders_count,
                    "orders_total_sum": DM_Courier_ledger.orders_total_sum,
                    "rate_avg": DM_Courier_ledger.rate_avg,
                    "order_processing_fee": DM_Courier_ledger.order_processing_fee,
                    "courier_order_sum": DM_Courier_ledger.courier_order_sum,
                    "courier_tips_sum": DM_Courier_ledger.courier_tips_sum,
                    "courier_reward_sum": DM_Courier_ledger.courier_reward_sum,
                },
            )

class DM_CourierLedgerLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.cdm = DM_Courier_ledgerDest()
        self.origin = DM_Courier_ledgerOrigin()
        self.log = log

    def load_courier_ledger(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                        cur.execute(
                                        """
                                            truncate cdm.dm_courier_ledger;
                                        """
                                    )
            # Вычитываем очередную пачку объектов.
            load_queue = self.origin.list_courier_ledger(conn)
            self.log.info(f"Found {len(load_queue)} courier_ledger to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rep in load_queue:
                self.cdm.insert_courier_ledger(conn, rep)
