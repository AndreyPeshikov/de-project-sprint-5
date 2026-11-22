DROP TABLE IF EXISTS cdm.dm_courier_ledger;

CREATE TABLE cdm.dm_courier_ledger (
	id serial NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int NOT NULL,
	settlement_month int NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	rate_avg numeric(3, 2) NOT NULL,
	order_processing_fee numeric(14, 2) NOT NULL,
	courier_order_sum numeric(14, 2) NOT NULL,
	courier_tips_sum numeric(14, 2) NOT NULL,
	courier_reward_sum numeric(14, 2) NOT NULL
);