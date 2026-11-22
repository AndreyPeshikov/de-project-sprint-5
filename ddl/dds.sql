-- 1. dds.dm_couriers (создать):
DROP TABLE IF EXISTS dds.dm_couriers CASCADE ;

CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
);

-- 2. dds.dm_rates (создать):
DROP TABLE IF EXISTS dds.dm_rates CASCADE ;

CREATE TABLE dds.dm_rates (
	id serial4 NOT NULL PRIMARY KEY,
	rate_value int NOT NULL
);

INSERT INTO dds.dm_rates 
(rate_value)
VALUES
(1),
(2),
(3),
(4),
(5);


--4. dds.fct_deliveries (создать):

DROP TABLE IF EXISTS  dds.fct_deliveries;

CREATE TABLE dds.fct_deliveries (
	id serial4 NOT NULL PRIMARY KEY,
	order_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	rate_id int4 NOT NULL,
	tip_sum numeric(14, 2) NOT NULL
);

ALTER TABLE dds.fct_deliveries ADD CONSTRAINT dm_fct_deliveries_fk_order_id FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
ALTER TABLE dds.fct_deliveries ADD CONSTRAINT dm_fct_deliveries_fk_courier_id FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.fct_deliveries ADD CONSTRAINT dm_fct_deliveries_fk_rate_id FOREIGN KEY (rate_id) REFERENCES dds.dm_rates(id);


CREATE TABLE dds.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_unique UNIQUE (workflow_key)
);
