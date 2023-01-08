DROP TABLE IF EXISTS dds.dm_couriers;
CREATE TABLE dds.dm_couriers (
	id serial PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL
);

ALTER TABLE dds.dm_orders 
ADD COLUMN courier_id int REFERENCES dds.dm_couriers (id) ON UPDATE CASCADE;

DROP TABLE IF EXISTS dds.dm_deliveries;
CREATE TABLE dds.dm_deliveries (
	id serial PRIMARY KEY,
	delivery_key varchar NOT NULL,
	order_id int4 NOT NULL REFERENCES dds.dm_orders (id),
	courier_id int4 NOT NULL REFERENCES dds.dm_couriers (id)
);

DROP TABLE IF EXISTS dds.fct_deliveries;
CREATE TABLE dds.fct_deliveries (
	id serial PRIMARY KEY,
	delivery_id int4 NOT NULL REFERENCES dds.dm_deliveries (id),
	rate int4 DEFAULT (0) CHECK (rate >= 1 AND rate <= 5),
	sum numeric(14,2) DEFAULT (0) CHECK(sum >= 0),
	tip_sum numeric(14,2) DEFAULT (0) CHECK(tip_sum >= 0)
);