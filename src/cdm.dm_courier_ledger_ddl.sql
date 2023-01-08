DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger (
	id serial PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year SMALLINT NOT NULL CHECK(settlement_year >= 2022 AND settlement_year < 2099),
	settlement_month varchar NOT NULL,
	orders_count SMALLINT DEFAULT (0) CHECK(orders_count >= 0),
	orders_total_sum numeric(14,2) DEFAULT (0) CHECK(orders_total_sum >= 0),
	rate_avg numeric(14,2) DEFAULT (0) CHECK(rate_avg >= 0),
	order_processing_fee numeric(14,2) DEFAULT (0) CHECK(order_processing_fee >= 0),
	courier_order_sum numeric(14,2) DEFAULT (0) CHECK(courier_order_sum >= 0),
	courier_tips_sum numeric(14,2) DEFAULT (0) CHECK(courier_tips_sum >= 0),
	courier_reward_sum numeric(14,2) DEFAULT (0) CHECK(courier_reward_sum >= 0),
	CONSTRAINT dm_courier_ledger_courier_id_settlement_month_unq UNIQUE (courier_id, settlement_month)
);