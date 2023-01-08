DROP TABLE IF EXISTS stg.deliverysystem_couriers;
CREATE TABLE stg.deliverysystem_couriers (
	id serial PRIMARY KEY,
	created_at timestamp DEFAULT(NOW()::timestamp),
	courier_id varchar NOT NULL,
	object_value text
);


DROP TABLE IF EXISTS stg.deliverysystem_deliveries;
CREATE TABLE stg.deliverysystem_deliveries (
	id serial PRIMARY KEY,
	created_at timestamp DEFAULT(NOW()::timestamp),
	delivery_id varchar NOT NULL,
	object_value text
);