CREATE TABLE IF NOT EXISTS stg.couriersystem_restaurants (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	restaurant_id VARCHAR NOT NULL,
	restaurant_name VARCHAR NOT NULL,
	CONSTRAINT couriersystem_restaurants_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.couriersystem_couriers (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	courier_id VARCHAR NOT NULL,
	courier_name VARCHAR NOT NULL,
	CONSTRAINT couriersystem_couriers_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.couriersystem_deliveries (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	order_id VARCHAR NOT NULL,
	order_ts TIMESTAMP NOT NULL,
	delivery_id VARCHAR NOT NULL,
	courier_id VARCHAR NOT NULL,
	address VARCHAR NOT NULL,
	delivery_ts TIMESTAMP NOT NULL,
	rate INT NOT NULL,
	sum NUMERIC(14, 2) NOT NULL,
	tip_sum NUMERIC(14, 2) NOT NULL,
	CONSTRAINT couriersystem_deliveries_pkey PRIMARY KEY (id)
);