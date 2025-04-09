CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	courier_id VARCHAR NOT NULL,
	courier_name VARCHAR NOT NULL,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT dm_couriers_courier_id_unique UNIQUE (courier_id)
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	delivery_id VARCHAR NOT NULL,
	courier_id INT NOT NULL,
	address VARCHAR NOT NULL,
	timestamp_id INT NOT NULL,
	rate INT NOT NULL,
	tip_sum NUMERIC(14, 2) NOT NULL,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_rate_check CHECK ((rate >= 1) AND (rate <= 5)),
	CONSTRAINT dm_deliveries_tip_sum_check CHECK (tip_sum >= 0),
	CONSTRAINT dm_deliveries_fkey_couriers FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers (id),
	CONSTRAINT dm_deliveries_fkey_timestamps FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps (id)
);

ALTER TABLE dds.dm_orders ADD COLUMN delivery_id INT;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_fkey_deliveries FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries (id);