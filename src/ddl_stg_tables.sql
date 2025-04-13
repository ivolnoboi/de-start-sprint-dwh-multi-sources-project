CREATE TABLE IF NOT EXISTS stg.couriersystem_couriers (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	object_id VARCHAR NOT NULL, -- ID курьера в БД
	object_value VARCHAR NOT NULL, -- информация о курьере
	update_ts TIMESTAMP NOT NULL, -- время загрузки записи
	CONSTRAINT couriersystem_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT couriersystem_couriers_object_id_uindex UNIQUE (object_id)
);


CREATE TABLE IF NOT EXISTS stg.couriersystem_deliveries (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	object_id VARCHAR NOT NULL, -- ID заказа
	object_value VARCHAR NOT NULL, -- информация о доставке
	update_ts TIMESTAMP NOT NULL, -- время загрузки записи
	CONSTRAINT couriersystem_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT couriersystem_deliveries_object_id_uindex UNIQUE (object_id)
);