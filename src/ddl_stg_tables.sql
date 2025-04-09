CREATE TABLE IF NOT EXISTS stg.couriersystem_restaurants (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	object_id VARCHAR NOT NULL, -- ID ресторана
	object_value VARCHAR NOT NULL, -- информация о ресторане
	load_ts TIMESTAMP NOT NULL, -- время загрузки записи
	CONSTRAINT couriersystem_restaurants_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.couriersystem_couriers (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	object_id VARCHAR NOT NULL, -- ID курьера в БД
	object_value VARCHAR NOT NULL, -- информация о курьере
	load_ts TIMESTAMP NOT NULL, -- время загрузки записи
	CONSTRAINT couriersystem_couriers_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.couriersystem_deliveries (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	object_id VARCHAR NOT NULL, -- ID заказа
	object_value VARCHAR NOT NULL, -- информация о доставке
	load_ts TIMESTAMP NOT NULL, -- время загрузки записи
	CONSTRAINT couriersystem_deliveries_pkey PRIMARY KEY (id)
);
