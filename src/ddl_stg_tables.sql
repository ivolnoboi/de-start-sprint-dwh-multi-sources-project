CREATE TABLE IF NOT EXISTS stg.couriersystem_restaurants (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	restaurant_id VARCHAR NOT NULL, -- ID ресторана
	restaurant_name VARCHAR NOT NULL, -- наименование ресторана
	CONSTRAINT couriersystem_restaurants_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.couriersystem_couriers (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	courier_id VARCHAR NOT NULL, -- ID курьера в БД
	courier_name VARCHAR NOT NULL, -- имя курьера
	CONSTRAINT couriersystem_couriers_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS stg.couriersystem_deliveries (
	id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
	order_id VARCHAR NOT NULL, -- ID заказа
	order_ts TIMESTAMP NOT NULL, -- дата и время создания заказа
	delivery_id VARCHAR NOT NULL, -- ID доставки
	courier_id VARCHAR NOT NULL, -- ID курьера
	address VARCHAR NOT NULL, -- адрес доставки
	delivery_ts TIMESTAMP NOT NULL, -- дата и время совершения доставки
	rate INT NOT NULL, -- рейтинг доставки, который выставляет покупатель
	sum NUMERIC(14, 2) NOT NULL, -- сумма заказа
	tip_sum NUMERIC(14, 2) NOT NULL, -- сумма чаевых, которые оставил покупатель курьеру (в руб.)
	CONSTRAINT couriersystem_deliveries_pkey PRIMARY KEY (id)
);
