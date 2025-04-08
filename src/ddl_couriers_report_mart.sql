CREATE TABLE IF NOT EXISTS cdm.couriers_report (
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
	courier_id VARCHAR NOT NULL, -- ID курьера
	courier_name VARCHAR NOT NULL, -- Ф.И.О. курьера
	settlement_year SMALLINT NOT NULL, -- год отчёта
	settlement_month SMALLINT NOT NULL, -- месяц отчёта, где 1 — январь и 12 — декабрь
	orders_count INT NOT NULL, -- количество заказов за период (месяц)
	orders_total_sum NUMERIC(14, 2) NOT NULL, -- общая стоимость заказов
	rate_avg NUMERIC(4, 2) NOT NULL, -- средний рейтинг курьера по оценкам пользователей
	order_processing_fee NUMERIC(14, 2) NOT NULL, -- сумма, удержанная компанией за обработку заказов
	courier_order_sum NUMERIC(14, 2) NOT NULL, -- сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы
	courier_tips_sum NUMERIC(14, 2) NOT NULL, -- сумма, которую пользователи оставили курьеру в качестве чаевых
	courier_reward_sum NUMERIC(14, 2) NOT NULL, -- сумма, которую необходимо перечислить курьеру
	CONSTRAINT couriers_report_pkey PRIMARY KEY (id),
	CONSTRAINT couriers_report_courier_id_unique UNIQUE (courier_id),
	CONSTRAINT couriers_report_settlement_year_check CHECK ((settlement_year >= 2022) AND (settlement_year < 2500)),
	CONSTRAINT couriers_report_settlement_month_check CHECK ((settlement_month >= 1) AND (settlement_month <= 12)),
	CONSTRAINT orders_count_greater_than_zero CHECK (orders_count >= 0),
	CONSTRAINT orders_total_sum_greater_than_zero CHECK (orders_total_sum >= 0),
	CONSTRAINT rate_avg_check CHECK ((rate_avg >= 0) AND (rate_avg <= 5)),
	CONSTRAINT order_processing_fee_greater_than_zero CHECK (order_processing_fee >= 0),
	CONSTRAINT courier_order_sum_greater_than_zero CHECK (courier_order_sum >= 0),
	CONSTRAINT courier_tips_sum_greater_than_zero CHECK (courier_tips_sum >= 0),
	CONSTRAINT courier_reward_sum_greater_than_zero CHECK (courier_reward_sum >= 0)
);