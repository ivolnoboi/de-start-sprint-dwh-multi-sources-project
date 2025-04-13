from typing import Dict, List


class QueryGenerator:
    def get_query(self, workflow_settings):
        pass

    def get_wf_settings(self):
        pass

    def get_wf_key(self):
        pass

    def get_saving_params(self, record):
        pass


class UserQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    SELECT
                        object_id,
                        object_value::json #>> '{{name}}',
                        object_value::json #>> '{{login}}',
                        MAX(update_ts) OVER()
                    FROM stg.ordersystem_users
                    WHERE update_ts > '{workflow_settings["last_loaded_ts"]}'::timestamp
                    ORDER BY update_ts;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_users_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.dm_users",
            "data": {
                        "user_id": record[0],
                        "user_name": record[1],
                        "user_login": record[2]
                    },
            "conflict_cond": "(user_id)",
            "update_columns": ["user_name", "user_login"]
        }


class RestaurantQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    SELECT
                        object_id,
                        object_value::json #>> '{{name}}',
                        update_ts,
                        '2099-12-31'::timestamp,
                        MAX(update_ts) OVER()
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > '{workflow_settings["last_loaded_ts"]}'::timestamp
                    ORDER BY update_ts;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_restaurants_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.dm_restaurants",
            "data": {
                        "restaurant_id": record[0],
                        "restaurant_name": record[1],
                        "active_from": record[2],
                        "active_to": record[3]
                    },
            "conflict_cond": "(restaurant_id)",
            "update_columns": ["restaurant_name", "active_from", "active_to"]
        }


class CourierQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    SELECT
                        object_id,
                        object_value::json #>> '{{name}}',
                        MAX(update_ts) OVER()
                    FROM stg.couriersystem_couriers cc
                    WHERE update_ts > '{workflow_settings["last_loaded_ts"]}'::timestamp;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_couriers_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.dm_couriers",
            "data": {
                        "courier_id": record[0],
                        "courier_name": record[1]
                    },
            "conflict_cond": "(courier_id)",
            "update_columns": ["courier_name"]
        }


class DeliveryQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    SELECT
                        object_id,
                        dc.id,
                        cd.object_value::json #>> '{{address}}',
                        dt.id,
                        cd.object_value::json #>> '{{rate}}',
                        cd.object_value::json #>> '{{tip_sum}}',
                        MAX(update_ts) OVER()
                    FROM stg.couriersystem_deliveries cd
                    INNER JOIN dds.dm_couriers dc
                    ON dc.courier_id = cd.object_value::json #>> '{{courier_id}}'
                    FULL JOIN dds.dm_timestamps dt
                    ON dt.ts = (cd.object_value::json #>> '{{delivery_ts}}')::timestamp
                    WHERE update_ts > '{workflow_settings["last_loaded_ts"]}'::timestamp;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_deliveries_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.dm_deliveries",
            "data": {
                        "delivery_id": record[0],
                        "courier_id": record[1],
                        "address": record[2],
                        "timestamp_id": record[3],
                        "rate": record[4],
                        "tip_sum": record[5]
                    },
            "conflict_cond": "(delivery_id)",
            "update_columns": ["courier_id", "address", "timestamp_id", "rate", "tip_sum"]
        }


class ProductQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    WITH menu AS (
                        SELECT
                            object_id,
                            JSON_ARRAY_ELEMENTS(object_value::json #> '{{menu}}') AS menu,
                            update_ts,
                            MAX(update_ts) OVER() AS last_update_ts
                        FROM stg.ordersystem_restaurants
                        WHERE update_ts > '{workflow_settings["last_loaded_ts"]}'::timestamp
                    )
                    SELECT
                        menu #>> '{{_id}}',
                        menu #>> '{{name}}',
                        menu #>> '{{price}}',
                        update_ts,
                        '2099-12-31'::timestamp,
                        dr.id,
                        last_update_ts
                    FROM menu m
                    INNER JOIN dds.dm_restaurants dr ON m.object_id = dr.restaurant_id;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_products_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.dm_products",
            "data": {
                        "product_id": record[0],
                        "product_name": record[1],
                        "product_price": record[2],
                        "active_from": record[3],
                        "active_to": record[4],
                        "restaurant_id": record[5]
                    },
            "conflict_cond": "(product_id)",
            "update_columns": ["product_name", "product_price", "active_from", "active_to", "restaurant_id"]
        }


class OrderQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    WITH delivery_order AS (
                        SELECT
                            cd.object_value::json #>> '{{order_id}}' AS order_id,
                            dd.id AS delivery_id
                            FROM stg.couriersystem_deliveries cd
                            INNER JOIN dds.dm_deliveries dd
                                ON dd.delivery_id = (cd.object_value::json #>> '{{delivery_id}}')
                    )
                    SELECT
                        oo.object_id,
                        oo.object_value::json #>> '{{final_status}}',
                        dr.id,
                        dt.id,
                        du.id,
                        dor.delivery_id,
                        MAX(oo.update_ts) OVER()
                    FROM stg.ordersystem_orders oo
                    INNER JOIN dds.dm_restaurants dr
                        ON (oo.object_value::json #>> '{{restaurant, id}}') = dr.restaurant_id
                    INNER JOIN dds.dm_timestamps dt
                        ON dt.ts = (oo.object_value::json #>> '{{date}}')::timestamp
                    INNER JOIN dds.dm_users du
                        ON du.user_id = (oo.object_value::json #>> '{{user, id}}')
                    INNER JOIN delivery_order dor
                        ON oo.object_id = dor.order_id
                    WHERE oo.update_ts > '{workflow_settings["last_loaded_ts"]}'::timestamp;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_orders_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.dm_orders",
            "data": {
                        "order_key": record[0],
                        "order_status": record[1],
                        "restaurant_id": record[2],
                        "timestamp_id": record[3],
                        "user_id": record[4],
                        "delivery_id": record[5]
                    },
            "conflict_cond": "(order_key)",
            "update_columns": ["order_status", "restaurant_id", "timestamp_id", "user_id", "delivery_id"]
        }


class ProductSaleQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    WITH order_product_info AS (
                        SELECT
                            JSON_ARRAY_ELEMENTS(object_value::json #> '{{order_items}}') #>> '{{id}}' AS product_id,
                            object_id AS order_id,
                            JSON_ARRAY_ELEMENTS(object_value::json #> '{{order_items}}') #>> '{{quantity}}' AS count,
                            (JSON_ARRAY_ELEMENTS(object_value::json #> '{{order_items}}') #>> '{{price}}')::numeric(19, 5) AS price,
                            (JSON_ARRAY_ELEMENTS(object_value::json #> '{{order_items}}') #>> '{{quantity}}')::int * (JSON_ARRAY_ELEMENTS(object_value::json #> '{{order_items}}') #>> '{{price}}')::numeric(19, 5) AS total_sum,
                            MAX(update_ts) OVER() AS last_update_ts
                        FROM stg.ordersystem_orders oo
                        WHERE update_ts > '{workflow_settings["last_loaded_order_system_ts"]}'::timestamp
                    ),
                    bonus_system AS (
                        SELECT
                            event_value::json #>> '{{order_id}}' AS order_id,
                            JSON_ARRAY_ELEMENTS(event_value::json #> '{{product_payments}}') #>> '{{product_id}}' AS product_id,
                            (JSON_ARRAY_ELEMENTS(event_value::json #> '{{product_payments}}') #>> '{{bonus_payment}}')::numeric(19, 5) AS bonus_payment,
                            (JSON_ARRAY_ELEMENTS(event_value::json #> '{{product_payments}}') #>> '{{bonus_grant}}')::numeric(19, 5) AS bonus_grant,
                            MAX(event_ts) OVER() AS last_event_ts
                        FROM stg.bonussystem_events be
                        WHERE event_type = 'bonus_transaction' AND event_ts > '{workflow_settings["last_loaded_bonus_system_ts"]}'::timestamp
                    )
                    SELECT
                        dp.id,
                        o.id,
                        opi.count,
                        opi.price,
                        opi.total_sum,
                        bs.bonus_payment,
                        bs.bonus_grant,
                        last_update_ts,
                        last_event_ts
                    FROM order_product_info opi
                    INNER JOIN bonus_system bs
                        ON opi.order_id = bs.order_id AND opi.product_id = bs.product_id
                    INNER JOIN dds.dm_orders o
                        ON opi.order_id = o.order_key
                    INNER JOIN dds.dm_products dp
                        ON opi.product_id = dp.product_id;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_order_system_ts", "last_loaded_bonus_system_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_product_sales_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.fct_product_sales",
            "data": {
                        "product_id": record[0],
                        "order_id": record[1],
                        "count": record[2],
                        "price": record[3],
                        "total_sum": record[4],
                        "bonus_payment": record[5],
                        "bonus_grant": record[6]
                    },
            "conflict_cond": "ON CONSTRAINT order_id_product_id_unique",
            "update_columns": ["count", "price", "total_sum", "bonus_payment", "bonus_grant"]
        }


class TimestampQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = f"""
                    WITH timestamps AS (
                        SELECT 
                            (object_value::json #>> '{{date}}')::timestamp AS ts,
                            update_ts AS update_ts,
                            'order_system' AS source
                            FROM stg.ordersystem_orders
                            WHERE (object_value::json #>> '{{final_status}}' = 'CLOSED' OR 
                                object_value::json #>> '{{final_status}}' = 'CANCELLED') AND
                                update_ts > '{workflow_settings["last_loaded_order_system_ts"]}'::timestamp
                            UNION
                            SELECT
                                (cd.object_value::json #>> '{{delivery_ts}}')::timestamp AS ts,
                                update_ts AS update_ts,
                                'delivery_system' AS source
                            FROM stg.couriersystem_deliveries cd
                            WHERE  update_ts > '{workflow_settings["last_loaded_delivery_system_ts"]}'::timestamp
                    )
                    SELECT
                        ts,
                        EXTRACT(YEAR FROM ts) AS YEAR,
                        EXTRACT(MONTH FROM ts) AS MONTH,
                        EXTRACT(DAY FROM ts) AS DAY,
                        ts::date AS date,
                        ts::time AS time,
                        MAX(CASE WHEN source = 'order_system' THEN update_ts END) OVER() AS order_system_max_ts,
                        MAX(CASE WHEN source = 'delivery_system' THEN update_ts END) OVER() AS delivery_system_max_ts
                    FROM timestamps;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_order_system_ts", "last_loaded_delivery_system_ts"]

    def get_wf_key(self) -> str:
        return "dds_dm_timestamps_stg_to_dds_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "dds.dm_timestamps",
            "data": {
                        "ts": record[0],
                        "year": record[1],
                        "month": record[2],
                        "day": record[3],
                        "date": record[4],
                        "time": record[5]
                    },
            "conflict_cond": "(ts)",
            "update_columns": ["year", "month", "day", "date", "time"]
        }


class DMCourierLedgerQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = """
                    WITH courier_aggregates AS (
                        SELECT
                            c.courier_id,
                            c.courier_name,
                            dt."year" AS settlement_year,
                            dt."month" AS settlement_month,
                            COUNT(ord.order_key) AS orders_count,
                            SUM(fps.total_sum) AS orders_total_sum,
                            AVG(dd.rate) AS rate_avg,
                            SUM(fps.total_sum) * 0.25 AS order_processing_fee,
                            SUM(dd.tip_sum) AS courier_tips_sum
                        FROM dds.dm_couriers c
                        INNER JOIN dds.dm_deliveries dd 
                            ON c.id = dd.courier_id
                        INNER JOIN dds.dm_orders ord
                            ON dd.id = ord.delivery_id
                        INNER JOIN dds.dm_timestamps dt
                            ON ord.timestamp_id = dt.id
                        INNER JOIN dds.fct_product_sales fps
                            ON fps.order_id = ord.id
                        GROUP BY c.courier_id, c.courier_name, settlement_year, settlement_month
                    ),
                    courier_info AS (
                        SELECT
                            *,
                            CASE
                                WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
                                WHEN rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
                                WHEN rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
                                ELSE GREATEST(orders_total_sum * 0.1, 200)
                            END AS courier_order_sum
                        FROM courier_aggregates
                    )
                    SELECT
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
                    FROM courier_info
                    ORDER BY settlement_year, settlement_month, courier_id;
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "cdm_dm_courier_ledger_dds_to_cdm_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "cdm.dm_courier_ledger",
            "data": {
                    "courier_id": record[0],
                    "courier_name": record[1],
                    "settlement_year": record[2],
                    "settlement_month": record[3],
                    "orders_count": record[4],
                    "orders_total_sum": record[5],
                    "rate_avg": record[6],
                    "order_processing_fee": record[7],
                    "courier_order_sum": record[8],
                    "courier_tips_sum": record[9],
                    "courier_reward_sum": record[10]
                },
            "conflict_cond": "ON CONSTRAINT couriers_report_courier_year_month_unique",
            "update_columns": ["courier_name", "orders_count", "orders_total_sum", "rate_avg", "order_processing_fee", "courier_order_sum", "courier_tips_sum", "courier_reward_sum"]
        }
    

class DMSettlementReportQueryGenerator(QueryGenerator):
    def get_query(self, workflow_settings):
        query = """
                    SELECT
                        dr.restaurant_id AS restaurant_id,
                        dr.restaurant_name AS restaurant_name,
                        dt."date" AS settlement_date,
                        COUNT(DISTINCT do2.order_key) AS orders_count,
                        SUM(fps.total_sum) AS orders_total_sum,
                        SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
                        SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
                        SUM(fps.total_sum) * 0.25 AS order_processing_fee,
                        SUM(fps.total_sum) - SUM(fps.bonus_payment) - SUM(fps.total_sum) * 0.25 AS restaurant_reward_sum
                    FROM dds.dm_restaurants dr
                    INNER JOIN dds.dm_orders do2
                        ON dr.id = do2.restaurant_id
                    INNER JOIN dds.dm_timestamps dt
                        ON do2.timestamp_id = dt.id
                    INNER JOIN dds.fct_product_sales fps
                        ON do2.id  = fps.order_id
                    WHERE do2.order_status = 'CLOSED'
                    GROUP BY dr.restaurant_id, dr.restaurant_name, dt."date"
                    ORDER BY dr.restaurant_id, dr.restaurant_name, dt."date";
                """
        return query

    def get_wf_settings(self) -> List[str]:
        return ["last_loaded_ts"]

    def get_wf_key(self) -> str:
        return "cdm_dm_settlement_report_dds_to_cdm_workflow"

    def get_saving_params(self, record) -> Dict:
        return {
            "table_name": "cdm.dm_settlement_report",
            "data": {
                    "restaurant_id": record[0],
                    "restaurant_name": record[1],
                    "settlement_date": record[2],
                    "orders_count": record[3],
                    "orders_total_sum": record[4],
                    "orders_bonus_payment_sum": record[5],
                    "orders_bonus_granted_sum": record[6],
                    "order_processing_fee": record[7],
                    "restaurant_reward_sum": record[8]
                },
            "conflict_cond": "ON CONSTRAINT restaurant_id_settlement_date_unique",
            "update_columns": ["restaurant_name", "orders_count", "orders_total_sum", "orders_bonus_payment_sum", "orders_bonus_granted_sum", "order_processing_fee", "restaurant_reward_sum"]
        }