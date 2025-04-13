from datetime import datetime
from typing import Any, Dict, List, Tuple

from psycopg import Connection


def make_insert_str(data: Dict) -> Tuple[str, str]:
    columns = data.keys()

    column_names_str = ', '.join(columns)
    column_values_str = ', '.join([f'%({column})s' for column in columns])

    return column_names_str, column_values_str


def make_set_expression(update_columns: List) -> str:
    expression_str = ''

    for column_nm in update_columns:
        expression_str += f'{column_nm} = EXCLUDED.{column_nm}, '

    return expression_str[:-2]


class PgSaver:

    def save_object(self, conn: Connection, table_name: str, data: Dict, conflict_cond: str, update_columns: List):
        with conn.cursor() as cur:

            column_names, column_values = make_insert_str(data)
            set_expression = make_set_expression(update_columns)

            cur.execute(
                f"""
                    INSERT INTO {table_name}({column_names})
                    VALUES ({column_values})
                    ON CONFLICT {conflict_cond} DO UPDATE
                    SET
                        {set_expression};
                """,
                data
            )
