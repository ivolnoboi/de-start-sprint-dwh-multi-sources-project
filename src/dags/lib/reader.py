from datetime import datetime
from typing import Dict, List

from lib import PgConnect


class Reader:
    def __init__(self, pg_dest: PgConnect) -> None:
        self.pg_dest = pg_dest

    def get_data(self, query: str) -> List[Dict]:
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                data = cur.fetchall()

        return data
