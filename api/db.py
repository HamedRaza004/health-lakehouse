import trino
import os
from dotenv import load_dotenv

load_dotenv()


def get_trino_connection():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", 8085)),
        user=os.getenv("TRINO_USER", "hamed"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema="gold",
    )


def run_query(sql: str) -> list[dict]:
    conn = get_trino_connection()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]
