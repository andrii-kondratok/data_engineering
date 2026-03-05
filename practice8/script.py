from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# MySQL connection settings – adjust to match your environment
# ---------------------------------------------------------------------------
MYSQL_CONN = dict(
    host="host.docker.internal",
    port=3306,
    user="root",          # change as needed
    password="****",      # change as needed
    database="training_dw",
)

OUTPUT_DIR = "/usr/local/airflow/data"
TEMP_DIR   = tempfile.gettempdir()


# ---------------------------------------------------------------------------
# Task 1 – Extract aggregated data from MySQL
# ---------------------------------------------------------------------------
def extract_from_mysql(**context) -> None:
    """
    Runs the aggregation query against MySQL.
    Serialises the result rows to a JSON temp-file and pushes the file-path
    to XCom so downstream tasks can locate it.
    """
    import mysql.connector  # pip install mysql-connector-python

    query = """
        SELECT
            c.city,
            COUNT(o.order_id)          AS paid_orders_cnt,
            SUM(o.amount_usd)          AS paid_revenue_usd
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        WHERE o.status = 'PAID'
        GROUP BY c.city
        ORDER BY c.city
    """

    conn   = mysql.connector.connect(**MYSQL_CONN)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Convert Decimal → float so JSON can serialise them
    rows = [
        {k: float(v) if hasattr(v, "__float__") and not isinstance(v, (int, str)) else v
         for k, v in row.items()}
        for row in rows
    ]

    # Write to a temp JSON file
    tmp_path = os.path.join(TEMP_DIR, "extracted_rows.json")
    with open(tmp_path, "w") as fh:
        json.dump(rows, fh)

    # Push the file path via XCom
    context["ti"].xcom_push(key="extracted_path", value=tmp_path)
    print(f"Extracted {len(rows)} rows → {tmp_path}")


# ---------------------------------------------------------------------------
# Task 2 – Transform with Polars
# ---------------------------------------------------------------------------
def transform_polars(**context) -> None:
    """
    Pulls the JSON path from XCom, loads into a Polars DataFrame,
    enforces the expected schema, adds as_of_date, and saves as parquet.
    The parquet path is pushed to XCom for the next task.
    """
    ti = context["ti"]
    ds = context["ds"]  # Airflow logical date, e.g. "2026-01-01"

    # Pull extracted file path from task 1
    extracted_path: str = ti.xcom_pull(task_ids="extract_from_mysql",
                                        key="extracted_path")

    with open(extracted_path) as fh:
        rows = json.load(fh)

    # ---------- Polars work ----------
    schema = {
        "city":             pl.Utf8,
        "paid_orders_cnt":  pl.Int64,
        "paid_revenue_usd": pl.Float64,
    }

    df = pl.DataFrame(rows, schema=schema)

    # Add the Airflow execution date column
    df = df.with_columns(pl.lit(ds).alias("as_of_date"))

    print(df)

    # Save as parquet for the next task
    parquet_path = os.path.join(TEMP_DIR, "city_paid_metrics.parquet")
    df.write_parquet(parquet_path)

    # Push parquet path via XCom
    ti.xcom_push(key="parquet_path", value=parquet_path)
    print(f"Transformed DataFrame written → {parquet_path}")


# ---------------------------------------------------------------------------
# Task 3 – Write CSV
# ---------------------------------------------------------------------------
def write_csv(**context) -> None:
    """
    Pulls the parquet path from XCom, loads it with Polars,
    and writes the final CSV to /opt/airflow/data/.
    """
    ti = context["ti"]
    ds = context["ds"]

    parquet_path: str = ti.xcom_pull(task_ids="transform_polars",
                                      key="parquet_path")

    df = pl.read_parquet(parquet_path)

    # Guarantee column order in the output CSV
    df = df.select(["city", "paid_orders_cnt", "paid_revenue_usd", "as_of_date"])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, f"city_paid_metrics_{ds}.csv")
    df.write_csv(csv_path)
    print(f"CSV written → {csv_path}")
    print(df)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="mysql_polars_to_csv",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["mysql", "polars", "csv"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_from_mysql",
        python_callable=extract_from_mysql,
    )

    t2 = PythonOperator(
        task_id="transform_polars",
        python_callable=transform_polars,
    )

    t3 = PythonOperator(
        task_id="write_csv",
        python_callable=write_csv,
    )

    t1 >> t2 >> t3
