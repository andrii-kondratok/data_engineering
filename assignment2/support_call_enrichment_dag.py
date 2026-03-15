"""
Hourly DAG: MySQL + JSON -> DuckDB

  MySQL (calls, employees)  ← базові дані дзвінків
  JSON файли                ← telephony дані (duration_sec, short_description)
  DuckDB                    ← тут вони вперше джойняться разом

Flow:
  detect_new_calls -> load_telephony_details -> transform_and_load_duckdb

Required Airflow Connection:
  Conn Id: mysql_support_calls  (MySQL, schema: support_calls)
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import duckdb

# ── Config ────────────────────────────────────────────────────────────────────
TELEPHONY_DIR = "/usr/local/airflow/include/telephony_json"
DUCKDB_PATH   = "/usr/local/airflow/data/support_calls.duckdb"
WATERMARK_VAR = "support_calls_last_loaded_call_time"
MYSQL_CONN_ID = "mysql_support_calls"

log = logging.getLogger(__name__)

with DAG(
    dag_id="support_call_enrichment",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
) as dag:

    # ── Task 1: detect_new_calls ──────────────────────────────────────────────
    # Читає watermark, питає MySQL які дзвінки нові, передає їх call_id через XCom.

    def detect_new_calls(**context):
        watermark = Variable.get(WATERMARK_VAR, default="1970-01-01 00:00:00")
        log.info("Watermark: %s", watermark)

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        rows = hook.get_records(
            "SELECT call_id FROM calls WHERE call_time > %(wm)s ORDER BY call_time",
            parameters={"wm": watermark},
        )

        new_ids = [r[0] for r in rows]
        log.info("New calls found: %d", len(new_ids))
        context["ti"].xcom_push(key="new_call_ids", value=new_ids)

    # ── Task 2: load_telephony_details ────────────────────────────────────────
    # Для кожного call_id читає JSON файл з telephony даними.
    # Якщо файл відсутній — підставляє пусті значення, не скіпає запис.

    def load_telephony_details(**context):
        new_ids = context["ti"].xcom_pull(task_ids="detect_new_calls", key="new_call_ids") or []

        if not new_ids:
            log.info("No new calls to process.")
            context["ti"].xcom_push(key="telephony_records", value=[])
            return

        valid = []
        for call_id in new_ids:
            path = os.path.join(TELEPHONY_DIR, f"{call_id}.json")
            try:
                with open(path) as f:
                    rec = json.load(f)
                if rec.get("duration_sec", 0) < 0:
                    rec["duration_sec"] = 0
            except (FileNotFoundError, json.JSONDecodeError):
                log.warning("%s: no JSON found, using empty values", call_id)
                rec = {"call_id": call_id, "duration_sec": 0, "short_description": ""}

            valid.append(rec)

        log.info("Telephony loaded: %d records", len(valid))
        context["ti"].xcom_push(key="telephony_records", value=valid)

    # ── Task 3: transform_and_load_duckdb ────────────────────────────────────
    # Тягне calls + employees з MySQL.
    # Джойнить з JSON даними (з XCom).
    # Результат — збагачений запис — пише в DuckDB.
    # MySQL при цьому не чіпаємо.

    def transform_and_load_duckdb(**context):
        telephony_records = context["ti"].xcom_pull(
            task_ids="load_telephony_details", key="telephony_records"
        ) or []

        if not telephony_records:
            log.info("Nothing to load.")
            return

        tel_by_id = {r["call_id"]: r for r in telephony_records}
        call_ids  = list(tel_by_id.keys())

        # Тягнемо calls + employees з MySQL
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        placeholders = ", ".join(["%s"] * len(call_ids))
        rows = hook.get_records(
            f"""
            SELECT c.call_id, c.call_time, c.phone, c.direction, c.status,
                   e.employee_id, e.full_name, e.team, e.role, e.hire_date
            FROM   calls c
            JOIN   employees e ON e.employee_id = c.employee_id
            WHERE  c.call_id IN ({placeholders})
            ORDER  BY c.call_time
            """,
            parameters=call_ids,
        )

        if not rows:
            log.warning("MySQL returned 0 rows.")
            return

        cols = ["call_id", "call_time", "phone", "direction", "status",
                "employee_id", "employee_name", "employee_team", "employee_role", "employee_hire_date"]

        # Джойнимо MySQL дані з JSON даними
        enriched = []
        for row in rows:
            rec = dict(zip(cols, row))
            tel = tel_by_id.get(rec["call_id"])
            if not tel:
                continue
            enriched.append((
                rec["call_id"],
                str(rec["call_time"]),
                rec["phone"],
                rec["direction"],
                rec["status"],
                rec["employee_id"],
                rec["employee_name"],
                rec["employee_team"],
                rec["employee_role"],
                str(rec["employee_hire_date"]),
                int(tel["duration_sec"]),
                tel["short_description"],
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            ))

        log.info("Rows to write: %d", len(enriched))

        # Пишемо в DuckDB — INSERT OR REPLACE не дублює при повторному рані
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
        with duckdb.connect(DUCKDB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS support_call_enriched (
                    call_id            VARCHAR PRIMARY KEY,
                    call_time          TIMESTAMP,
                    phone              VARCHAR,
                    direction          VARCHAR,
                    status             VARCHAR,
                    employee_id        INTEGER,
                    employee_name      VARCHAR,
                    employee_team      VARCHAR,
                    employee_role      VARCHAR,
                    employee_hire_date DATE,
                    duration_sec       INTEGER,
                    short_description  VARCHAR,
                    loaded_at          TIMESTAMP
                )
            """)
            conn.executemany(
                "INSERT OR REPLACE INTO support_call_enriched VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                enriched,
            )
            total = conn.execute("SELECT COUNT(*) FROM support_call_enriched").fetchone()[0]

        log.info("DuckDB total rows: %d", total)

        # Watermark оновлюється тільки після успішного запису
        new_watermark = max(r[1] for r in enriched)
        Variable.set(WATERMARK_VAR, new_watermark)
        log.info("Watermark advanced to: %s", new_watermark)

    # ── Task dependencies ─────────────────────────────────────────────────────
    t1 = PythonOperator(task_id="detect_new_calls",          python_callable=detect_new_calls)
    t2 = PythonOperator(task_id="load_telephony_details",    python_callable=load_telephony_details)
    t3 = PythonOperator(task_id="transform_and_load_duckdb", python_callable=transform_and_load_duckdb)

    t1 >> t2 >> t3