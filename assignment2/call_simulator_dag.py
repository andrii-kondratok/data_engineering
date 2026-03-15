import json
import os
import random
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

TELEPHONY_DIR = "/usr/local/airflow/include/telephony_json"
MYSQL_CONN_ID = "mysql_support_calls"

DESCRIPTIONS = [
    "Customer reported an incorrect charge. Agent issued a credit.",
    "Customer could not log in. Agent guided through password reset.",
    "Customer asked about upgrading plan. Agent sent a quote by email.",
    "Technical issue with desktop client. Agent fixed remotely.",
    "Customer reported a missed delivery. Agent raised a logistics ticket.",
    "Outbound retention call. Customer accepted a loyalty discount.",
    "Billing dispute — double charge. Agent initiated a refund.",
    "Customer requested email change. Agent verified and updated.",
    "VIP client reported slow dashboard. Escalated to Tier 2.",
    "Customer wanted to cancel. Agent offered free pause. Customer agreed.",
]

with DAG(
    dag_id="call_simulator",
    schedule="*/30 * * * *",   # кожні 30 хвилин
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2026, 4, 1, tzinfo=timezone.utc),   # зупиняється 1 квітня
    catchup=False,
    default_args={"owner": "data_engineering", "retries": 1},
) as dag:

    def simulate_new_call(**context):
        # Унікальний call_id на основі часу запуску
        run_time = context["logical_date"]
        call_id  = f"CALL-{run_time.strftime('%Y%m%d%H%M')}"

        status    = random.choice(["completed", "completed", "completed", "missed", "voicemail", "transferred"])
        direction = random.choice(["inbound", "inbound", "outbound"])
        emp_id    = random.randint(1, 50)
        phone     = f"+1-555-{random.randint(1000, 9999)}"
        duration  = 0 if status == "missed" else random.randint(30, 90) if status == "voicemail" else random.randint(90, 900)

        # 1. Вставляємо в MySQL
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        hook.run(
            """
            INSERT IGNORE INTO calls (call_id, employee_id, call_time, phone, direction, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(call_id, emp_id, run_time.strftime("%Y-%m-%d %H:%M:%S"), phone, direction, status),
        )

        # 2. Створюємо JSON файл для цього дзвінка
        os.makedirs(TELEPHONY_DIR, exist_ok=True)
        record = {
            "call_id":           call_id,
            "duration_sec":      duration,
            "short_description": "Call was not answered." if status == "missed"
                                 else "Customer left a voicemail." if status == "voicemail"
                                 else random.choice(DESCRIPTIONS),
        }
        with open(os.path.join(TELEPHONY_DIR, f"{call_id}.json"), "w") as f:
            json.dump(record, f, indent=2)

    t1 = PythonOperator(
        task_id="simulate_new_call",
        python_callable=simulate_new_call
    )
    t1