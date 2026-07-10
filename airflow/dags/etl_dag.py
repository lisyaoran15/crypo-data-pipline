from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging

sys.path.append('/opt/airflow/my_prj')

from codes.deploy_airflow import task_backfill, task_etl_full, task_etl_incremental

log = logging.getLogger(__name__)

# CONFIG
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
RAW_PATH = "/opt/airflow/my_prj/data/raw"
FEATURE_ETL_PATH = "/opt/airflow/my_prj/data/interim/test_feature_etl_1"
WINDOWS = [5, 10, 20, 50, 200]
MOMENTUM_WINDOWS = [20, 50, 100, 200]

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# DAG 1: Data Pipeline
with DAG(dag_id="data_pipeline",
         default_args=default_args,
         start_date=datetime(2026, 1, 1),
         schedule="0 0 * * *",
         catchup=False,) as dag1:

    backfill = PythonOperator(
        task_id="backfill_raw",
        python_callable=task_backfill,
        op_kwargs={
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "raw_path": RAW_PATH,
        }
    )

# DAG 2: Feature Pipeline
with DAG(dag_id="feature_pipeline",
         default_args=default_args,
         start_date=datetime(2026, 1, 1),
         schedule="5 0 * * *",
         catchup=False,) as dag2:

    etl_full = PythonOperator(
        task_id="etl_feature_backfill",
        python_callable=task_etl_full,
        op_kwargs={
            "raw_path": RAW_PATH,
            "feature_path": FEATURE_ETL_PATH,
            "symbol": SYMBOL,
            "windows": WINDOWS,
            "momentum_windows": MOMENTUM_WINDOWS,
        }
    )
    etl_incremental = PythonOperator(
        task_id="etl_incremental",
        python_callable=task_etl_incremental,
        op_kwargs={
            "raw_path": RAW_PATH,
            "feature_path": FEATURE_ETL_PATH,
            "symbol": SYMBOL,
            "windows": WINDOWS,
            "momentum_windows": MOMENTUM_WINDOWS,
        }
    )

    etl_full >> etl_incremental