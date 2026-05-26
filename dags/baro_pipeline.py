from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from plugins.fetch_data import fetch_vehicle_data

with DAG(
    dag_id="baro_pipeline",
    description="배차 수요 분석 및 가중치 산출 파이프라인",
    schedule="0 2 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["baro", "pipeline"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_vehicle_data",
        python_callable=fetch_vehicle_data,
    )
    
    t2 = PythonOperator(
        task_id="train_and_generate_weight",
        python_callable=train_and_generate_weight,
    )

    t1 >> t2