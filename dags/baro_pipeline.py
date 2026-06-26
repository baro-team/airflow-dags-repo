from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from plugins.fetch_requests import fetch_requests_data
from plugins.fetch_dispatches import fetch_dispatches_data
from plugins.process_data import process_vehicle_data
from plugins.train_model import train_and_generate_weight
from plugins.send_weight import send_weight_to_public

# 한국 시간(KST) 설정
local_tz = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="baro_pipeline",
    description="배차 수요 분석 및 가중치 산출 파이프라인 (ELT + TimescaleDB)",
    schedule="0 2 * * *",  # 한국 시간(Asia/Seoul) 기준 새벽 2시
    start_date=datetime(2026, 5, 1, tzinfo=local_tz),
    catchup=False,
    tags=["baro", "pipeline"],
) as dag:

    # Task 1-1: 배차 요청 데이터 수집
    t1_fetch_requests = PythonOperator(
        task_id="fetch_requests_data",
        python_callable=fetch_requests_data,
    )

    # Task 1-2: 배차 데이터 수집
    t1_fetch_dispatches = PythonOperator(
        task_id="fetch_dispatches_data",
        python_callable=fetch_dispatches_data,
    )

    # Task 2: 학습 가능한 형태로 데이터 가공
    t2_process = PythonOperator(
        task_id="process_vehicle_data",
        python_callable=process_vehicle_data,
    )

    # Task 3: 데이터 학습시키기
    t3_train = PythonOperator(
        task_id="train_and_generate_weight",
        python_callable=train_and_generate_weight,
    )

    # Task 4: 가중치 데이터 전송
    t4_send = PythonOperator(
        task_id="send_weight_to_public",
        python_callable=send_weight_to_public,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # 병렬로 수집 -> 가공 -> 학습 -> 전송
    [t1_fetch_requests, t1_fetch_dispatches] >> t2_process >> t3_train >> t4_send