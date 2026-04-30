from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3, io, os, pandas as pd

BUCKET_NAME = "baro-data-s3"
LOCAL_DIR   = "/opt/airflow/downloaded"

def pull_from_s3():
    conn = BaseHook.get_connection("aws_default")
    s3 = boto3.client(
        "s3",
        region_name="ap-northeast-2",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    os.makedirs(LOCAL_DIR, exist_ok=True)
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=10)
    if "Contents" in response:
        for obj in response["Contents"]:
            print(f"[발견] {obj[chr(75)+"ey"]} (size: {obj[chr(83)+"ize"]} bytes)")
    else:
        print("[결과] 버킷이 비어있습니다")

with DAG(
    dag_id="baro_s3_pull",
    description="Git Sync 반영 테스트용 DAG",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["baro", "s3", "test"],
) as dag:
    PythonOperator(
        task_id="pull_s3_data",
        python_callable=pull_from_s3,
    )
