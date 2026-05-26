from airflow.sdk import Variable
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta


def fetch_vehicle_data(**context):
    # Airflow Variable에서 DB_URL 가져오기
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    # 어제 날짜 기준
    yesterday = datetime.now() - timedelta(days=1)
    start = yesterday.replace(hour=0,  minute=0,  second=0,  microsecond=0)
    end   = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

    # 1. 어제 배차 요청 데이터 조회
    dispatch_df = pd.read_sql(f"""
        SELECT
            request_id,
            start_latitude,
            start_longitude,
            requested_at,
            EXTRACT(hour FROM requested_at) AS hour,
            EXTRACT(dow  FROM requested_at) AS day_of_week,
            CASE
                WHEN EXTRACT(dow FROM requested_at) IN (0, 6)
                THEN 1 ELSE 0
            END AS is_weekend
        FROM dispatch_request
        WHERE requested_at BETWEEN '{start}' AND '{end}'
          AND status = 'completed'
    """, engine)

    if dispatch_df.empty:
        raise ValueError(
            f"[fetch_data] 어제({yesterday.date()}) 배차 요청 데이터가 없습니다"
        )

    print(f"[fetch_data] 배차 요청 조회 완료: {len(dispatch_df)}건")

    # 2. 승차대 데이터 조회
    stands_df = pd.read_sql("""
        SELECT
            id,
            latitude,
            longitude,
            name,
            district
        FROM taxi_stands
    """, engine)

    print(f"[fetch_data] 승차대 조회 완료: {len(stands_df)}건")

    # 3. 다음 Task로 전달
    context['ti'].xcom_push(
        key='dispatch_data',
        value=dispatch_df.to_json()
    )
    context['ti'].xcom_push(
        key='stands_data',
        value=stands_df.to_json()
    )

    print("[fetch_data] XCom push 완료")