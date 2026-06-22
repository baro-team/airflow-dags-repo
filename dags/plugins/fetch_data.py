from airflow.sdk import Variable
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta


def fetch_vehicle_data(**context):
    # Airflow Variable에서 DB_URL 가져오기
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    INTERNAL_ALB_URL = Variable.get("INTERNAL_ALB_URL")
    INTERNAL_API_KEY = Variable.get("INTERNAL_API_KEY")

    print("[fetch_data] 배차 데이터 API 호출 시작")
    import requests
    import gzip
    from io import BytesIO

    response = requests.get(
        f"{INTERNAL_ALB_URL}/internal/dispatch/export/daily",
        headers={"X-Internal-Api-Key": INTERNAL_API_KEY},
        timeout=60
    )
    response.raise_for_status()

    with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
        dispatch_df = pd.read_csv(f)

    if dispatch_df.empty:
        raise ValueError("[fetch_data] 최근 24시간 배차 데이터가 없습니다")

    # 원본 데이터 Private DB에 저장
    dispatch_df['requested_at'] = pd.to_datetime(dispatch_df['requested_at'])
    
    columns_to_save = [
        'requested_at', 'request_id', 'user_id', 
        'start_latitude', 'start_longitude', 
        'end_latitude', 'end_longitude', 'status'
    ]

    dispatch_df[columns_to_save].to_sql(
        'dispatch_request',
        engine,
        if_exists='append',
        index=False
    )
    print(f"[fetch_data] 배차 요청 원본 데이터 DB 누적 저장 완료: {len(dispatch_df)}건")

    # 파생 변수 생성
    dispatch_df['hour'] = dispatch_df['requested_at'].dt.hour
    dispatch_df['day_of_week'] = dispatch_df['requested_at'].dt.dayofweek
    dispatch_df['is_weekend'] = dispatch_df['day_of_week'].isin([5, 6]).astype(int)

    print(f"[fetch_data] 배차 요청 조회/처리 완료: {len(dispatch_df)}건")

    # 승차대 데이터 조회
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

    # 다음 Task로 전달
    context['ti'].xcom_push(
        key='dispatch_data',
        value=dispatch_df.to_json()
    )
    context['ti'].xcom_push(
        key='stands_data',
        value=stands_df.to_json()
    )

    print("[fetch_data] XCom push 완료")