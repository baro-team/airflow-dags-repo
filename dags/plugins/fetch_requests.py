from airflow.sdk import Variable
import pandas as pd
from sqlalchemy import create_engine
import requests
import gzip
from io import BytesIO

def fetch_requests_data(**context):
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    INTERNAL_ALB_URL = Variable.get("INTERNAL_ALB_URL")
    INTERNAL_API_KEY = Variable.get("INTERNAL_API_KEY")
    headers = {"X-Internal-Api-Key": INTERNAL_API_KEY}

    print("[fetch_requests] 배차 요청 데이터 API 호출 시작")

    req_resp = requests.get(
        f"{INTERNAL_ALB_URL}/internal/dispatch/export/daily/requests",
        headers=headers, timeout=60
    )
    req_resp.raise_for_status()
    
    with gzip.GzipFile(fileobj=BytesIO(req_resp.content)) as f:
        req_df = pd.read_csv(f)
    
    if not req_df.empty:
        if 'requested_at' in req_df.columns:
            req_df['requested_at'] = pd.to_datetime(req_df['requested_at'])
        req_df.to_sql('dispatch_request', engine, if_exists='append', index=False)
        print(f"[fetch_requests] 배차 요청 데이터 적재 완료: {len(req_df)}건")
    else:
        print("[fetch_requests] 최근 24시간 배차 요청 데이터가 없습니다.")

    print("[fetch_requests] Task 완료")
