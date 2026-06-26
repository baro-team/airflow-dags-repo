from airflow.sdk import Variable
import pandas as pd
from sqlalchemy import create_engine
import requests
import gzip
from io import BytesIO

def fetch_dispatches_data(**context):
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    INTERNAL_ALB_URL = Variable.get("INTERNAL_ALB_URL")
    INTERNAL_API_KEY = Variable.get("INTERNAL_API_KEY")
    headers = {"X-Internal-Api-Key": INTERNAL_API_KEY}

    print("[fetch_dispatches] 배차 결과 데이터 API 호출 시작")

    disp_resp = requests.get(
        f"{INTERNAL_ALB_URL}/internal/dispatch/export/daily/dispatches",
        headers=headers, timeout=60
    )
    disp_resp.raise_for_status()
    
    with gzip.GzipFile(fileobj=BytesIO(disp_resp.content)) as f:
        disp_df = pd.read_csv(f)
    
    if not disp_df.empty:
        if 'created_at' in disp_df.columns:
            disp_df['created_at'] = pd.to_datetime(disp_df['created_at'])
        disp_df.to_sql('dispatch', engine, if_exists='append', index=False)
        print(f"[fetch_dispatches] 배차 결과 데이터 적재 완료: {len(disp_df)}건")
    else:
        print("[fetch_dispatches] 최근 24시간 배차 결과 데이터가 없습니다.")

    print("[fetch_dispatches] Task 완료")
