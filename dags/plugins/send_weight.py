from airflow.sdk import Variable
import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime

def send_weight_to_public(**context):
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    # 1. DB에서 가장 최신 가중치 가져오기
    weight_df = pd.read_sql("SELECT * FROM stand_weight", engine)

    if weight_df.empty:
        raise ValueError("[send_weight] DB에 저장된 가중치 데이터가 없습니다")

    print(f"[send_weight] 가중치 수신: {len(weight_df)}건")


    # 3. 전송할 데이터 형태로 변환
    weights = weight_df.to_dict(orient='records')

    # 4. relocation-service로 전송
    INTERNAL_ALB_URL = Variable.get("INTERNAL_ALB_URL")
    INTERNAL_API_KEY = Variable.get("INTERNAL_API_KEY")

    try:
        response = requests.post(
            f"{INTERNAL_ALB_URL}/internal/relocation/standWeights",
            headers={"X-Internal-Api-Key": INTERNAL_API_KEY},
            json={"weights": weights},
            timeout=10
        )
        response.raise_for_status()
        print(f"[send_weight] 가중치 전송 성공: {len(weights)}건")

    except requests.exceptions.Timeout:
        raise Exception("[send_weight] 요청 시간 초과")
    
    except requests.exceptions.ConnectionError:
        raise Exception(f"[send_weight] relocation-service 연결 실패: {INTERNAL_ALB_URL}")
    
    except requests.exceptions.HTTPError as e:
        raise Exception(f"[send_weight] HTTP 에러: {e.response.status_code}")