from airflow.sdk import Variable
import pandas as pd
import requests
from io import StringIO
from sqlalchemy import create_engine
from datetime import datetime


def send_weight_to_public(**context):

    # 1. train_model에서 가중치 받기
    weight_json = context['ti'].xcom_pull(
        task_ids='train_and_generate_weight',
        key='weight_data'
    )

    if not weight_json:
        raise ValueError(
            "[send_weight] train_model에서 가중치를 받지 못했습니다"
        )

    weight_df = pd.read_json(StringIO(weight_json))
    print(f"[send_weight] 가중치 수신: {len(weight_df)}건")

    # 2. Private DB에 가중치 누적 저장
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)
    
    history_df = weight_df.copy()
    history_df['created_at'] = datetime.now()
    
    history_df.to_sql(
        'stand_weight_history',
        engine,
        if_exists='append',
        index=False
    )
    print(f"[send_weight] Private DB 누적 저장 완료: {len(history_df)}건")

    # 3. 전송할 데이터 형태로 변환
    weights = weight_df.to_dict(orient='records')

    # 3. relocation-service로 전송
    RELOCATION_URL = Variable.get("RELOCATION_URL")

    try:
        response = requests.post(
            RELOCATION_URL,
            json={"weights": weights},
            timeout=10
        )
        response.raise_for_status()
        print(f"[send_weight] 가중치 전송 성공: {len(weights)}건")

    except requests.exceptions.Timeout:
        raise Exception("[send_weight] 요청 시간 초과")

    except requests.exceptions.ConnectionError:
        raise Exception(
            f"[send_weight] relocation-service 연결 실패: {RELOCATION_URL}"
        )

    except requests.exceptions.HTTPError as e:
        raise Exception(
            f"[send_weight] HTTP 에러: {e.response.status_code}"
        )