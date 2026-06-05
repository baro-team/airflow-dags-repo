from airflow.sdk import Variable
import pandas as pd
import requests
from io import StringIO


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

    # 2. 전송할 데이터 형태로 변환
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