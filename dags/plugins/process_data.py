from airflow.sdk import Variable
from sqlalchemy import create_engine
import pandas as pd

def process_vehicle_data(**context):
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    print("[process_data] 데이터 가공 시작 (DB 내 순수 SQL ELT 연산)")

    # TimescaleDB와 PostgreSQL 함수를 활용한 단일 ELT 쿼리
    elt_query = """
    WITH preprocessed AS (
        SELECT
            s.id AS stand_id,
            EXTRACT(ISODOW FROM d.created_at) - 1 AS day_of_week,
            EXTRACT(HOUR FROM d.created_at) AS hour
        FROM dispatch d
        JOIN dispatch_request dr ON d.request_id = dr.request_id
        -- PostgreSQL 하버사인(Haversine) 공식을 활용한 가장 가까운 승차대 탐색
        CROSS JOIN LATERAL (
            SELECT id
            FROM taxi_stands ts
            ORDER BY (
                6371 * 2 * ASIN(SQRT(
                    POWER(SIN((ts.latitude - dr.start_latitude) * PI() / 180 / 2), 2) +
                    COS(dr.start_latitude * PI() / 180) * COS(ts.latitude * PI() / 180) *
                    POWER(SIN((ts.longitude - dr.start_longitude) * PI() / 180 / 2), 2)
                ))
            )
            LIMIT 1
        ) s
        -- TimescaleDB 청크 배제(Chunk Exclusion) 작동 구간
        WHERE d.created_at >= NOW() - INTERVAL '24 HOURS'
          AND dr.start_latitude BETWEEN 37.4 AND 37.7
          AND dr.start_longitude BETWEEN 126.8 AND 127.2
    )
    SELECT 
        stand_id,
        day_of_week,
        CASE WHEN day_of_week IN (5, 6) THEN 1 ELSE 0 END AS is_weekend,
        CASE 
            WHEN hour BETWEEN 7 AND 9 THEN 0
            WHEN hour BETWEEN 10 AND 16 THEN 1
            WHEN hour BETWEEN 17 AND 20 THEN 2
            WHEN hour BETWEEN 21 AND 23 THEN 3
            ELSE 4 
        END AS time_zone,
        COUNT(*) AS demand
    FROM preprocessed
    GROUP BY stand_id, day_of_week, is_weekend, time_zone;
    """

    print("[process_data] SQL 쿼리 실행 중")
    # DB에서 연산이 끝난 '최종 집계 결과'만 Pandas로 아주 가볍게 불러옵니다.
    demand_df = pd.read_sql(elt_query, engine)

    if demand_df.empty:
        print("[process_data] 가공할 배차 요청 데이터가 없거나 전처리 과정에서 모두 필터링되었습니다.")
        # 빈 데이터프레임이라도 테이블 구조를 덮어쓰기 위해 저장
        demand_df.to_sql('demand_aggregated', engine, if_exists='replace', index=False)
        return

    # 가공된 집계 데이터를 다음 Task(학습)가 사용할 수 있도록 중간 테이블에 저장
    demand_df.to_sql('demand_aggregated', engine, if_exists='replace', index=False)
    print(f"[process_data] 데이터 가공 완료 및 DB 저장: {len(demand_df)}건 (Task 2 종료)")
