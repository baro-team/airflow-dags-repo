-- 1. TimescaleDB 확장 활성화
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2. 배차 요청(dispatch_request) 테이블 생성
CREATE TABLE IF NOT EXISTS dispatch_request (
    request_id BIGINT,
    user_id BIGINT,
    start_latitude DOUBLE PRECISION,
    start_longitude DOUBLE PRECISION,
    start_location VARCHAR(255),
    start_name VARCHAR(255),
    end_latitude DOUBLE PRECISION,
    end_longitude DOUBLE PRECISION,
    end_location VARCHAR(255),
    end_name VARCHAR(255),
    fare BIGINT,
    estimated_time INTEGER,
    distance_km DOUBLE PRECISION,
    requested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50),
    route_path JSONB
);

-- TimescaleDB 하이퍼테이블 변환 (시간 기준 파티셔닝, 단위: 1일)
SELECT create_hypertable('dispatch_request', 'requested_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);


-- 3. 배차 결과(dispatch) 테이블 생성
CREATE TABLE IF NOT EXISTS dispatch (
    dispatch_id BIGINT,
    request_id BIGINT,
    user_id BIGINT,
    car_id BIGINT,
    car_number VARCHAR(50),
    stand_id VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    estimated_pickup_time INTEGER,
    estimated_ride_time INTEGER,
    fare BIGINT,
    status VARCHAR(50),
    pickup_route_path JSONB,
    dropoff_route_path JSONB
);

-- TimescaleDB 하이퍼테이블 변환
SELECT create_hypertable('dispatch', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);


-- 4. 승차대 테이블 (taxi_stands)
CREATE TABLE IF NOT EXISTS taxi_stands (
    id VARCHAR(50) PRIMARY KEY,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    name VARCHAR(255),
    district VARCHAR(100)
);


-- 5. 가중치 저장 및 히스토리 테이블 (stand_weight, stand_weight_history)
CREATE TABLE IF NOT EXISTS stand_weight (
    stand_id VARCHAR(50),
    time_zone INTEGER,
    day_of_week INTEGER,
    weight DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stand_weight_history (
    stand_id VARCHAR(50),
    time_zone INTEGER,
    day_of_week INTEGER,
    weight DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

SELECT create_hypertable('stand_weight_history', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 6. 중간 집계 테이블 (demand_aggregated)
-- Airflow의 process_data Task가 매번 생성/덮어쓰기 하지만, 구조화 및 초기 세팅 완결성을 위해 명시적으로 생성
CREATE TABLE IF NOT EXISTS demand_aggregated (
    stand_id VARCHAR(50),
    day_of_week INTEGER,
    is_weekend INTEGER,
    time_zone INTEGER,
    demand BIGINT
);
