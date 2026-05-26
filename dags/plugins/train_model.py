from airflow.sdk import Variable
import pandas as pd
import numpy as np
from io import StringIO
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sqlalchemy import create_engine
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime


# =============================================
# 유틸 함수
# =============================================

def haversine(lat1, lng1, lat2, lng2):
    """두 좌표 간 거리 계산 (km)"""
    R = 6371
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = (sin(dlat / 2) ** 2
         + cos(radians(lat1))
         * cos(radians(lat2))
         * sin(dlng / 2) ** 2)
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def find_nearest_stand(lat, lng, stands_df):
    """가장 가까운 승차대 찾기"""
    min_dist = float('inf')
    nearest_id = None

    for _, stand in stands_df.iterrows():
        dist = haversine(
            lat, lng,
            stand['latitude'],
            stand['longitude']
        )
        if dist < min_dist:
            min_dist = dist
            nearest_id = stand['id']

    return nearest_id


def get_time_zone(hour):
    """시간대 구분
    0: 출근 (07~09시)
    1: 낮   (10~16시)
    2: 퇴근 (17~20시)
    3: 저녁 (21~23시)
    4: 심야 (00~06시)
    """
    if 7  <= hour <= 9:  return 0
    if 10 <= hour <= 16: return 1
    if 17 <= hour <= 20: return 2
    if 21 <= hour <= 23: return 3
    return 4


# =============================================
# 전처리 함수
# =============================================

def preprocess(dispatch_df, stands_df):
    """
    1. 결측값 처리
    2. 이상치 처리
    3. 승차대 매핑
    4. 시간대 변환
    5. 수요 집계
    """

    # 1. 결측값 처리
    before = len(dispatch_df)
    dispatch_df = dispatch_df.dropna(
        subset=['start_latitude', 'start_longitude']
    )
    after = len(dispatch_df)
    if before != after:
        print(f"[전처리] 결측값 제거: {before - after}건")

    # 2. 위경도 범위 이상치 제거 (서울 범위)
    before = len(dispatch_df)
    dispatch_df = dispatch_df[
        dispatch_df['start_latitude'].between(37.4, 37.7) &
        dispatch_df['start_longitude'].between(126.8, 127.2)
    ]
    after = len(dispatch_df)
    if before != after:
        print(f"[전처리] 위경도 이상치 제거: {before - after}건")

    # 3. 가장 가까운 승차대 매핑
    dispatch_df['stand_id'] = dispatch_df.apply(
        lambda row: find_nearest_stand(
            row['start_latitude'],
            row['start_longitude'],
            stands_df
        ), axis=1
    )
    print("[전처리] 승차대 매핑 완료")

    # 4. 시간대 변환 (hour → time_zone)
    dispatch_df['time_zone'] = dispatch_df['hour'].apply(get_time_zone)

    # 5. 승차대별 시간대별 수요 집계
    demand_df = dispatch_df.groupby(
        ['stand_id', 'time_zone', 'day_of_week', 'is_weekend']
    ).size().reset_index(name='demand')

    # 6. 수요 이상치 제거 (IQR)
    Q1  = demand_df['demand'].quantile(0.25)
    Q3  = demand_df['demand'].quantile(0.75)
    IQR = Q3 - Q1
    before = len(demand_df)
    demand_df = demand_df[
        demand_df['demand'].between(
            Q1 - 1.5 * IQR,
            Q3 + 1.5 * IQR
        )
    ]
    after = len(demand_df)
    if before != after:
        print(f"[전처리] 수요 이상치 제거: {before - after}건")

    print(f"[전처리] 완료: {len(demand_df)}건")
    return demand_df


# =============================================
# 메인 Task 함수
# =============================================

def train_and_generate_weight(**context):
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    # 1. fetch_data에서 데이터 받기
    dispatch_json = context['ti'].xcom_pull(
        task_ids='fetch_vehicle_data',
        key='dispatch_data'
    )
    stands_json = context['ti'].xcom_pull(
        task_ids='fetch_vehicle_data',
        key='stands_data'
    )

    if not dispatch_json or not stands_json:
        raise ValueError(
            "[train_model] fetch_data에서 데이터를 받지 못했습니다"
        )

    dispatch_df = pd.read_json(StringIO(dispatch_json))
    stands_df   = pd.read_json(StringIO(stands_json))

    print(f"[train_model] 배차 요청 수신: {len(dispatch_df)}건")
    print(f"[train_model] 승차대 수신: {len(stands_df)}건")

    # 2. 전처리
    demand_df = preprocess(dispatch_df, stands_df)

    # 3. feature / target 분리
    # stand_id 인코딩 (XGBoost는 문자열 못 받음)
    demand_df['stand_id_encoded'] = pd.factorize(
        demand_df['stand_id']
    )[0]

    feature_cols = [
        'time_zone',
        'day_of_week',
        'is_weekend',
        'stand_id_encoded'
    ]

    X = demand_df[feature_cols]
    y = demand_df['demand']

    # 4. 데이터 분리 (시계열 분리)
    # 데이터가 적을 때는 전체로 학습
    # 데이터가 충분할 때는 시계열 분리 사용
    if len(demand_df) < 50:
        print(f"[train_model] 데이터 부족({len(demand_df)}건) → 전체 학습")
        X_train, y_train = X, y
        X_test,  y_test  = X, y  # 평가용으로만 사용
        is_data_sufficient = False
    else:
        train_size = int(len(demand_df) * 0.8)
        X_train = X.iloc[:train_size]
        y_train = y.iloc[:train_size]
        X_test  = X.iloc[train_size:]
        y_test  = y.iloc[train_size:]
        is_data_sufficient = True
        print(f"[train_model] 시계열 분리 → 학습: {len(X_train)}건, 검증: {len(X_test)}건")

    # 5. XGBoost 학습
    # 데이터 적을 때는 단순한 모델 사용
    model = XGBRegressor(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        random_state=42
    )
    model.fit(X_train, y_train)
    print("[train_model] 모델 학습 완료")

    # 6. 모델 평가
    y_pred = model.predict(X_test)
    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2   = r2_score(y_test, y_pred)

    print(f"[train_model] MAE:  {mae:.4f}")
    print(f"[train_model] RMSE: {rmse:.4f}")
    print(f"[train_model] R2:   {r2:.4f}")

    if is_data_sufficient and r2 < 0:
        print("[train_model] WARNING: R2 음수 → 모델 성능 낮음, 데이터 확인 필요")

    # 7. Feature Importance
    importance = dict(zip(feature_cols, model.feature_importances_))
    print(f"[train_model] Feature Importance: {importance}")

    # 8. 가중치 예측 및 정규화 (0~1)
    demand_df['weight'] = model.predict(X)

    min_w = demand_df['weight'].min()
    max_w = demand_df['weight'].max()

    if max_w == min_w:
        demand_df['weight'] = 1.0
    else:
        demand_df['weight'] = (
            (demand_df['weight'] - min_w) / (max_w - min_w)
        )

    # 9. region_weight 테이블 저장
    weight_df = demand_df[[
        'stand_id', 'time_zone', 'day_of_week', 'weight'
    ]].copy()
    weight_df.rename(columns={'stand_id': 'region_id'}, inplace=True)
    weight_df['updated_at'] = datetime.now()

    weight_df.to_sql(
        'region_weight',
        engine,
        if_exists='replace',
        index=False
    )
    print(f"[train_model] 가중치 저장 완료: {len(weight_df)}건")

    # 10. 다음 Task로 전달
    context['ti'].xcom_push(
        key='weight_data',
        value=weight_df.drop(columns=['updated_at']).to_json()
    )

    print("[train_model] XCom push 완료")