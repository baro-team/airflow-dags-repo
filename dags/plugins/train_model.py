from airflow.sdk import Variable
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sqlalchemy import create_engine
from datetime import datetime

def train_and_generate_weight(**context):
    DB_URL = Variable.get("DB_URL")
    engine = create_engine(DB_URL)

    print("[train_model] DB에서 가공된 데이터 조회 시작")

    # 1. DB에서 가공 완료된 demand_aggregated 조회
    demand_df = pd.read_sql("SELECT * FROM demand_aggregated", engine)
    
    # 2. DB에서 승차대 정보 조회 (결과 저장 시 조인용)
    stands_df = pd.read_sql("SELECT id, latitude, longitude FROM taxi_stands", engine)

    if demand_df.empty:
        print("[train_model] 학습할 데이터가 없습니다 (demand_aggregated 비어있음)")
        return

    print(f"[train_model] 집계 데이터 수신 완료: {len(demand_df)}건")

    # 3. feature / target 분리
    # stand_id 인코딩
    demand_df['stand_id_encoded'] = pd.factorize(demand_df['stand_id'])[0]

    feature_cols = [
        'time_zone',
        'day_of_week',
        'is_weekend',
        'stand_id_encoded'
    ]

    X = demand_df[feature_cols]
    y = demand_df['demand']

    # 4. 데이터 분리 (시계열 분리)
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

    # 9. stand_weight 테이블 저장
    demand_df = demand_df.merge(
        stands_df, 
        left_on='stand_id', 
        right_on='id', 
        how='left'
    )
    weight_df = demand_df[[
        'stand_id', 'time_zone', 'day_of_week', 'weight', 'latitude', 'longitude'
    ]].copy()
    weight_df['updated_at'] = datetime.now()

    weight_df.to_sql(
        'stand_weight',
        engine,
        if_exists='replace',
        index=False
    )

    # 히스토리 보존을 위해 stand_weight_history 테이블에 누적 저장
    history_df = weight_df.copy()
    history_df.rename(columns={'updated_at': 'created_at'}, inplace=True)
    history_df.to_sql(
        'stand_weight_history',
        engine,
        if_exists='append',
        index=False
    )

    print(f"[train_model] 가중치 및 히스토리 저장 완료: {len(weight_df)}건")

    # 10. 다음 Task로 전달
    context['ti'].xcom_push(
        key='weight_data',
        value=weight_df.drop(columns=['updated_at']).to_json()
    )

    print("[train_model] XCom push 완료")