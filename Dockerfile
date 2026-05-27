FROM apache/airflow:3.2.0

RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    requests \
    scikit-learn

RUN pip install --no-cache-dir --no-deps \
    xgboost