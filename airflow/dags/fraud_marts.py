from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {"owner": "student"}

with DAG(
    dag_id="fraud_marts",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dz5", "fraud"],
) as dag:

    # 0) Load CSV -> raw.train(payload jsonb)
    load_raw = BashOperator(
        task_id="load_raw_train_csv",
        bash_command="python /opt/airflow/scripts/load_csv.py",
        env={
            "CSV_PATH": "/opt/airflow/data/train.csv",
            "PG_DSN": "host=postgres dbname=fraud user=fraud password=fraud",
        },
    )

    # 1) Build base view from json payload
    create_base = PostgresOperator(
        task_id="create_base_view",
        postgres_conn_id="fraud_pg",
        sql="""
        CREATE SCHEMA IF NOT EXISTS base;

        CREATE OR REPLACE VIEW base.tx AS
        SELECT
          (payload->>'transaction_time')::timestamp                AS tx_ts,
          ((payload->>'transaction_time')::timestamp)::date       AS tx_date,
          payload->>'us_state'                                     AS state,
          payload->>'one_city'                                     AS city,
          payload->>'cat_id'                                       AS category_id,
          payload->>'merch'                                        AS merchant_id,
          (payload->>'amount')::numeric                            AS amount,
          (payload->>'target')::int                                AS is_fraud,

          -- surrogate customer key (since no customer_id in csv)
          concat_ws('|',
            payload->>'name_1',
            payload->>'name_2',
            payload->>'street',
            payload->>'post_code'
          )                                                        AS customer_key,

          EXTRACT(DOW  FROM (payload->>'transaction_time')::timestamp) AS dow,
          EXTRACT(HOUR FROM (payload->>'transaction_time')::timestamp) AS hour
        FROM raw.train;
        """,
    )

    # 2) mart_daily_state_metrics
    mart_daily_state_metrics = PostgresOperator(
        task_id="mart_daily_state_metrics",
        postgres_conn_id="fraud_pg",
        sql="""
        CREATE SCHEMA IF NOT EXISTS marts;
        DROP TABLE IF EXISTS marts.mart_daily_state_metrics;

        CREATE TABLE marts.mart_daily_state_metrics AS
        SELECT
          tx_date,
          state,
          COUNT(*) AS tx_cnt,
          SUM(amount) AS total_amount,
          AVG(amount) AS avg_check,
          PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) AS p95_amount,
          AVG(CASE WHEN amount >= 1000 THEN 1 ELSE 0 END)::numeric AS share_large_tx
        FROM base.tx
        GROUP BY 1,2;
        """,
    )

    # 3) mart_fraud_by_category
    mart_fraud_by_category = PostgresOperator(
        task_id="mart_fraud_by_category",
        postgres_conn_id="fraud_pg",
        sql="""
        DROP TABLE IF EXISTS marts.mart_fraud_by_category;

        CREATE TABLE marts.mart_fraud_by_category AS
        SELECT
          category_id,
          COUNT(*) AS tx_cnt,
          SUM(is_fraud) AS fraud_cnt,
          (SUM(is_fraud)::numeric / NULLIF(COUNT(*),0)) * 100 AS fraud_rate_pct,
          SUM(amount) AS total_amount,
          SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END) AS fraud_amount
        FROM base.tx
        GROUP BY 1
        ORDER BY fraud_rate_pct DESC;
        """,
    )

    # 4) mart_fraud_by_state
    mart_fraud_by_state = PostgresOperator(
        task_id="mart_fraud_by_state",
        postgres_conn_id="fraud_pg",
        sql="""
        DROP TABLE IF EXISTS marts.mart_fraud_by_state;

        CREATE TABLE marts.mart_fraud_by_state AS
        SELECT
          state,
          COUNT(*) AS tx_cnt,
          SUM(is_fraud) AS fraud_cnt,
          (SUM(is_fraud)::numeric / NULLIF(COUNT(*),0)) * 100 AS fraud_rate_pct,
          COUNT(DISTINCT customer_key) AS uniq_customers,
          COUNT(DISTINCT merchant_id) AS uniq_merchants,
          SUM(amount) AS total_amount,
          SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END) AS fraud_amount
        FROM base.tx
        GROUP BY 1
        ORDER BY fraud_rate_pct DESC;
        """,
    )

    # 5) mart_customer_risk_profile
    mart_customer_risk_profile = PostgresOperator(
        task_id="mart_customer_risk_profile",
        postgres_conn_id="fraud_pg",
        sql="""
        DROP TABLE IF EXISTS marts.mart_customer_risk_profile;

        CREATE TABLE marts.mart_customer_risk_profile AS
        WITH c AS (
          SELECT
            customer_key,
            COUNT(*) AS tx_cnt,
            SUM(is_fraud) AS fraud_cnt,
            (SUM(is_fraud)::numeric / NULLIF(COUNT(*),0)) AS fraud_rate,
            AVG(amount) AS avg_check
          FROM base.tx
          GROUP BY 1
        )
        SELECT
          customer_key,
          tx_cnt,
          fraud_cnt,
          fraud_rate,
          avg_check,
          CASE
            WHEN fraud_rate >= 0.10 THEN 'HIGH'
            WHEN fraud_rate >= 0.03 THEN 'MEDIUM'
            ELSE 'LOW'
          END AS risk_segment
        FROM c;
        """,
    )

    # 6) mart_hourly_fraud_pattern
    mart_hourly_fraud_pattern = PostgresOperator(
        task_id="mart_hourly_fraud_pattern",
        postgres_conn_id="fraud_pg",
        sql="""
        DROP TABLE IF EXISTS marts.mart_hourly_fraud_pattern;

        CREATE TABLE marts.mart_hourly_fraud_pattern AS
        SELECT
          dow,
          hour,
          COUNT(*) AS tx_cnt,
          SUM(is_fraud) AS fraud_cnt,
          (SUM(is_fraud)::numeric / NULLIF(COUNT(*),0)) * 100 AS fraud_rate_pct,
          SUM(amount) AS total_amount
        FROM base.tx
        GROUP BY 1,2
        ORDER BY fraud_rate_pct DESC;
        """,
    )

    # 7) mart_merchant_analytics
    mart_merchant_analytics = PostgresOperator(
        task_id="mart_merchant_analytics",
        postgres_conn_id="fraud_pg",
        sql="""
        DROP TABLE IF EXISTS marts.mart_merchant_analytics;

        CREATE TABLE marts.mart_merchant_analytics AS
        WITH m AS (
          SELECT
            merchant_id,
            COUNT(*) AS tx_cnt,
            SUM(is_fraud) AS fraud_cnt,
            (SUM(is_fraud)::numeric / NULLIF(COUNT(*),0)) AS fraud_rate,
            SUM(amount) AS turnover
          FROM base.tx
          GROUP BY 1
        )
        SELECT
          merchant_id,
          tx_cnt,
          fraud_cnt,
          fraud_rate,
          turnover,
          CASE
            WHEN (tx_cnt >= 50 AND fraud_rate >= 0.07) OR (turnover >= 50000 AND fraud_rate >= 0.05)
              THEN 1 ELSE 0
          END AS is_suspicious
        FROM m;
        """,
    )

    load_raw >> create_base >> [
        mart_daily_state_metrics,
        mart_fraud_by_category,
        mart_fraud_by_state,
        mart_customer_risk_profile,
        mart_hourly_fraud_pattern,
        mart_merchant_analytics,
    ]
