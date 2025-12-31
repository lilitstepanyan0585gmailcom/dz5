# Airflow Fraud Marts Pipeline

Учебный проект с использованием **Apache Airflow** для построения ETL-пайплайна и витрин данных (marts) по транзакциям с признаками фрода.

Проект демонстрирует:
- оркестрацию задач в Airflow
- загрузку сырых данных в Postgres
- построение витрин (marts) с помощью SQL
- работу с зависимостями между тасками

---

## Стек технологий

- Apache Airflow
- Docker & Docker Compose
- PostgreSQL
- Python
- SQL

---

## Структура проекта

├── airflow/
│ └── dags/
│ └── fraud_marts.py # Основной DAG
├── scripts/
│ └── load_csv.py # Загрузка CSV в Postgres
├── sql/
│ └── 01_create_raw.sql # SQL для создания raw-таблиц и витрин
├── docker-compose.yml # Инфраструктура Airflow + Postgres
├── requirements.txt # Python-зависимости
├── README.md
└── .gitignore


> Датасеты намеренно **не включены в репозиторий** и добавлены в `.gitignore`.

---

## Описание DAG

DAG: **`fraud_marts`**

Состоит из следующих шагов:

1. `load_raw_train_csv`  
   Загрузка исходных CSV-данных в raw-таблицы PostgreSQL

2. `create_base_view`  
   Создание базового представления для дальнейших витрин

3. `mart_daily_state_metrics`  
   Агрегация транзакций по дням и штатам

4. `mart_fraud_by_category`  
   Витрина с фродом по категориям

5. `mart_fraud_by_state`  
   Витрина с фродом по штатам

6. `mart_customer_risk_profile`  
   Профили риска клиентов

7. `mart_hourly_fraud_pattern`  
   Почасовые паттерны фрода

8. `mart_merchant_analytics`  
   Аналитика по мерчантам

Все downstream-задачи зависят от успешного выполнения загрузки данных.

---

## Запуск проекта

### 1. Запуск инфраструктуры

```bash
docker compose up -d

2. Доступ к Airflow

URL: http://localhost:8080

Логин: admin

Пароль: admin

3. Запуск DAG

В интерфейсе Airflow:

включить DAG fraud_marts

нажать Trigger DAG

Примечания

Первая таска (load_raw_train_csv) может выполняться длительное время из-за объёма данных и ограничений Docker-окружения.

Остальные таски запускаются после завершения upstream-задач.




