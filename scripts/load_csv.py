import csv
import json
import os
import psycopg2

CSV_PATH = os.environ.get("CSV_PATH", "/opt/airflow/data/train.csv")
PG_DSN = os.environ.get("PG_DSN", "host=postgres dbname=fraud user=fraud password=fraud")

def main():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("DROP TABLE IF EXISTS raw.train;")
    cur.execute("CREATE TABLE raw.train (payload JSONB);")

    i = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        for i, row in enumerate(reader, 1):
            batch.append((json.dumps(row),))
            if len(batch) >= 5000:
                cur.executemany("INSERT INTO raw.train(payload) VALUES (%s::jsonb)", batch)
                batch.clear()
        if batch:
            cur.executemany("INSERT INTO raw.train(payload) VALUES (%s::jsonb)", batch)

    cur.close()
    conn.close()
    print(f"Loaded {i} rows from {CSV_PATH}")

if __name__ == "__main__":
    main()
