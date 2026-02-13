import os
from dotenv import load_dotenv
import requests
import psycopg2
import json

load_dotenv()


APP_ID = os.getenv("APP_ID")
APP_KEY = os.getenv("APP_KEY")

BASE_URL = "https://api.adzuna.com/v1/api/jobs/ca/search/{}"

CREATE_RAW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_job_postings (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  source_job_id TEXT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw_json JSONB NOT NULL,
  UNIQUE (source, source_job_id)
);
"""

print(APP_ID)
print(APP_KEY)
print(os.getenv("DB_NAME"))
print(os.getenv("DB_USERNAME"))
print(os.getenv("DB_PASSWORD"))
print(os.getenv("DB_HOST"))
print(os.getenv("DB_PORT"))

def fetch_page(page):

    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": 20,
        "what": "software engineer",
        "content-type": "application/json",
    }

    res = requests.get(BASE_URL.format(page), params=params)
    res.raise_for_status()

    return res.json()


def main(max_pages):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        cur = conn.cursor()
        cur.execute(CREATE_RAW_TABLE_SQL)
        conn.commit()

        for i in range(1, max_pages + 1):
            data = fetch_page(i)
            for job in data.get("results", []):
                cur.execute(
                    """
                    INSERT INTO raw_job_postings (source, source_job_id, raw_json)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (source, source_job_id) DO NOTHING;
                    """,
                    (
                        "adzuna",
                        job.get("id"),
                        json.dumps(job),
                    ),
                )
            conn.commit()

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

main(max_pages=5)