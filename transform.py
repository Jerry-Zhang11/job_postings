import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import json
from psycopg2.extras import execute_values

CREATE_STAGING_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS stg_job_postings (
        id SERIAL PRIMARY KEY,
        raw_id INTEGER,
        title TEXT,
        company TEXT,
        location TEXT,
        date_posted DATE,
        job_description TEXT
    )
"""

INSERT_SQL = """
    INSERT INTO stg_job_postings
    (raw_id, title, company, location, date_posted, job_description)
    VALUES %s
"""

def main():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        cur = conn.cursor()
        cur.execute(CREATE_STAGING_TABLE_SQL)
        conn.commit()

        query = """
            SELECT id, raw_json FROM raw_job_postings
        """

        df = pd.read_sql(query, conn)

        df["raw_id"] = df["id"]
        df["title"] = df["raw_json"].apply(lambda x: x.get("title"))
        df["company"] = df["raw_json"].apply(lambda x: x.get("company", {}).get("display_name"))
        df["location"] = df["raw_json"].apply(lambda x: x.get("location", {}).get("area"))
        df["date_posted"] = df["raw_json"].apply(lambda x: x.get("created"))
        df["job_description"] = df["raw_json"].apply(lambda x: x.get("description"))

        df = df.explode("location")

        df = df.drop(columns=["raw_json", "id"])
        df = df.drop_duplicates(subset=["raw_id"])

        rows = list(df.itertuples(index=False, name=None))

        execute_values(cur, INSERT_SQL, rows, page_size=1000)
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    load_dotenv()
    main()