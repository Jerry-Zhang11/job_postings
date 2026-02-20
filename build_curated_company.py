import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import json
from psycopg2.extras import execute_values

CREATE_TOP_COMPANIES_SQL = """
    CREATE TABLE IF NOT EXISTS top_companies (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        job_count INT NOT NULL
    )
"""

INSERT_SQL = """
    INSERT INTO top_companies
    (name, job_count)
    VALUES %s
    ON CONFLICT (name) DO UPDATE SET job_count = EXCLUDED.job_count
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
        cur.execute(CREATE_TOP_COMPANIES_SQL)
        conn.commit()

        query = """
            SELECT company as name, count(*) as job_count
            FROM stg_job_postings
            GROUP BY company
            ORDER BY job_count DESC
            Limit 5
        """

        df = pd.read_sql(query, conn)

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