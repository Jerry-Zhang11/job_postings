import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import json
from psycopg2.extras import execute_values

KEYWORDS = [
    # languages
    "python","java","c++","c#","javascript","typescript","sql","scala",

    # backend
    "spring","spring boot","django","flask","fastapi","node","express","asp.net",

    # data
    "pandas","numpy","spark","hadoop","airflow","dbt","etl","elt","data warehouse",

    # cloud/devops
    "aws","azure","gcp","docker","kubernetes","terraform","ci/cd","jenkins","github actions",

    # databases
    "postgres","mysql","mongodb","redis","snowflake","bigquery",

    # ai/ml
    "machine learning","deep learning","tensorflow","pytorch","nlp","llm","openai"
]

CREATE_TOP_KEYWORDS_SQL = """
    CREATE TABLE IF NOT EXISTS top_keywords (
        id SERIAL PRIMARY KEY,
        keyword VARCHAR(255) NOT NULL UNIQUE,
        keyword_count INT NOT NULL
    )
"""

INSERT_SQL = """
    INSERT INTO top_keywords
    (keyword, keyword_count)
    VALUES %s
    ON CONFLICT (keyword) DO UPDATE SET keyword_count = EXCLUDED.keyword_count
"""

DIM_KEYWORD_INSERT_SQL = """
    INSERT INTO dim_keywords
    (keyword)
    VALUES %s
    ON CONFLICT DO NOTHING
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
        cur.execute(CREATE_TOP_KEYWORDS_SQL)
        conn.commit()

        # sql parameter must be in a tuple
        cur.execute("CREATE TABLE IF NOT EXISTS dim_keywords (keyword TEXT PRIMARY KEY);")
        execute_values(cur, DIM_KEYWORD_INSERT_SQL, [(kw,) for kw in KEYWORDS], page_size=1000)
        conn.commit()

        query = """
            SELECT keyword, COUNT(*) as keyword_count
            FROM dim_keywords k
            JOIN stg_job_postings p ON p.job_description ILIKE '%' || k.keyword || '%'
            GROUP BY k.keyword
            ORDER BY keyword_count DESC
            LIMIT 10
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