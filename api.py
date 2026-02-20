from flask import Flask, jsonify, request
import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import json
from psycopg2.extras import execute_values

app = Flask(__name__)
load_dotenv()

def query(sql, param):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        
        cur = conn.cursor()
        cur.execute(sql, param or ())

        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        print(columns)
        print(rows)
        return [dict(zip(columns, row)) for row in rows]
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

@app.get("/metrics/top-keywords")
def top_keywords():
    limit = int(request.args.get("limit", 10))
    sql = """
        SELECT id, keyword, keyword_count
        FROM top_keywords
        ORDER BY keyword_count DESC
        LIMIT %s
    """
    return jsonify(query(sql, (limit,)))

@app.get("/metrics/top-companies")
def top_locations():
    limit = int(request.args.get("limit", 10))
    sql = """
        SELECT id, name, job_count
        FROM top_companies
        ORDER BY job_count DESC
        LIMIT %s;
    """
    return jsonify(query(sql, (limit,)))

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == '__main__':
    app.run(debug=True, port=8000)