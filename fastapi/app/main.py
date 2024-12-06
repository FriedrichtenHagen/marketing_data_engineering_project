from fastapi import FastAPI, HTTPException
import duckdb

app = FastAPI()

DATABASE_FILE = "facebook_insights_pipeline.duckdb"

def get_db_connection():
    conn = duckdb.connect(DATABASE_FILE, read_only=True)
    return conn

@app.get("/data/{table_name}")
async def get_table_data(table_name: str):
    try:
        conn = get_db_connection()
        query = f"SELECT * FROM dbt_transformations_dbt_transformations.{table_name} LIMIT 100"
        result = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.description]
        return {"columns": columns, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))