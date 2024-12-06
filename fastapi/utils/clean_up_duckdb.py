import duckdb

# Connect to your DuckDB database
con = duckdb.connect('./facebook_insights_pipeline.duckdb')

# Generate DROP TABLE statements dynamically
drop_statements = con.execute("""
    SELECT 
        CASE table_type
            WHEN 'BASE TABLE' THEN 'DROP TABLE ' || table_schema || '.' || table_name || ';'
            WHEN 'VIEW' THEN 'DROP VIEW ' || table_schema || '.' || table_name || ';'
        END AS drop_statement
    FROM 
        information_schema.tables
    WHERE 
        table_name != 'mrt_facebook_insights'
        AND table_schema NOT IN ('pg_catalog', 'information_schema');
""").fetchall()

# Execute each DROP TABLE statement
for statement, in drop_statements:
    print(f"Executing: {statement}")
    con.execute(statement)

# Close the connection
con.close()