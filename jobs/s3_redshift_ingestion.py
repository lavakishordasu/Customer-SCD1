import psycopg2

BUCKET_SRC = "s3://lava-etl-project/CustomerSCD1JOBS/jobs/cleaned_data/"

# Redshift connection details
REDSHIFT_HOST = "etlproject-workgroup.011528266088.us-east-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "Lavakishor14"

REDSHIFT_TABLE = "etlproject.customers_stage"

# Correct COPY command
copy_sql = f"""
COPY {REDSHIFT_TABLE}
FROM '{BUCKET_SRC}'
IAM_ROLE 'arn:aws:iam::011528266088:role/service-role/AmazonRedshift-CommandsAccessRole-20251120T190659'
FORMAT AS PARQUET;
"""

conn = None

try:
    print("Connecting to Redshift:", REDSHIFT_HOST)

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    conn.autocommit = True
    cur = conn.cursor()

    print("Running COPY command...")
    print(copy_sql)

    cur.execute(copy_sql)

    print("COPY completed successfully!")

except Exception as e:
    print("Error:", str(e))
    raise

finally:
    if conn:
        conn.close()
