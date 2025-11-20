import os
import json
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# ---------------------------------------------------------
#                  SETUP LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl_job.log"),  # Log to file
        logging.StreamHandler()              # Also log to console
    ]
)

logger = logging.getLogger("ETL_Logger")

# ---------------------------------------------------------
#                  SET HADOOP & PATH
# ---------------------------------------------------------
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

# Load output path from environment variable
OUTPUT_PATH = os.environ.get("OUTPUT_PATH")

# ---------------------------------------------------------
#                  SPARK PACKAGES
# ---------------------------------------------------------
PKGS = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "mysql:mysql-connector-java:8.0.33"
)

# ---------------------------------------------------------
#                  CREATE SPARK SESSION
# ---------------------------------------------------------
logger.info("Creating Spark session...")

spark = (
    SparkSession.builder
    .appName("RDS MySQL Ingestion")
    .master("local[*]")
    .config("spark.jars.packages", PKGS)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.region", "ap-south-1")
    .getOrCreate()
)

logger.info(f"Spark version: {spark.version}")

# ---------------------------------------------------------
#           FETCH RDS CREDENTIALS FROM SECRETS MANAGER
# ---------------------------------------------------------
def get_secret(secret_name="dev/rds/mysql", region_name="us-east-1"):
    """Fetch RDS credentials from AWS Secrets Manager"""
    logger.info(f"Fetching secrets from AWS Secrets Manager: {secret_name}")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Error fetching secret: {e}")
        raise e

    logger.info("Secrets fetched successfully")
    return json.loads(response['SecretString'])


DB_Params = get_secret()
logger.info(f"Loaded secret keys: {list(DB_Params.keys())}")

# ---------------------------------------------------------
#               BUILD JDBC CONNECTION URL
# ---------------------------------------------------------
def build_jdbc_url(secret):
    host = secret.get("host") or secret.get("Host")
    port = secret.get("port") or 3306
    database = secret.get("dbname") or secret.get("database")

    if not host or not database:
        logger.error("Secret must include host & database keys.")
        raise RuntimeError("Secret must include host & database keys.")

    url = f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&serverTimezone=UTC"
    logger.info(f"Built JDBC URL for database: {database}")
    return url


db_url = build_jdbc_url(DB_Params)

# ---------------------------------------------------------
#               READ DATA FROM RDS USING JDBC
# ---------------------------------------------------------
logger.info("Reading data from RDS MySQL...")

query = """
(
    SELECT customer_id, name, email, phone, address, created_at
    FROM customers
) AS tmp
"""

try:
    df = (
        spark.read
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", query)
        .option("user", DB_Params["user"])
        .option("password", DB_Params["Password"])
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )
    logger.info(f"Loaded {df.count()} rows from RDS")
except Exception as e:
    logger.error(f"Error reading data from RDS: {e}")
    raise e

# ---------------------------------------------------------
#               CLEAN DATA
# ---------------------------------------------------------
logger.info("Cleaning data: replacing empty strings and dropping nulls...")

df_cleaned = df.na.replace("", None).dropna(how="any")
df_cleaned = df_cleaned.withColumn("created_at", col("created_at").cast("string"))

logger.info(f"Data cleaned. Total rows after cleaning: {df_cleaned.count()}")

# ---------------------------------------------------------
#               WRITE DATA TO S3
# ---------------------------------------------------------
logger.info(f"Writing cleaned data to S3 path: {OUTPUT_PATH} partitioned by 'created_at'...")

try:
    df_cleaned.write.mode("overwrite").partitionBy("created_at").parquet(OUTPUT_PATH)
    logger.info(f"Data written successfully to {OUTPUT_PATH}")
except Exception as e:
    logger.error(f"Error writing data to S3: {e}")
    raise e

