import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp

# --- Spark session (driver on HOST; if you run inside Docker, see note below) ---
def spark_session(app="pg-prod-to-ch"):
    pkgs = ",".join([
        "org.postgresql:postgresql:42.0.0",
        "com.clickhouse:clickhouse-jdbc:0.6.3"
    ])
    return (
        SparkSession.builder
        .appName(app)
        .master(os.getenv("SPARK_MASTER_URL", "spark://localhost:7077"))
        .config("spark.jars.packages", pkgs)
        # if driver runs on host and executors in Docker, let them call back:
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "host.docker.internal"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )

# --- JDBC conn helpers ---
def pg_url():
    host = os.getenv("PGHOST", "localhost")
    print(f"Connecting to Postgres at {host}...")
    port = os.getenv("PGPORT", "5431")
    print(f"Postgres port: {port}")
    db   = os.getenv("PGDATABASE", "bi_project_db")
    print(f"Postgres database: {db}")
    return f"jdbc:postgresql://{host}:{port}/{db}"

def pg_props():
    a = {
        "user": os.getenv("PGUSER", "XXXXXX"),
        "password": os.getenv("PGPASSWORD", "XXXXXX"),
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000"  # stream rows
    }
    print(a)
    return a

def ch_url():
    host = os.getenv("CH_HOST", "localhost")
    port = os.getenv("CH_PORT", "8123")
    db   = os.getenv("CH_DB", "clkdb")
    return f"jdbc:ch://{host}:{port}/{db}"

def ch_props():
    return {
        "user": os.getenv("CH_USER", "default"),
        "password": os.getenv("CH_PASSWORD", ""),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

if __name__ == "__main__":
    spark = spark_session("pg-prod-to-ch")
    print("Driver Spark:", spark.version)

    # Read prod.transactions from Postgres
    print("Reading prod.transactions from------------------------------ Postgres...")
    src = spark.read.jdbc(pg_url(), "prod.transactions", properties=pg_props())
    src.show()
    print(f"Read {src.count()} rows from------------------------------ prod.transactions")

    # Normalize columns for ClickHouse target schema
    cols = src.columns
    if "ts" in cols:
        print("Found 'ts' column in prod.transactions.------------------------------ Using it directly.")
        fact = (src
                .withColumn("amount", col("amount") if "amount" in cols else col("transaction_amount").cast("double"))
                .withColumn("type", col("type") if "type" in cols else col("transaction_type"))
                .selectExpr(
                    "cast(transaction_id as string) as transaction_id",
                    "cast(account_number as string) as account_number",
                    "cast(customer_id as string) as customer_id",
                    "ts",
                    "cast(amount as double) as amount",
                    "cast(type as string) as type",
                    "cast(category as string) as category",
                    "cast(location as string) as location",
                    "cast(channel as string) as channel"
                ))
    else:
        print("No 'ts' column found in prod.transactions.------------------------------ Assuming separate date/time fields.")
        # If prod still has separate date/time, build ts and rename fields
        fact = (src
                .withColumn("ts",
                            to_timestamp(concat_ws(" ",
                                                   col("transaction_date").cast("string"),
                                                   col("transaction_time").cast("string")),
                                         "yyyy-MM-dd HH:mm:ss"))
                .withColumn("amount", col("transaction_amount").cast("double"))
                .withColumn("type", col("transaction_type"))
                .selectExpr(
                    "cast(transaction_id as string) as transaction_id",
                    "cast(account_number as string) as account_number",
                    "cast(customer_id as string) as customer_id",
                    "ts",
                    "cast(amount as double) as amount",
                    "cast(type as string) as type",
                    "cast(category as string) as category",
                    "cast(location as string) as location",
                    "cast(channel as string) as channel"
                ))

    # Write to ClickHouse
    (fact.write
         .mode("append")
         .option("batchsize", "50000")
         .jdbc(ch_url(), "transactions", properties=ch_props()))

    print("Loaded prod.transactions -> ClickHouse clkdb.transactions")
    spark.stop()
