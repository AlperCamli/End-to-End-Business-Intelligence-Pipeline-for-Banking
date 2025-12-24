# save as /etl/pg_to_ch_date_string.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

# KEEP your CH packages; append Postgres JDBC (nothing else changes)
PKGS = (
    "com.clickhouse:clickhouse-jdbc:0.6.3,"
    "org.apache.httpcomponents.client5:httpclient5:5.2.1,"
    "org.apache.httpcomponents.core5:httpcore5:5.2.1,"
    "org.postgresql:postgresql:42.7.7"
)

spark = (
    SparkSession.builder
    .appName("pg->ch-date-string")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.jars.packages", PKGS)
    .getOrCreate()
)

# --- JDBC URLs/props (driver INSIDE Docker; use service names) ---
def pg_url():
    host = os.getenv("PGHOST", "postgres")
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE", "bi_project_db")
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    print(f"[PG] {url}")
    return url

def pg_props():
    return {
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "Tonbalikli2003"),
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000"
    }

def ch_url():
    host = os.getenv("CH_HOST", "clickhouse")
    port = os.getenv("CH_PORT", "8123")
    db   = os.getenv("CH_DB", "clk_dw")
    url = f"jdbc:ch://{host}:{port}/{db}"  # DB in URL → pass only table name later
    print(f"[CH] {url}")
    return url

def ch_props():
    return {
        "user": os.getenv("CH_USER", "default"),
        "password": os.getenv("CH_PASSWORD", "password"),   # change if you have one
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "socket_timeout": "600000"
    }


if __name__ == "__main__":
    # quick probes (fail fast if creds/network off)
    spark.read.jdbc(pg_url(), "(SELECT 1) t", properties=pg_props()).show()
    spark.read.jdbc(ch_url(), "(SELECT 1) t", properties=ch_props()).show()

    # Read Postgres prod table
    src = spark.read.jdbc(pg_url(), "prod.calender", properties=pg_props())
    print(f"Read {src.count()} rows from prod.calender")

    fact = (
        src
        .select(
            col("date_key").cast("string").alias("date_key"),
            col("calender").cast("date").alias("calender"),
            col("year").cast("int").alias("year"),
            col("quarter").cast("int").alias("quarter"),
            col("day").cast("int").alias("day"),
            col("month").cast("string").alias("month"),
            col("weekday").cast("string").alias("weekday"),
            col("week_of_year").cast("int").alias("week_of_year"),
            col("isweekend").cast("boolean").alias("isweekend"),
            col("isholiday").cast("boolean").alias("isholiday"),
            col("isholiday_in_US").cast("boolean").alias("isholiday_in_US"),
            col("day_in_year").cast("int").alias("day_in_year"),
            col("day_in_quarter").cast("int").alias("day_in_quarter"),
            col("day_in_month").cast("int").alias("day_in_month"),
            col("day_in_week").cast("int").alias("day_in_week"),
        )
    )

    print(f"Prepared {fact.count()} rows to write to ClickHouse (date kept as string).")
    fact.show(5, truncate=False)

    # Write to ClickHouse table with string date
    (
        fact.write
        .mode("append")
        .option("batchsize", "50000")
        .jdbc(ch_url(), "calender", properties=ch_props())
    )

    # Sanity read-back
    back = spark.read.jdbc(ch_url(), "calender", properties=ch_props())
    back.orderBy("calender").show(5, truncate=False)

    spark.stop()
