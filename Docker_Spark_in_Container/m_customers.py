# save as /etl/pg_to_ch_date_string.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_date

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
    src = spark.read.jdbc(pg_url(), "prod.customers", properties=pg_props())
    print(f"Read {src.count()} rows from prod.customers")

    fact = (
        src
        .select(
            col("customer_id").cast("string").alias("customer_id"),
            col("first_name").cast("string").alias("first_name"),
            col("last_name").cast("string").alias("last_name"),
            col("date_of_birth").cast("date").alias("date_of_birth"),
            col("gender").cast("string").alias("gender"),
            col("marital_status").cast("string").alias("marital_status"),
            col("education_level").cast("string").alias("education_level"),
            col("employment_status").cast("string").alias("employment_status"),
            col("job_title").cast("string").alias("job_title"),
            col("housing").cast("string").alias("housing"),
            col("monthly_salary_usd").cast("decimal(18,2)").alias("monthly_salary_usd"),
            col("monthly_passive_income_usd").cast("decimal(18,2)").alias("monthly_passive_income_usd"),
            col("monthly_rent_in_usd").cast("decimal(18,2)").alias("monthly_rent_in_usd"),
            col("monthly_rent_out_usd").cast("decimal(18,2)").alias("monthly_rent_out_usd"),
            col("annual_income").cast("decimal(18,2)").alias("annual_income"),
            col("monthly_salary_day").cast("int").alias("monthly_salary_day"),
            col("bill_responsible").cast("boolean").alias("bill_responsible"),
            col("bills_due_day").cast("decimal(18,2)").alias("bills_due_day"),
            col("is_renter").cast("boolean").alias("is_renter"),
            col("rent_due_day").cast("decimal(18,2)").alias("rent_due_day"),
            col("credit_score").cast("int").alias("credit_score"),
            col("num_dependents").cast("int").alias("num_dependents"),
            col("segment").cast("string").alias("segment"),
            col("has_passive_income").cast("boolean").alias("has_passive_income"),
            col("has_real_estate_income").cast("boolean").alias("has_real_estate_income"),
            current_date().cast("date").alias("etl_date")
        )
    )

    print(f"Prepared {fact.count()} rows to write to ClickHouse.")
    fact.show(5, truncate=False)

    # Write to ClickHouse table with string date
    (
        fact.write
        .mode("append")
        .option("batchsize", "50000")
        .jdbc(ch_url(), "customers", properties=ch_props())
    )

    # Sanity read-back
    back = spark.read.jdbc(ch_url(), "customers", properties=ch_props())
    back.orderBy("customer_id").show(5, truncate=False)

    spark.stop()
