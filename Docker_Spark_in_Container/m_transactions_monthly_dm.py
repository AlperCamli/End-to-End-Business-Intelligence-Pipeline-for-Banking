# save as /etl/pg_to_ch_date_string.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_date, datediff, floor, lit

# KEEP your CH packages; append Postgres JDBC (nothing else changes)
PKGS = (
    "com.clickhouse:clickhouse-jdbc:0.6.3,"
    "org.apache.httpcomponents.client5:httpclient5:5.2.1,"
    "org.apache.httpcomponents.core5:httpcore5:5.2.1,"
)

spark = (
    SparkSession.builder
    .appName("dw->dm")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.jars.packages", PKGS)
    .getOrCreate()
)

# --- JDBC URLs/props (driver INSIDE Docker; use service names) ---
def ch_url_Source():
    host = os.getenv("CH_HOST", "clickhouse")
    port = os.getenv("CH_PORT", "8123")
    db   = os.getenv("CH_DB", "clk_dw")
    url = f"jdbc:ch://{host}:{port}/{db}"  # DB in URL → pass only table name later
    print(f"[CH] {url}")
    return url

def ch_url_Target():
    host = os.getenv("CH_HOST", "clickhouse")
    port = os.getenv("CH_PORT", "8123")
    db   = os.getenv("CH_DB", "clk_dm")
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

    # Read Postgres prod table
    src_Transactions = spark.read.jdbc(ch_url_Source(), "clk_dw.transactions", properties=ch_props())
    src_Calender = spark.read.jdbc(ch_url_Source(), "clk_dw.calender", properties=ch_props())
    src_Transaction_types = spark.read.jdbc(ch_url_Source(), "clk_dw.transaction_types", properties=ch_props())
    src_Customers = spark.read.jdbc(ch_url_Source(), "clk_dw.customers", properties=ch_props()) \
                    .withColumn("age", floor(datediff(current_date(), col("date_of_birth")) / lit(365))) \
                    .alias("s_cust")    

    src = src_Transactions.join(src_Calender, src_Transactions.transaction_date_key == src_Calender.date_key, "left")
    src = src.join(src_Transaction_types, src_Transactions.transaction_type_id == src_Transaction_types.transaction_type_id, "left")
    src = src.join(src_Customers, src_Transactions.customer_id == src_Customers.customer_id, "left")

    src = src.select(
        src_Transactions["*"],
        src_Customers["segment"].alias("segment"),
        src_Calender["date_key"].alias("transaction_date_key"), 
        src_Calender["month"].alias("month")
    ).where(src_Transaction_types.is_Income == False
    ).groupBy("customer_id", "segment"
    ).pivot("month"
    ).sum("amount").orderBy("customer_id").alias("src")

    new_src = src.join(src_Customers, on="customer_id", how="left") \
        .select("src.customer_id", "s_cust.first_name", "s_cust.last_name", "s_cust.monthly_salary_usd", "s_cust.age",
        "src.January", "src.February", "src.March", "src.April", "src.May", "src.June", "src.July", "src.August",
        "src.September", "src.October", "src.November", "src.December")

    new_src.show(20, truncate=False)


    fact = (
        new_src
    )



    # Write to ClickHouse table with string date
    (
        fact.write
        .mode("append")
        .option("batchsize", "50000")
        .jdbc(ch_url_Target(), "customer_monthly", properties=ch_props())
    )

    # Sanity read-back
    back = spark.read.jdbc(ch_url_Target(), "customer_monthly", properties=ch_props())
    back.orderBy("customer_id").show(5, truncate=False)

    spark.stop()
