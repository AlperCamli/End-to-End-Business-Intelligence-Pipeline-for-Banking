# save as /etl/pg_to_ch_date_string.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_date, datediff, floor, lit, avg, countDistinct, year

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
    src_Transactions = spark.read.jdbc(ch_url_Source(), "clk_dw.transactions", properties=ch_props()).alias("Trans")
    src_Category = spark.read.jdbc(ch_url_Source(), "clk_dw.categories", properties=ch_props()).alias("Cat")
    src_Merchants = spark.read.jdbc(ch_url_Source(), "clk_dw.merchants", properties=ch_props()).alias("Merc")
    src_Customers = spark.read.jdbc(ch_url_Source(), "clk_dw.customers", properties=ch_props()).alias("Cust")
    src_Accounts = spark.read.jdbc(ch_url_Source(), "clk_dw.accounts", properties=ch_props()).alias("Acc")
    src_Channels = spark.read.jdbc(ch_url_Source(), "clk_dw.channels", properties=ch_props()).alias("Chan")
    src_Calender = spark.read.jdbc(ch_url_Source(), "clk_dw.calender", properties=ch_props()).alias("Cal")
    src_Cities = spark.read.jdbc(ch_url_Source(), "clk_dw.cities", properties=ch_props()).alias("Cit")
    src_Branches = spark.read.jdbc(ch_url_Source(), "clk_dw.branches", properties=ch_props()).alias("Bran")
    src_Transaction_types = spark.read.jdbc(ch_url_Source(), "clk_dw.transaction_types", properties=ch_props()).alias("Tt")
    
    src = src_Transactions.join(src_Category, src_Category.category_id == src_Transactions.category_id, "left")
    src = src.join(src_Merchants, src_Merchants.merchant_id == src_Transactions.merchant_id, "left")
    src = src.join(src_Customers, src_Transactions.customer_id == src_Customers.customer_id, "left")
    src = src.join(src_Accounts, src_Accounts.account_id == src_Transactions.account_id, "left")
    src = src.join(src_Channels, src_Channels.channel_id == src_Transactions.channel_id, "left")
    src = src.join(src_Calender, src_Calender.date_key == src_Transactions.transaction_date_key, "left")
    src = src.join(src_Cities, src_Cities.city_id == src_Customers.city_id, "left")
    src = src.join(src_Branches, src_Branches.branch_id == src_Accounts.branch_id, "left")
    src = src.join(src_Transaction_types, src_Transactions.transaction_type_id == src_Transaction_types.transaction_type_id, "left")
    src = src.where(src_Transaction_types.is_Income == False).alias("src")




    fact = (
        src
    )



    # Write to ClickHouse table with string date
    (
        fact.write
        .mode("append")
        .option("batchsize", "50000")
        .jdbc(ch_url_Target(), "count_category_segment", properties=ch_props())
    )

    # Sanity read-back
    back = spark.read.jdbc(ch_url_Target(), "count_category_segment", properties=ch_props())
    back.orderBy("segment").show(6, truncate=False)

    spark.stop()
