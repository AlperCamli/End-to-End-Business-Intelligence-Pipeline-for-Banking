import os
from pyspark.sql.functions import col, to_date, to_timestamp, lpad
from pyspark.sql.types import DecimalType
from common import spark_session, pg_jdbc_url, pg_props

DATA_PATH = "/Users/XXXXXX/bi-stack/data/transactions.csv"  # ../data from /work

spark = spark_session("extract-load")

# 1) Read raw CSV
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(DATA_PATH))

# 2) Light cleaning / casting to match staging schema
df_clean = (df
    .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(12,2)))
    .withColumn("transaction_date", to_date(col("transaction_date")))
    .withColumn("transaction_time", col("transaction_time").cast("string"))
    .withColumn("transaction_id", lpad(col("transaction_id").cast("string"), 11, "0"))
)

# 3) Write to Postgres staging
(df_clean.write
    .mode("append")
    .jdbc(pg_jdbc_url(), "staging.transactions", properties=pg_props()))

print("Loaded to Postgres staging.transactions")
spark.stop()
