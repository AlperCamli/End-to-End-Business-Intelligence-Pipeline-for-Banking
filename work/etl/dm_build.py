from pyspark.sql.functions import col, concat_ws, to_timestamp, sum as _sum, count as _count, date_format
from common import spark_session, pg_jdbc_url, pg_props, ch_jdbc_url, ch_props

spark = spark_session("dm-build")

# Read from Postgres staging
stg = spark.read.jdbc(pg_jdbc_url(), "prod.transactions", properties=pg_props())

# Example: create a simple fact table and a daily aggregate
fact = (stg
    .withColumn("ts", to_timestamp(concat_ws(" ", col("transaction_date").cast("string"), col("transaction_time"))))
    .selectExpr(
        "transaction_id",
        "account_number",
        "customer_id",
        "ts",
        "cast(transaction_amount as double) as amount",
        "transaction_type as type",
        "category",
        "location",
        "channel"
    )
)

daily_agg = (fact
    .withColumn("day", date_format(col("ts"), "yyyy-MM-dd"))
    .groupBy("account_number", "day")
    .agg(_count("*").alias("tx_count"), _sum("amount").alias("total_amount"))
)

# Write fact to ClickHouse (fast analytics)
(fact.write
    .mode("append")
    .jdbc(ch_jdbc_url(), "transactions", properties=ch_props()))

# (Optionally) keep aggregates in Postgres DWH too
(daily_agg.write
    .mode("overwrite")
    .option("truncate", "true")
    .jdbc(pg_jdbc_url(), "dwh.daily_account_agg", properties=pg_props()))

print("Fact loaded to ClickHouse and daily aggregate saved to Postgres dwh.")
spark.stop()
