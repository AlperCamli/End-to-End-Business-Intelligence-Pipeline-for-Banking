import os
from pyspark.sql import SparkSession

def spark_session(app="bi-etl"):
    # Bring JDBC drivers at runtime
    pkgs = ",".join([
        "org.postgresql:postgresql:42.7.3",
        "com.clickhouse:clickhouse-jdbc:0.6.3"
    ])
    return (
        SparkSession.builder
        .appName(app)
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", pkgs)
        .getOrCreate()
    )

def pg_jdbc_url():
    # Inside Docker network we reach postgres by service name 'postgres' on its internal 5432
    db = os.getenv("POSTGRES_DB", "bi_project_db")
    return f"jdbc:postgresql://postgres:5432/{db}"

def pg_props():
    return {
        "user": os.getenv("POSTGRES_USER", "bi_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "bi_pass"),
        "driver": "org.postgresql.Driver",
    }

def ch_jdbc_url():
    # ClickHouse modern URL form
    return "jdbc:ch://clickhouse:8123/bi_dw"

def ch_props():
    return {
        "user": os.getenv("CLICKHOUSE_USER", "bi_user"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "bi_pass"),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }
