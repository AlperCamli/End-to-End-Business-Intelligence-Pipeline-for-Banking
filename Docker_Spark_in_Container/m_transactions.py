# /etl/pg_to_ch_daily_exchange_loop.py
import os
import base64
import urllib.parse
import urllib.request
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format, current_date, when, coalesce, lit
)
from pyspark.sql.types import DecimalType

# === Keep your package set intact ===
PKGS = (
    "com.clickhouse:clickhouse-jdbc:0.6.3,"
    "org.apache.httpcomponents.client5:httpclient5:5.2.1,"
    "org.apache.httpcomponents.core5:httpcore5:5.2.1,"
    "org.postgresql:postgresql:42.7.7"
)

spark = (
    SparkSession.builder
    .appName("pg->ch-transactions-daily")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.jars.packages", PKGS)
    .getOrCreate()
)

DEC2 = DecimalType(18, 2)

# ---------- JDBC helpers ----------
def pg_url():
    return f"jdbc:postgresql://{os.getenv('PGHOST','postgres')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','bi_project_db')}"

def pg_props():
    return {
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "Tonbalikli2003"),
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }

def ch_url(db):
    return f"jdbc:ch://{os.getenv('CH_HOST','clickhouse')}:{os.getenv('CH_PORT','8123')}/{db}"

def ch_props():
    return {
        "user": os.getenv("CH_USER", "default"),
        "password": os.getenv("CH_PASSWORD", "password"),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "socket_timeout": "600000",
    }

# ---------- ClickHouse HTTP (for ALTER PARTITION) ----------
def ch_http(sql: str) -> str:
    host = os.getenv("CH_HTTP_HOST", os.getenv("CH_HOST", "clickhouse"))
    port = os.getenv("CH_HTTP_PORT", os.getenv("CH_PORT", "8123"))
    user = os.getenv("CH_USER", "default")
    pwd  = os.getenv("CH_PASSWORD", "password")
    
    print(f"[DEBUG] Executing ClickHouse SQL: {sql}")
    
    # Encode the SQL query
    encoded_sql = urllib.parse.quote(sql)
    url = f"http://{host}:{port}/?query={encoded_sql}"
    
    req = urllib.request.Request(url, method='POST')
    
    # Add authentication if provided
    if user or pwd:
        token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        req.add_header("Authorization", f"Basic {token}")
    
    # Add headers for better compatibility
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    req.add_header("User-Agent", "PySpark-ETL/1.0")
    
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            response = r.read().decode('utf-8')
            return response
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else "No error details"
        print(f"[ERROR] ClickHouse HTTP Error {e.code}: {e.reason}")
        print(f"[ERROR] Response body: {error_body}")
        print(f"[ERROR] SQL that failed: {sql}")
        raise Exception(f"ClickHouse HTTP Error {e.code}: {error_body}")
    except Exception as e:
        print(f"[ERROR] Unexpected error in ch_http: {e}")
        print(f"[ERROR] SQL that failed: {sql}")
        raise

# ---------- date helpers ----------
def to_date_obj(s: str) -> date:
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def day_id_str(d: date) -> str:
    # partition id used by toYYYYMMDD(Date) -> 'YYYYMMDD'
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

# ---------- Partition management helpers ----------
def partition_exists(table_name: str, day_id: str) -> bool:
    """Check if a partition exists in the given table"""
    try:
        sql = f"SELECT COUNT(*) FROM system.parts WHERE table = '{table_name.split('.')[-1]}' AND partition = '{day_id}'"
        result = ch_http(sql)
        return int(result.strip()) > 0
    except Exception as e:
        print(f"[WARN] Could not check partition existence: {e}")
        return False

def safe_drop_partition(table_name: str, day_id: str):
    """Safely drop a partition if it exists"""
    try:
        if partition_exists(table_name, day_id):
            sql = f"ALTER TABLE {table_name} DROP PARTITION '{day_id}'"
            ch_http(sql)
            print(f"[INFO] Dropped existing partition {day_id} from {table_name}")
        else:
            print(f"[INFO] No existing partition {day_id} in {table_name} to drop")
    except Exception as e:
        print(f"[WARN] Could not drop partition {day_id} from {table_name}: {e}")

# ---------- ETL for a single day ----------
def load_one_day_to_stage(day_str: str):
    # predicate pushdown to Postgres for the single day
    subq = f"(SELECT * FROM prod.transactions WHERE transaction_date = DATE '{day_str}') as t"
    src = spark.read.jdbc(pg_url(), subq, properties=pg_props())
    cnt = src.count()
    print(f"[INFO] {day_str}: read {cnt} rows from Postgres")
    if cnt == 0:
        return 0

    # Build dataframe matching CH DDL (transaction_date as Date)
    fact = (
        src
        .select(
            col("transaction_id_dd").cast("string").alias("transaction_id_dd"),
            col("transaction_date").cast("date").alias("transaction_date"),                        # Date
            col("transaction_time").cast("string").alias("transaction_time"),
            col("transaction_date_key").cast("string").alias("transaction_date_key"),
            col("customer_id").cast("string").alias("customer_id"),
            col("account_id").cast("string").alias("account_id"),
            col("merchant_id").cast("string").alias("merchant_id"),
            col("branch_id").cast("string").alias("branch_id"),
            col("channel_id").cast("string").alias("channel_id"),
            col("transaction_type_id").cast("string").alias("transaction_type_id"),
            col("amount").cast("decimal(18,2)").alias("amount"),
            col("currency").cast("string").alias("currency"),
            col("is_recurring").cast("boolean").alias("is_recurring"),
            col("is_salary").cast("boolean").alias("is_salary"),
            col("previous_balance").cast("decimal(18,2)").alias("previous_balance"),
            col("balance_after_transaction").cast("decimal(18,2)").alias("balance_after_transaction"),
            current_date().cast("date").alias("etl_date")
        )
    )

    # Write to STAGE
    (fact.write
         .mode("append")
         .option("batchsize", "50000")
         .jdbc(ch_url("clk_dw_stage"), "transactions_stage", properties=ch_props()))
    print(f"[INFO] {day_str}: wrote {cnt} rows to clk_dw_stage.transactions_stage")
    return cnt

def ensure_stage_partition_cleared(day_id: str):
    """Drop stage partition if it exists to keep stage clean and avoid EXCHANGE surprises"""
    safe_drop_partition("clk_dw_stage.transactions_stage", day_id)

def exchange_or_replace(day_id: str):
    """
    Attempt to replace partition data from stage to production table.
    Uses REPLACE PARTITION which works whether the partition exists in prod or not.
    """
    try:
        # Use REPLACE PARTITION - it works for both existing and new partitions
        sql = f"ALTER TABLE clk_dw.transactions REPLACE PARTITION '{day_id}' FROM clk_dw_stage.transactions_stage"
        ch_http(sql)
        print(f"[INFO] Successfully REPLACED partition '{day_id}' from STAGE into PROD")
        
    except Exception as e:
        print(f"[ERROR] REPLACE PARTITION failed for {day_id}: {e}")
        
        # Fallback: try to attach the partition data
        try:
            print(f"[INFO] Trying fallback approach for {day_id}...")
            
            # First, ensure target partition is dropped
            safe_drop_partition("clk_dw.transactions", day_id)
            
            # Try to move data using INSERT SELECT
            sql = f"""
            INSERT INTO clk_dw.transactions 
            SELECT * FROM clk_dw_stage.transactions_stage 
            WHERE toYYYYMMDD(transaction_date) = {day_id}
            """
            ch_http(sql)
            print(f"[INFO] Fallback INSERT succeeded for {day_id}")
            
        except Exception as fallback_error:
            print(f"[ERROR] Fallback also failed for {day_id}: {fallback_error}")
            raise Exception(f"Both REPLACE PARTITION and INSERT fallback failed for {day_id}")
    
    finally:
        # Clean up: drop the partition from stage to keep it small
        try:
            safe_drop_partition("clk_dw_stage.transactions_stage", day_id)
        except Exception as cleanup_error:
            print(f"[WARN] Could not clean up stage partition {day_id}: {cleanup_error}")

# ---------- main loop ----------
if __name__ == "__main__":
    # Input options:
    # 1) LOAD_DATES="YYYY-MM-DD,YYYY-MM-DD,..."
    # 2) LOAD_START="YYYY-MM-DD" and LOAD_END="YYYY-MM-DD" (inclusive range)
    dates_arg = os.getenv("LOAD_DATES", "2024-04-14, 2024-04-17")
    if False:  # to test range mode, set to False
        days = [d.strip() for d in dates_arg.split(",") if d.strip()]
        print("Using LOAD_DATES")
    else:
        print("Using LOAD_START/LOAD_END range")
        start = os.getenv("LOAD_START", "2024-01-01")
        end   = os.getenv("LOAD_END", "2024-12-28")
        if not (start and end):
            raise SystemExit("Set either LOAD_DATES (comma-separated) OR LOAD_START and LOAD_END (YYYY-MM-DD).")
        sd, ed = to_date_obj(start), to_date_obj(end)
        if ed < sd:
            raise SystemExit("LOAD_END must be >= LOAD_START")
        days = [d.isoformat() for d in daterange(sd, ed)]

    # Optional sanity: filter out pre-1970 days because CH Date won't accept them.
    clean_days = []
    for dstr in days:
        if to_date_obj(dstr) < date(1970, 1, 1):
            print(f"[WARN] Skipping {dstr}: ClickHouse Date does not support pre-1970.")
        else:
            clean_days.append(dstr)

    print(f"[INFO] Will process days: {clean_days}")

    for dstr in clean_days:
        print(f"[INFO] Processing day {dstr}")
        did = day_id_str(to_date_obj(dstr))
        print(f"[INFO] Partition ID: {did}")
        
        try:
            ensure_stage_partition_cleared(did)
            wrote = load_one_day_to_stage(dstr)
            if wrote > 0:
                exchange_or_replace(did)
            else:
                print(f"[INFO] {dstr}: nothing to load (skipping partition operations).")
        except Exception as e:
            print(f"[ERROR] Failed to process day {dstr}: {e}")
            # Continue with other days rather than stopping completely
            continue

    spark.stop()
    print("[DONE] Daily loop complete.")