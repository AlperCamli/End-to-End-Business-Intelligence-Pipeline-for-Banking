#!/usr/bin/env python3
"""
Bulk-load all your CSVs with COPY (very fast).
- Creates tables using the DDL above (embedded in this file).
- Loads each CSV if the file exists next to this script.
"""

import os
import sys
import psycopg2
from psycopg2 import sql

# ---- configure here or via env vars ----
PGHOST = os.getenv("PGHOST", "localhost")   # use "postgres" if running inside Docker network
PGPORT = int(os.getenv("PGPORT", "5431"))
PGDB   = os.getenv("PGDATABASE", "XXXXXX")
PGUSER = os.getenv("PGUSER", "XXXXXX")
PGPWD  = os.getenv("PGPASSWORD", "XXXXXX")  # change if needed

CSV_DIR = os.getenv("CSV_DIR", "/Users/alpercamli/Desktop/BI_Project/DB_Tables/")  # folder where the CSVs live

# filename -> table name
FILES = {
    "accounts.csv":            "accounts",
    "branches.csv":            "branches",
    "calender.csv":            "calender",
    "categories.csv":          "categories",
    "channels.csv":            "channels",
    "cities.csv":              "cities",
    "customers.csv":        "customers",
    "merchants.csv":           "merchants",
    "transaction_types.csv":   "transaction_types",
    "transactions.csv":        "transactions",  # large file (optional if not present yet)
}


DDL_SQL = r'''
-- (same SQL as above; keep them in sync)
SET search_path = prod;
CREATE TABLE IF NOT EXISTS "accounts" (
    "account_number" TEXT,
    "customer_id" TEXT,
    "initial_balance" NUMERIC(18,2),
    "overdraft_limit" NUMERIC(18,2),
    "branch_id" TEXT,
    "account_open_date" DATE
    , PRIMARY KEY ("account_number")
);
CREATE TABLE IF NOT EXISTS "branches" (
    "branch_id" TEXT,
    "branch_name" TEXT,
    "city_id" TEXT,
    "branch_start_date" DATE
    , PRIMARY KEY ("branch_id")
);
CREATE TABLE IF NOT EXISTS "calender" (
    "date_key" TEXT,
    "calender" DATE,
    "year" INTEGER,
    "quarter" INTEGER,
    "day" INTEGER,
    "month" TEXT,
    "weekday" TEXT,
    "week_of_year" INTEGER,
    "isweekend" BOOLEAN,
    "isholiday" BOOLEAN,
    "isholiday_in_us" BOOLEAN,
    "day_in_year" INTEGER,
    "day_in_quarter" INTEGER,
    "day_in_month" INTEGER,
    "day_in_week" INTEGER
    , PRIMARY KEY ("date_key")
);
CREATE TABLE IF NOT EXISTS "categories" (
    "category_id" INTEGER,
    "category_name" TEXT,
    "is_essential" BOOLEAN,
    PRIMARY KEY ("category_id")
);
CREATE TABLE IF NOT EXISTS "channels" (
    "channel_id" TEXT,
    "channel_name" TEXT,
    PRIMARY KEY ("channel_id")
);
CREATE TABLE IF NOT EXISTS "cities" (
    "city_id" TEXT,
    "city_name" TEXT,
    "country" TEXT,
    "is_Capital" BOOLEAN
    , PRIMARY KEY ("city_id")
);
CREATE TABLE IF NOT EXISTS "customers" (
    "customer_id" TEXT,
    "first_name" TEXT,
    "last_name" TEXT,
    "date_of_birth" DATE,
    "gender" TEXT,
    "marital_status" TEXT,
    "education_level" TEXT,
    "employment_status" TEXT,
    "job_title" TEXT,
    "housing" TEXT,
    "monthly_salary_usd" NUMERIC(18,2),
    "monthly_passive_income_usd" NUMERIC(18,2),
    "monthly_rent_in_usd" NUMERIC(18,2),
    "monthly_rent_out_usd" NUMERIC(18,2),
    "annual_income" NUMERIC(18,2),
    "monthly_salary_day" INTEGER,
    "bill_responsible" BOOLEAN,
    "bills_due_day" DECIMAL,
    "is_renter" BOOLEAN,
    "rent_due_day" DECIMAL,
    "credit_score" INTEGER,
    "num_dependents" INTEGER,
    "segment" TEXT,
    "has_passive_income" BOOLEAN,
    "has_real_estate_income" BOOLEAN
    , PRIMARY KEY ("customer_id")
);
CREATE TABLE IF NOT EXISTS "merchants" (
    "merchant_id" TEXT,
    "merchant_name" TEXT,
    "category_id" TEXT,
    "city_id" TEXT
    , PRIMARY KEY ("merchant_id")
);
CREATE TABLE IF NOT EXISTS "transaction_types" (
    "transaction_type_id" TEXT,
    "transaction_type_name" TEXT,
    "is_Transfer" BOOLEAN,
    "is_Income" BOOLEAN
    , PRIMARY KEY ("transaction_type_id")
);
CREATE TABLE IF NOT EXISTS "transactions" (
    "transaction_id_dd" TEXT,
    "transaction_date" DATE,
    "transaction_time" TIME,
    "transaction_date_key" TEXT,
    "customer_id" TEXT,
    "account_id" TEXT,
    "merchant_id" TEXT,
    "branch_id" TEXT,
    "channel_id" TEXT,
    "transaction_type_id" TEXT,
    "amount" NUMERIC(18,2),
    "currency" VARCHAR(8),
    "is_recurring" BOOLEAN,
    "is_salary" BOOLEAN,
    "previous_balance" NUMERIC(18,2),
    "balance_after_transaction" NUMERIC(18,2)
    , PRIMARY KEY ("transaction_id_dd")
);
CREATE INDEX IF NOT EXISTS idx_transactions_date     ON "transactions" ("transaction_date");
CREATE INDEX IF NOT EXISTS idx_transactions_customer ON "transactions" ("customer_id");
CREATE INDEX IF NOT EXISTS idx_transactions_account  ON "transactions" ("account_id");
CREATE INDEX IF NOT EXISTS idx_transactions_merchant ON "transactions" ("merchant_id");
CREATE INDEX IF NOT EXISTS idx_transactions_channel  ON "transactions" ("channel_id");
CREATE INDEX IF NOT EXISTS idx_transactions_branch   ON "transactions" ("branch_id");
CREATE INDEX IF NOT EXISTS idx_transactions_type     ON "transactions" ("transaction_type_id");
CREATE INDEX IF NOT EXISTS idx_transactions_datekey  ON "transactions" ("transaction_date_key");
'''

def run_sql(cur, sql_text: str):
    cur.execute(sql_text)

def copy_csv(cur, csv_path: str, table: str):
    # COPY maps by column order (so we created tables to match your CSV order).
    with open(csv_path, "r", encoding="utf-8") as fh:
        cur.copy_expert(
            sql=sql.SQL('COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)')
               .format(sql.Identifier(table)),
            file=fh
        )

def main():
    os.environ.setdefault("PGTZ", "UTC")  # avoid timezone surprises
    print(f"Connecting to postgresql://{PGUSER}@{PGHOST}:{PGPORT}/{PGDB}")
    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDB, user=PGUSER, password=PGPWD
    )
    try:
        with conn:
            with conn.cursor() as cur:
                # ensure consistent parsing
                cur.execute("SET client_encoding TO 'UTF8';")
                cur.execute("SET datestyle TO ISO, YMD;")

                print("Creating tables / indexes...")
                run_sql(cur, DDL_SQL)

                for fname, tname in FILES.items():
                    fpath = os.path.join(CSV_DIR, fname)
                    if os.path.exists(fpath):
                        print(f"Loading {fpath} -> {tname} ...")
                        copy_csv(cur, fpath, tname)
                    else:
                        print(f"Skip (not found): {fpath}")

        print("✅ All done.")
    except Exception as e:
        print("❌ ERROR:", e)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
