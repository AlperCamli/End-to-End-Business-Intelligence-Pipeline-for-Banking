-- =========================================================
-- Banking BI Schema - All Tables + Partitioned Transactions
-- PostgreSQL DDL (single file)
-- =========================================================

CREATE SCHEMA IF NOT EXISTS prod;
SET search_path = prod, public;

-- =========================================================
-- 1) Date dimension (create first so FKs can reference it)
-- =========================================================
CREATE TABLE IF NOT EXISTS date_dim (
    date              DATE PRIMARY KEY,
    year              INTEGER NOT NULL,
    month             VARCHAR(20) NOT NULL,   -- e.g., 'January'
    weekday           VARCHAR(20) NOT NULL,   -- e.g., 'Monday'
    isweekend         BOOLEAN NOT NULL DEFAULT FALSE,
    isholiday         BOOLEAN NOT NULL DEFAULT FALSE,
    isholiday_in_us   BOOLEAN NOT NULL DEFAULT FALSE,
    day_in_quarter    INTEGER NOT NULL,
    day_in_month      INTEGER NOT NULL,
    day_in_week       INTEGER NOT NULL
);

-- =========================================================
-- 2) Core lookup / dimension tables
-- =========================================================

CREATE TABLE IF NOT EXISTS cities (
    city_id        INTEGER PRIMARY KEY,
    city_name      VARCHAR(120) NOT NULL,
    country        VARCHAR(120) NOT NULL,
    is_capital     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS categories (
    category_id    INTEGER PRIMARY KEY,
    category_name  VARCHAR(120) NOT NULL,
    created_at     DATE NOT NULL,
    updated_at     DATE NOT NULL,
    risk_level     INTEGER NOT NULL DEFAULT 0,
    is_essential   BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT fk_categories_created_at
        FOREIGN KEY (created_at)  REFERENCES prod.date_dim(date),
    CONSTRAINT fk_categories_updated_at
        FOREIGN KEY (updated_at)  REFERENCES prod.date_dim(date)
);

CREATE TABLE IF NOT EXISTS channels (
    channel_id    INTEGER PRIMARY KEY,
    channel_name  VARCHAR(80) NOT NULL,
    create_date   DATE NOT NULL,
    update_date   DATE NOT NULL,
    CONSTRAINT fk_channels_create_date
        FOREIGN KEY (create_date) REFERENCES prod.date_dim(date),
    CONSTRAINT fk_channels_update_date
        FOREIGN KEY (update_date) REFERENCES prod.date_dim(date)
);

CREATE TABLE IF NOT EXISTS transaction_types (
    transaction_type_id   INTEGER PRIMARY KEY,
    transaction_type_name VARCHAR(120) NOT NULL,
    is_transfer           BOOLEAN NOT NULL DEFAULT FALSE,
    is_income             BOOLEAN NOT NULL DEFAULT FALSE,
    created_at            DATE NOT NULL,
    updated_at            DATE NOT NULL,
    CONSTRAINT fk_tx_types_created_at
        FOREIGN KEY (created_at) REFERENCES prod.date_dim(date),
    CONSTRAINT fk_tx_types_updated_at
        FOREIGN KEY (updated_at) REFERENCES prod.date_dim(date)
);

-- =========================================================
-- 3) Geographical / organizational tables
-- =========================================================
CREATE TABLE IF NOT EXISTS branches (
    branch_id          INTEGER PRIMARY KEY,
    branch_name        VARCHAR(120) NOT NULL,
    city_id            INTEGER NOT NULL,
    branch_start_date  DATE NOT NULL,
    CONSTRAINT fk_branches_city
        FOREIGN KEY (city_id) REFERENCES prod.cities(city_id),
    CONSTRAINT fk_branches_start_date
        FOREIGN KEY (branch_start_date) REFERENCES prod.date_dim(date)
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id   VARCHAR(64) PRIMARY KEY,
    merchant_name VARCHAR(160) NOT NULL,
    category_id   INTEGER NOT NULL,
    city_id       INTEGER NOT NULL,
    CONSTRAINT fk_merchants_category
        FOREIGN KEY (category_id) REFERENCES prod.categories(category_id),
    CONSTRAINT fk_merchants_city
        FOREIGN KEY (city_id) REFERENCES prod.cities(city_id)
);

-- =========================================================
-- 4) Customer & Account tables
-- =========================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id        INTEGER PRIMARY KEY,
    first_name         VARCHAR(80)  NOT NULL,
    last_name          VARCHAR(80)  NOT NULL,
    date_of_birth      DATE         NOT NULL,
    gender             VARCHAR(20),
    marital_status     VARCHAR(30),
    education_level    VARCHAR(60),
    employment_status  VARCHAR(60),
    job_title          VARCHAR(120),
    housing            VARCHAR(60),
    annual_income      NUMERIC(14,2),
    monthly_salary_day DATE         NOT NULL,
    credit_score       NUMERIC(6,2),
    num_dependents     INTEGER      NOT NULL DEFAULT 0,
    CONSTRAINT fk_customers_dob
        FOREIGN KEY (date_of_birth)      REFERENCES prod.date_dim(date),
    CONSTRAINT fk_customers_salary_day
        FOREIGN KEY (monthly_salary_day) REFERENCES prod.date_dim(date)
);

CREATE TABLE IF NOT EXISTS accounts (
    account_number    VARCHAR(32) PRIMARY KEY,
    customer_id       INTEGER NOT NULL,
    initial_balance   NUMERIC(18,2) NOT NULL DEFAULT 0,
    overdraft_limit   NUMERIC(18,2) NOT NULL DEFAULT 0,
    branch_id         INTEGER NOT NULL,
    account_open_date DATE NOT NULL,
    CONSTRAINT fk_accounts_customer
        FOREIGN KEY (customer_id) REFERENCES prod.customers(customer_id),
    CONSTRAINT fk_accounts_branch
        FOREIGN KEY (branch_id)   REFERENCES prod.branches(branch_id),
    CONSTRAINT fk_accounts_open_date
        FOREIGN KEY (account_open_date) REFERENCES prod.date_dim(date)
);

-- =========================================================
-- 5) FACT: Transactions (Partitioned by RANGE on transaction_datetime)
--    PK must include the partition key in PostgreSQL
-- =========================================================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id            VARCHAR(64) NOT NULL,
    transaction_datetime      DATE        NOT NULL,  -- partition key
    account_number            VARCHAR(32) NOT NULL,
    merchant_id               VARCHAR(64),
    customer_id               INTEGER     NOT NULL,
    channel_id                INTEGER     NOT NULL,
    transaction_type_id       INTEGER     NOT NULL,
    amount                    NUMERIC(18,2) NOT NULL,
    currency                  VARCHAR(8)  NOT NULL,
    is_recurring              BOOLEAN NOT NULL DEFAULT FALSE,
    is_salary                 BOOLEAN NOT NULL DEFAULT FALSE,
    previous_balance          NUMERIC(18,2),
    balance_after_transaction NUMERIC(18,2),

    CONSTRAINT pk_transactions PRIMARY KEY (transaction_id, transaction_datetime),

    CONSTRAINT fk_tx_date
        FOREIGN KEY (transaction_datetime) REFERENCES prod.date_dim(date),
    CONSTRAINT fk_tx_account
        FOREIGN KEY (account_number)       REFERENCES prod.accounts(account_number),
    CONSTRAINT fk_tx_merchant
        FOREIGN KEY (merchant_id)          REFERENCES prod.merchants(merchant_id),
    CONSTRAINT fk_tx_customer
        FOREIGN KEY (customer_id)          REFERENCES prod.customers(customer_id),
    CONSTRAINT fk_tx_channel
        FOREIGN KEY (channel_id)           REFERENCES prod.channels(channel_id),
    CONSTRAINT fk_tx_type
        FOREIGN KEY (transaction_type_id)  REFERENCES prod.transaction_types(transaction_type_id)
) PARTITION BY RANGE (transaction_datetime);

-- Default catch-all partition (for out-of-range data)
CREATE TABLE IF NOT EXISTS transactions_default
    PARTITION OF transactions DEFAULT;

-- Monthly partitions (adjust the range as needed)
DO $$
DECLARE
    start_date DATE := DATE '2023-01-01';
    end_date   DATE := DATE '2027-01-01'; -- exclusive upper bound
    d DATE;
BEGIN
    d := start_date;
    WHILE d < end_date LOOP
        EXECUTE format($f$
            CREATE TABLE IF NOT EXISTS transactions_%s
            PARTITION OF transactions
            FOR VALUES FROM (%L) TO (%L);
        $f$, to_char(d, 'YYYY_MM'), d, (d + INTERVAL '1 month')::date);
        d := (d + INTERVAL '1 month')::date;
    END LOOP;
END$$;

-- Partitioned indexes on transactions (created on parent)
CREATE INDEX IF NOT EXISTS idx_tx_account   ON transactions (account_number);
CREATE INDEX IF NOT EXISTS idx_tx_customer  ON transactions (customer_id);
CREATE INDEX IF NOT EXISTS idx_tx_merchant  ON transactions (merchant_id);
CREATE INDEX IF NOT EXISTS idx_tx_channel   ON transactions (channel_id);
CREATE INDEX IF NOT EXISTS idx_tx_type      ON transactions (transaction_type_id);
CREATE INDEX IF NOT EXISTS idx_tx_date      ON transactions (transaction_datetime);

-- =========================================================
-- 6) Helpful indexes on other tables
-- =========================================================
CREATE INDEX IF NOT EXISTS idx_branches_city            ON branches(city_id);
CREATE INDEX IF NOT EXISTS idx_merchants_category       ON merchants(category_id);
CREATE INDEX IF NOT EXISTS idx_merchants_city           ON merchants(city_id);
CREATE INDEX IF NOT EXISTS idx_customers_dob            ON customers(date_of_birth);
CREATE INDEX IF NOT EXISTS idx_customers_salary_day     ON customers(monthly_salary_day);
CREATE INDEX IF NOT EXISTS idx_accounts_customer        ON accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_accounts_branch          ON accounts(branch_id);
CREATE INDEX IF NOT EXISTS idx_accounts_open_date       ON accounts(account_open_date);
CREATE INDEX IF NOT EXISTS idx_channels_create_date     ON channels(create_date);
CREATE INDEX IF NOT EXISTS idx_channels_update_date     ON channels(update_date);
CREATE INDEX IF NOT EXISTS idx_txtypes_created_at       ON transaction_types(created_at);
CREATE INDEX IF NOT EXISTS idx_txtypes_updated_at       ON transaction_types(updated_at);
CREATE INDEX IF NOT EXISTS idx_categories_created_at    ON categories(created_at);
CREATE INDEX IF NOT EXISTS idx_categories_updated_at    ON categories(updated_at);

-- =========================================================
-- End of file
-- =========================================================
