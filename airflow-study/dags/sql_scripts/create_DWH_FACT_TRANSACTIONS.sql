CREATE TABLE IF NOT EXISTS DWH_FACT_TRANSACTIONS (
    transaction_id INTEGER PRIMARY KEY,
    transaction_date DATE,
    product_id VARCHAR(128),
    customer_id VARCHAR(128),
    online_order VARCHAR(128),
    order_status VARCHAR(128),
    brand VARCHAR(128),
    product_line VARCHAR(128),
    product_class VARCHAR(128),
    product_size VARCHAR(128),
    list_price REAL,
    standard_cost REAL,
    product_first_sold_date DATE,
    create_dt DATETIME DEFAULT (DATETIME('now')),
    update_dt DATETIME DEFAULT (DATETIME('now'))
);

CREATE TRIGGER IF NOT EXISTS set_create_dt_transactions
AFTER INSERT ON DWH_FACT_TRANSACTIONS
BEGIN
    UPDATE DWH_FACT_TRANSACTIONS
    SET create_dt = DATETIME('now')
    WHERE transaction_id = NEW.transaction_id;
END;

CREATE TRIGGER IF NOT EXISTS set_update_dt_transactions
AFTER UPDATE ON DWH_FACT_TRANSACTIONS
BEGIN
    UPDATE DWH_FACT_TRANSACTIONS
    SET update_dt = DATETIME('now')
    WHERE transaction_id = NEW.transaction_id;
END;
