CREATE TABLE IF NOT EXISTS DWH_DIM_CUSTOMER_ADDRESSES (
    customer_id VARCHAR(128) PRIMARY KEY,
    address VARCHAR(128),
    postcode VARCHAR(128),
    state VARCHAR(128),
    country VARCHAR(128),
    property_valuation INTEGER,
    create_dt DATETIME DEFAULT (DATETIME('now')),
    update_dt DATETIME DEFAULT (DATETIME('now'))
);

CREATE TRIGGER IF NOT EXISTS set_create_dt_addresses
AFTER INSERT ON DWH_DIM_CUSTOMER_ADDRESSES
BEGIN
    UPDATE DWH_DIM_CUSTOMER_ADDRESSES
    SET create_dt = DATETIME('now')
    WHERE customer_id = NEW.customer_id;
END;

CREATE TRIGGER IF NOT EXISTS set_update_dt_addresses
AFTER UPDATE ON DWH_DIM_CUSTOMER_ADDRESSES
BEGIN
    UPDATE DWH_DIM_CUSTOMER_ADDRESSES
    SET update_dt = DATETIME('now')
    WHERE customer_id = NEW.customer_id;
END;