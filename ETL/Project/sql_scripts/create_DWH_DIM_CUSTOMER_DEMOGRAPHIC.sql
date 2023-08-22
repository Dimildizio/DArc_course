CREATE TABLE IF NOT EXISTS DWH_DIM_CUSTOMERS_DEMOGRAPHIC (
    customer_id VARCHAR(128) PRIMARY KEY,
    first_name VARCHAR(128),
    last_name VARCHAR(128),
    gender VARCHAR(128),
    past_3_years_bike_related_purchases INTEGER,
    DOB DATE,
    job_title VARCHAR(128),
    job_industry_category VARCHAR(128),
    wealth_segment VARCHAR(128),
    deceased_indicator VARCHAR(128),
    owns_car VARCHAR(128),
    tenure INTEGER,
    create_dt DATETIME DEFAULT (DATETIME('now')),
    update_dt DATETIME DEFAULT (DATETIME('now'))
);

CREATE TRIGGER IF NOT EXISTS set_create_dt_demograptic
AFTER INSERT ON DWH_DIM_CUSTOMERS_DEMOGRAPHIC
BEGIN
    UPDATE DWH_DIM_CUSTOMERS_DEMOGRAPHIC
    SET create_dt = DATETIME('now')
    WHERE customer_id = NEW.customer_id;
END;

CREATE TRIGGER IF NOT EXISTS set_update_dt_demographic
AFTER UPDATE ON DWH_DIM_CUSTOMERS_DEMOGRAPHIC
BEGIN
    UPDATE DWH_DIM_CUSTOMERS_DEMOGRAPHIC
    SET update_dt = DATETIME('now')
    WHERE customer_id = NEW.customer_id;
END;