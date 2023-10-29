INSERT OR IGNORE INTO DWH_DIM_CUSTOMER_ADDRESSES (
    customer_id, address, postcode, state, country,
    property_valuation
)
SELECT
    customer_id, address, postcode, state, country,
    property_valuation
FROM STG_CUSTOMER_ADDRESS
UNION ALL
SELECT
    customer_id, address, postcode, state, country,
    property_valuation
FROM STG_NEW_CUSTOMERS_LIST;