INSERT OR IGNORE INTO DWH_FACT_TRANSACTIONS (
    transaction_id, transaction_date, product_id, customer_id,
    online_order, order_status, brand, product_line,
    product_class, product_size, list_price, standard_cost,
    product_first_sold_date
)
SELECT
    transaction_id, transaction_date, product_id, customer_id,
    online_order, order_status, brand, product_line,
    product_class, product_size, list_price, standard_cost,
    product_first_sold_date
FROM STG_TRANSACTIONS;