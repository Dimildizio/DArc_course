INSERT INTO DWH_DataMart (
    transaction_id, transaction_date, product_id, customer_id, online_order,
    order_status, brand, product_line, product_class, product_size,
    list_price, standard_cost, product_first_sold_date, first_name, last_name,
    gender, past_3_years_bike_related_purchases, DOB, job_title,
    job_industry_category, wealth_segment, deceased_indicator, owns_car, tenure
)
SELECT
   F.transaction_id, F.transaction_date, F.product_id, F.customer_id, F.online_order,
   F.order_status, F.brand, F.product_line, F.product_class, F.product_size,
   F.list_price, F.standard_cost, F.product_first_sold_date, C.first_name, C.last_name,
   C.gender, C.past_3_years_bike_related_purchases, C.DOB, C.job_title,
   C.job_industry_category, C.wealth_segment, C.deceased_indicator, C.owns_car, C.tenure
FROM
   DWH_FACT_TRANSACTIONS F
   LEFT JOIN DWH_DIM_CUSTOMERS_DEMOGRAPHIC C ON F.customer_id = C.customer_id
   LEFT JOIN DWH_DIM_CUSTOMER_ADDRESSES A ON F.customer_id = A.customer_id;
