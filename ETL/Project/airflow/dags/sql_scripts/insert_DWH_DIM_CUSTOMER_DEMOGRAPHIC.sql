INSERT OR IGNORE INTO DWH_DIM_CUSTOMERS_DEMOGRAPHIC (
    customer_id, first_name, last_name, gender,
    past_3_years_bike_related_purchases, DOB, job_title,
    job_industry_category, wealth_segment, deceased_indicator,
    owns_car, tenure
)
SELECT
    customer_id, first_name, last_name, gender,
    past_3_years_bike_related_purchases, DOB, job_title,
    job_industry_category, wealth_segment, deceased_indicator,
    owns_car, tenure
FROM STG_CUSTOMERS_DEMOGRAPHICS
UNION ALL
SELECT
    customer_id, first_name, last_name, gender,
    past_3_years_bike_related_purchases, DOB, job_title,
    job_industry_category, wealth_segment, deceased_indicator,
    owns_car, tenure
FROM STG_NEW_CUSTOMERS_LIST;