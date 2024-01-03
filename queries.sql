/* How many stores does the business have and in which counties */
SELECT 
    DISTINCT(country_code) AS country, 
    COUNT(country_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;

/* Which locations currently have the most stores */
SELECT 
    DISTINCT(locality), 
    COUNT(store_code)
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    count DESC;

/* Which months produces the largest amount of sales */
SELECT 
    dim_date_times.month, 
    ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales
FROM 
    orders_table
JOIN 
    dim_products
ON 
    orders_table.product_code = dim_products.product_code
JOIN 
    dim_date_times
ON 
    orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY 
    dim_date_times.month
ORDER BY 
    total_sales DESC;

/* How many sales are coming from online */
SELECT 
    COUNT(orders_table.product_quantity) AS number_of_sales, 
    SUM(orders_table.product_quantity) AS product_quantity_count,
    CASE 
    WHEN dim_store_details.store_type  =  'Web Portal' THEN 'Web'
    ELSE 'Offline'
    END
    AS location
FROM 
    orders_table
JOIN 
    dim_store_details
ON 
    orders_table.store_code = dim_store_details.store_code
GROUP BY 
    location;

/* What percentage of sales come through each type of store */
SELECT
    dim_store_details.store_type AS store_type,
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
    ROUND(((SUM(orders_table.product_quantity * dim_products.product_price) / SUM(SUM(orders_table.product_quantity * dim_products.product_price)) OVER ()) * 100)::numeric, 2) AS percentage_total
FROM 
    orders_table
JOIN 
    dim_store_details
ON 
    orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products
ON 
    orders_table.product_code = dim_products.product_code
GROUP BY 
    store_type
ORDER BY 
    total_sales DESC;

/* Which month in each year produced the highest cost of sales */
WITH MonthlySales AS (
    SELECT
        ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
        dim_date_times.year AS year,
        dim_date_times.month AS month
    FROM
        orders_table
    JOIN
        dim_products
    ON
        orders_table.product_code = dim_products.product_code
    JOIN
        dim_date_times
    ON
        orders_table.date_uuid = dim_date_times.date_uuid
    GROUP BY
        month,
        year
)
SELECT
    total_sales,
    year,
    month
FROM (
    SELECT
        year,
        month,
        total_sales,
        RANK() OVER (PARTITION BY year ORDER BY total_sales DESC) as sales_rank
    FROM
        MonthlySales
) AS RankedSales
WHERE
    sales_rank = 1
ORDER BY
    total_sales DESC;

/* What is our staff headcount */
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY 
    country_code
ORDER BY
    total_staff_numbers DESC;

/* Which German store type is selling the most */
SELECT
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) as total_sales,
    dim_store_details.store_type,
    dim_store_details.country_code
FROM 
    orders_table
JOIN
    dim_products
ON
    orders_table.product_code = dim_products.product_code
JOIN
    dim_store_details
ON 
    orders_table.store_code = dim_store_details.store_code
WHERE
    country_code = 'DE'
GROUP BY
    store_type,
    country_code
ORDER BY
    total_sales;

/* How quickly is the company making sales */
SELECT 
    year,
    AVG(time_interval) AS actual_time_taken
FROM (
    SELECT
        year,
        date,
        LEAD(date, 1) OVER (ORDER BY date) - date AS time_interval
    FROM (
        SELECT
            (year || '-' || month || '-' || day || ' ' || timestamp)::TIMESTAMP AS date,
            year
        FROM
            dim_date_times
    ) AS date_time
    ORDER BY
        date
) AS time_between_sales
GROUP BY
    year
ORDER BY
    actual_time_taken DESC;


