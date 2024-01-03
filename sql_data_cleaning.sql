/* Cast columns in order table */
    /* removes level_0 column */
    ALTER TABLE orders_table
    DROP COLUMN IF EXISTS level_0;

    /* changes card number from big int to text */
    ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE text;

    /* finds max length of store code and product code*/
    SELECT max(length(store_code)) AS max_store_code, max(length(product_code)) AS max_product_code, max(length(card_number)) As max_card_number
    FROM orders_table;

    /* Changes remaining columns in orders table to the correct data types*/
    ALTER TABLE orders_table
    ALTER COLUMN store_code TYPE varchar(12), 
    ALTER COLUMN product_code TYPE varchar(11), 
    ALTER COLUMN card_number TYPE varchar(19),
    ALTER COLUMN product_quantity TYPE smallint,
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid, ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

/* Cast columns in dim_users table */
    /* finds max length of country code*/
    SELECT max(length(country_code)) AS max_country_code
    FROM dim_users;

    /* Deletes inavlid data */
    DELETE FROM dim_users
    WHERE LENGTH(user_uuid) < 36

    /* changes first name and last name to varchar (255) and country code to varchar (10), dates to type date and user uuid to type uuid */
    ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE varchar(255), 
    ALTER COLUMN last_name TYPE varchar(255), 
    ALTER COLUMN country_code TYPE varchar(10),
    ALTER COLUMN date_of_birth TYPE date,
    ALTER COLUMN join_date TYPE date,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;


/* Cast columns in dim_store_details table */
    /* Merges latidtude and lat columns, deletes lat column */
    UPDATE dim_store_details
    SET latitude = CONCAT(latitude, lat);

    ALTER TABLE dim_store_details
    DROP COLUMN lat; 

    /* Cleans table */
    DELETE 
    FROM dim_store_details
    WHERE continent ~ '[0-9]' OR continent = 'NULL';

    /* removes letters from staff_numbers column */
    UPDATE dim_store_details
    SET staff_numbers = REGEXP_REPLACE(staff_numbers, '[^0-9]', '', 'g')
    WHERE staff_numbers ~ '[^0-9]';

    /* finds max length of country code and store code*/
    SELECT max(length(store_code)) AS max_store_code, max(length(country_code)) AS max_country_code
    FROM dim_store_details;

    /* changes columns to correct data types */
    ALTER TABLE dim_store_details
    ALTER COLUMN store_code TYPE varchar(12), 
    ALTER COLUMN country_code TYPE varchar(2),
    ALTER COLUMN locality TYPE varchar(255), 
    ALTER COLUMN continent TYPE varchar(255), 
    ALTER COLUMN store_type TYPE varchar(255),
    ALTER COLUMN staff_numbers TYPE smallint USING staff_numbers::smallint,
    ALTER COLUMN opening_date TYPE date;


    /* changes lat and long to float */
    UPDATE dim_store_details
    SET longitude = CASE
        WHEN longitude = 'N/A' THEN NULL
        ELSE CAST(longitude AS float)
    END;

    UPDATE dim_store_details
    SET latitude = CASE
        WHEN latitude = 'N/A' THEN NULL
        ELSE CAST(latitude AS float)
    END;

    ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE float USING longitude::float, 
    ALTER COLUMN latitude TYPE float USING latitude::float;

/* Cast columns in dim_products table */
    /* Removes £ from proudct price */
    UPDATE dim_products
    SET product_price = REPLACE(product_price, '£', '')

    /*Creates and populates weight class column */
    ALTER TABLE dim_products
    ADD COLUMN weight_class varchar(20)

    UPDATE dim_products
    SET weight_class = 
    CASE 
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_required'
    END

    /* Cleans price column, changes price and product type to float */
    UPDATE dim_products
    SET product_price = REGEXP_REPLACE(product_price, '[^0-9.]', '', 'g')
    WHERE product_price ~ '[0-9.]'

    /* Finds max length of variable stings */
    SELECT MAX(length("EAN")) AS max_EAN, MAX(LENGTH(product_code)) AS max_product_code
    FROM dim_products

    /* Updates dates in invalid formats */
    UPDATE dim_products
    SET date_added = '2018-10-22'
    WHERE index = 307;

    UPDATE dim_products
    SET date_added = '2017-09-06'
    WHERE index = 1217;

    DELETE FROM dim_products
    WHERE date_added ~ '[^0-9-]';

    /* changes columns to correct data types */
    ALTER TABLE dim_products
    ALTER COLUMN product_code TYPE varchar(11), 
    ALTER COLUMN "EAN" TYPE varchar(17),
    ALTER COLUMN date_added TYPE date USING date_added::date, 
    ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
    ALTER COLUMN product_price TYPE float USING product_price::float, 
    ALTER COLUMN weight TYPE float;

    /* Changes column name to still available, alters values to True or False, then sets type to boolean */
    ALTER TABLE dim_products
    RENAME COLUMN removed TO still_available;

    UPDATE dim_products
    SET still_available = 
    CASE
    WHEN still_available = 'Still_avaliable' THEN TRUE 
    WHEN still_available = 'Removed' THEN FALSE
    END;

    ALTER TABLE dim_products
    ALTER COLUMN still_available TYPE boolean USING still_available::boolean

/* Casting dim_date_times table */
    /* Finds max length of variable stings */
    SELECT MAX(LENGTH(month)) as length_month, 
    MAX(LENGTH(year)) as length_year, 
    MAX(LENGTH(day)) as length_day, 
    MAX(LENGTH(time_period)) as length_time_period 
    FROM dim_date_times;

    /* Cleans invalid data */
    DELETE FROM dim_date_times
    WHERE LENGTH(month) > 2;

    /* changes columns to correct data types */
    ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE varchar(2), 
    ALTER COLUMN year TYPE varchar(4), 
    ALTER COLUMN day TYPE varchar(2), 
    ALTER COLUMN time_period TYPE varchar(10),
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
    
/* Casting dim_card_details table */
    /* Finds max length of variable stings */
    SELECT MAX(LENGTH(card_number)) as max_card_number, 
    MAX(LENGTH(expiry_date)) AS max_expiry 
    from dim_card_details;

    /* Cleans invalid card numbers */
    UPDATE dim_card_details
    SET card_number = REPLACE(card_number, '?', '');

    DELETE FROM dim_card_details
    WHERE card_number ~ '[^0-9]';

    /* changes columns to correct data types */
    ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE varchar(19), 
    ALTER COLUMN expiry_date TYPE varchar(10), 
    ALTER COLUMN date_payment_confirmed TYPE date;

/* Create primary keys */
ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);

/* Create foreign keys */
ALTER TABLE orders_table ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
ALTER TABLE orders_table ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
ALTER TABLE orders_table ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
ALTER TABLE orders_table ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
ALTER TABLE orders_table ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

