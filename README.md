# multinational-retail-data-centralisation

The aim of this project is to gather sales data that is spread across many different data sources and make it accessible from one centralised location, that can then be queried to produce up to date metrics for the business.

There are three main classes DataExtractor that extracts the sales data from the various sources, DataCleaning which cleans the data, and DatabaseConnector that uploads the cleaned data to a centralised Postgresql database. The upload data is then further cleaned, and cast into the correct data types. This allows the creation of a database schema so the data can be queried to produced metrics.

This project requires:
Pandas,
Dateutil,
Tabula,
Requests,
Boto3,
Yaml,
Sqlalchemy.

To extract, clean and upload the data run the main.py file. 

Then use the sql_data_cleaning.sql file to ensure the uploaded data is correct and create the database schema.
