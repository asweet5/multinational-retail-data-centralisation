import yaml
from sqlalchemy import create_engine, inspect
from data_extraction import DataExctractor
from data_cleaning import DataCleaning
import api_info


class DatabaseConnector:

    #takes file name and reads database credentials from yaml file, returns dictionary of credentials
    def read_db_creds(self, creds_file):
        with open(creds_file, 'r') as creds:
            creds = yaml.safe_load(creds)
            return creds

    #takes credentials dictionary, intitialises and returns a sqlalchemy engine     
    def init_db_engine(self, creds):
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']
        engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    #uses the engine to access table names, returns table names
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, clean_data_df, df_name):
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'Test123'
        DATABASE = 'sales_data'
        PORT = 5432
        engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        clean_data_df.to_sql(f'{df_name}', engine, if_exists='replace')
    
database_connector = DatabaseConnector()
data_extractor = DataExctractor()
data_cleaner = DataCleaning()


def user_data():
    #connects with aws rds tables, extracts user data as pd df, cleans and uploads
    aws_rds_user_table = data_extractor.read_rds_table(database_connector, 'legacy_users')
    clean_aws_user_df = data_cleaner.clean_user_data(aws_rds_user_table)
    database_connector.upload_to_db(clean_aws_user_df, 'dim_users')

def card_data():
    #extracts pdf from s3 bucket as pd df, cleans and uploads
    card_details_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    clean_card_details_df = data_cleaner.clean_card_data(card_details_df)
    database_connector.upload_to_db(clean_card_details_df, 'dim_card_details')

def store_data():
    #uses API to extract total number of stores, then concats dfs of each store, cleans and uploads
    num_stores = data_extractor.list_number_of_stores(api_info.num_stores, api_info.header)
    store_details_df = data_extractor.retrieve_stores_data(api_info.store_info, num_stores, api_info.header)
    clean_store_df = data_cleaner.clean_store_data(store_details_df)
    database_connector.upload_to_db(clean_store_df, 'dim_store_details')

def product_data():
    #extracts csv from s3 bucket, converts to pd df, cleans and uploads    
    product_details_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
    clean_product_details_df = data_cleaner.clean_products_data(product_details_df)
    database_connector.upload_to_db(clean_product_details_df, 'dim_products')

def order_details():
    aws_rds_order_table = data_extractor.read_rds_table(database_connector, 'orders_table')
    clean_aws_order_df = data_cleaner.clean_orders_data(aws_rds_order_table)
    database_connector.upload_to_db(clean_aws_order_df, 'orders_table')

def dim_date_times():
    sales_date_df = data_extractor.extract_sale_details('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    clean_sales_date_df = data_cleaner.clean_date_details(sales_date_df)
    database_connector.upload_to_db(clean_sales_date_df, 'dim_date_times')


