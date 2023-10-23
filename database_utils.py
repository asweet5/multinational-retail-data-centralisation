import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

class DatabaseConnector:
    def __init__(self, creds):
        self.creds = creds

    def read_db_creds(self):
        with open(self.creds, 'r') as creds:
            data = yaml.safe_load(creds)
            return data

    def init_db_engine(self, data):
        HOST = data['RDS_HOST']
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        DATABASE = data['RDS_DATABASE']
        PORT = data['RDS_PORT']
        engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, clean_data_df, df_name):
        pass
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'Fender121'
        DATABASE = 'sales_data'
        PORT = 5432
        engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        clean_data_df.to_sql(f'{df_name}', engine, if_exists='replace')



#'legacy_store_details', 'legacy_users', 'orders_table'
connect_aws_db = DatabaseConnector('db_creds.yaml')
exctract_users_aws = DataExtractor(connect_aws_db, 'legacy_users')
rds_table_users = exctract_users_aws.read_rds_table()
rds_table_users = DataCleaning(rds_table_users)
clean_rds_table_users = rds_table_users.clean_user_data()
connect_aws_db.upload_to_db(clean_rds_table_users, 'dim_users')
card_df = exctract_users_aws.retireve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
clean_card_df = DataCleaning(card_df)
clean_card_df = clean_card_df.clean_card_data()
connect_aws_db.upload_to_db(clean_card_df, 'dim_card_details')
