import pandas as pd
import tabula
import requests
import boto3

class DataExctractor:

    #takes an instance of DatabaseConnector and name of chosen table, returns extracted data as pd df
    def read_rds_table(self, database_connector, table_name):
        credentials = database_connector.read_db_creds('db_creds.yaml')
        engine = database_connector.init_db_engine(credentials)
        tables = database_connector.list_db_tables(engine)
        if table_name in tables:
            table = pd.read_sql_table(f'{table_name}', engine)
            return table
        else:
            print('Incorrect table name')
    
    #retrieves data from pdf in s3 bucket, returns pd df
    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, pages='all')
        df = pd.concat(dfs)
        return df
    
    #uses API key and endpoint to extract the total number of stores
    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        if response.status_code == 200:
            num_stores = response.json()
            num_stores = num_stores['number_stores']
            return num_stores
        else:
            return f'Error, status code {response.status_code}'

    #retrieves data from each store in sequence, concats into one df    
    def retrieve_stores_data(self, endpoint, num_stores, header):
        all_store_data_df = pd.DataFrame()
        for _ in range(num_stores):
            response = requests.get(endpoint + str(_), headers=header)
            if response.status_code == 200:
                store_data = response.json()
                store_data_df = pd.DataFrame([store_data])
                all_store_data_df = pd.concat([all_store_data_df, store_data_df], ignore_index=True)
            else:
                return f'Error, status code {response.status_code}'
        return all_store_data_df
    
    #uses boto3 to download csv file from s3 bucket, then converts to pd df
    def extract_from_s3(self, link):
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'products.csv', '/Users/Alex/MULTINATIONAL_RETAIL_DATA_CENTRALISATION/products.csv')
        product_df = pd.read_csv('products.csv')
        return product_df
    
    def extract_sale_details(self, link):
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'date_details.json', '/Users/Alex/MULTINATIONAL_RETAIL_DATA_CENTRALISATION/date_details.json')
        product_df = pd.read_json('date_details.json')
        return product_df
