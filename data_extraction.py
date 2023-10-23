import pandas as pd
import tabula

class DataExtractor:
    def __init__(self, databaseconnector, table):
        self.databaseconnector = databaseconnector
        self.table = table
    
    def read_rds_table(self):
        engine = self.databaseconnector.init_db_engine(self.databaseconnector.read_db_creds())
        tables = self.databaseconnector.list_db_tables(engine)
        if self.table in tables:
            table = pd.read_sql_table(f'{self.table}', engine)
            return table
        else:
            print('Incorrect table name')

    def retireve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, pages='all')
        df = pd.concat(dfs)
        return df
    
    def list_number_of_stores(self, num_stores, header):
        pass