import yaml
from sqlalchemy import create_engine, inspect

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

test = DatabaseConnector('db_creds.yaml')
engine = test.init_db_engine(test.read_db_creds())
print(test.list_db_tables(engine))
