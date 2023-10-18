import yaml

class DatabaseConnector:
    def __init__(self, creds):
        self.creds = creds

    def read_db_creds(self):
        with open(self.creds, 'r') as creds:
            data = yaml.safe_load(creds)
            return data
        
