import pandas as pd
from dateutil import parser

class DataCleaning:
    def __init__(self, extracted_data):
        self.extracted_data = extracted_data

    def clean_user_data(self):
        table = self.extracted_data
        table = table.sort_values(by=['index'])
        table = table.dropna()
        table.first_name = table.first_name.astype('string')
        table.last_name = table.last_name.astype('string')
        table['date_of_birth'] = table['date_of_birth'].apply(self.parse_date)
        table['date_of_birth'] = pd.to_datetime(table['date_of_birth'])
        table['join_date'] = table['join_date'].apply(self.parse_date)
        table['join_date'] = pd.to_datetime(table['join_date'])
        return table
    
    def clean_card_data(self):
        card_df = self.extracted_data
        card_df = card_df.dropna()
        card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].apply(self.parse_date)
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'])
        return card_df

    def parse_date(self, date_str):
        try:
            return parser.parse(date_str).strftime('%Y-%m-%d')
        except:
            return None
        
    