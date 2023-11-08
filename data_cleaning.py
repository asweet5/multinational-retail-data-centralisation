import pandas as pd
from dateutil import parser

class DataCleaning:
    
    def clean_user_data(self, data):
        data = data.sort_values(by=['index'])
        data = data.dropna()
        data.first_name = data.first_name.astype('string')
        data.last_name = data.last_name.astype('string')
        data['date_of_birth'] = data['date_of_birth'].apply(self.parse_date)
        data['date_of_birth'] = pd.to_datetime(data['date_of_birth'])
        data['join_date'] = data['join_date'].apply(self.parse_date)
        data['join_date'] = pd.to_datetime(data['join_date'])
        return data
    
    def clean_card_data(self, data):
        data = data.dropna()
        data['date_payment_confirmed'] = data['date_payment_confirmed'].apply(self.parse_date)
        data['date_payment_confirmed'] = pd.to_datetime(data['date_payment_confirmed'])
        return data
    
    def clean_store_data(self, data):
        data['opening_date'] = data['opening_date'].apply(self.parse_date)
        data['opening_date'] = pd.to_datetime(data['opening_date'])
        return data
    
    def convert_product_weights(self, weight):
        weight = weight.replace(' ', '')
        if 'x' in weight:
            parts = weight.split('x')
            quantity = float(parts[0].strip())
            unit = parts[1].strip()
            if 'kg' in unit:
                unit = float(unit.replace('kg', ''))
                return quantity * unit
            elif 'g' in unit:
                unit = float(unit.replace('g', ''))
                return (quantity * unit) / 1000
            elif 'ml' in unit:
                unit = float(unit.replace('ml', ''))
                return (quantity * unit) / 1000
        elif 'kg' in weight:
            return float(weight.replace('kg', '').strip())
        elif 'g' in weight:
            return float(weight.replace('g', '').strip()) / 1000
        elif 'ml' in weight:
            return float(weight.replace('ml', '').strip()) / 1000
        
    def clean_products_data(self, data):
        data['weight'] = data['weight'].astype(str)
        data['weight'] = data['weight'].apply(self.convert_product_weights)
        data['weight'].fillna(0, inplace=True)
        data = data.dropna()
        return data
    
    def clean_orders_data(self, data):
        data = data.drop('first_name', axis=1)
        data = data.drop('last_name', axis=1)
        data = data.drop('1', axis=1)
        data = data.drop('level_0', axis=1)
        return data
    
    def clean_date_details(self, data):
        data = data.dropna()
        return data

    
    def parse_date(self, date_str):
        try:
            return parser.parse(date_str).strftime('%Y-%m-%d')
        except:
            return None
        
