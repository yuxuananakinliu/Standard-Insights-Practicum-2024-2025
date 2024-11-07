import pandas as pd
from typing import Literal

class DataCleaner:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def handle_missing_data(self, strategy:str='drop', column:list[str]=None, value:list=None):
        if strategy == 'drop':
            self.df = self.df.dropna(subset=column) if column else self.df.dropna()
        elif strategy == 'fill':
            for col, val in zip(column, value):
                self.df[col] = self.df[col].fillna(val)
        return self

    def correct_inconsistent_data(self, column:str, mapping_dict:dict):
        self.df[column] = self.df[column].str.lower()
        self.df[column] = self.df[column].replace(mapping_dict)
        return self

    def correct_data_types(self, column:str, dtype:Literal['datetime', 'category', 'numeric']):
        if dtype == 'datetime':
            self.df[column] = pd.to_datetime(self.df[column])
        elif dtype == 'category':
            self.df[column] = self.df[column].astype('category')
        elif dtype == 'numeric':
            self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        return self

    def handle_duplicates(self, column:list[str]=None):
        self.df = self.df.drop_duplicates(subset=column) if column else self.df.drop_duplicates()
        return self

    def handle_outliers(self, column:str, method:str='iqr'):
        if method == 'iqr':
            Q1 = self.df[column].quantile(0.25)
            Q3 = self.df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            self.df = self.df[~((self.df[column] < lower_bound) | (self.df[column] > upper_bound))]
        elif method == 'clip':
            lower_cap = self.df[column].quantile(0.05)
            upper_cap = self.df[column].quantile(0.95)
            self.df[column] = self.df[column].clip(lower_cap, upper_cap)
        return self

    def get_cleaned_data(self):
        return self.df

    def save_to_csv(self, filename:str, path:str=None):
        if path:
            self.df.to_csv(path+filename, index=False)
        else:
            self.df.to_csv(filename, index=False)
