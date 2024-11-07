import numpy as np
import pandas as pd
import os
import google.generativeai as genai
from difflib import SequenceMatcher

class ColumnFilter:
    def __init__(
            self, 
            dfs:list[str],
            api_key:str, 
            model=genai.GenerativeModel("gemini-pro")
        ):
        genai.configure(api_key=api_key)
        self.df_names = dfs
        self.__dfs = {name: pd.read_csv(name) for name in dfs}
        self.df_count = len(dfs)
        self.model = model
        self.__target_tables = {
            'Customer':['CustomerId', 'FullName', 'Email', 'Phone', 'State', 'City', 'Gender', 
                        'Occupation', 'IncomeLevel'],
            'Order':['OrderId', 'CustomerId', 'OrderDate'],
            'Sales':['SaleId', 'TransactionDate', 'TransactionTime', 'InvoiceNum', 
                     'SalesChannel', 'QuantitySold', 'TotalAmount', 'CustomerId', 'ProductId'],
            'Purchase':['PurchaseId', 'ProductId', 'PurchaseDate', 'QuantityPurchased', 
                        'PurchasePrice', 'SupplierName', 'SupplierContactInfo'],
            'Product':['ProductId', 'ProductName', 'SKU', 'Price', 'DiscountedPrice', 'BrandId', 
                       'CategoryId'],
            'Category':['CategoryId', 'CategoryName'],
            'Brand':['BrandId', 'BrandName']
        }
    
    # Retrieve column names of the table
    def __get_cols(self)->list:
        columns = []
        for df_name, df in self.__dfs.items():
            lists = df.columns.tolist()
            columns.append((df_name, len(lists), str(lists)))
        return columns
    
    # Gnerate the prompt for column filtering
    def __gen_prompt(self, columns:list) -> str:
        text1 = '''
        We are reading data from a spreadsheet into our database.
        Our database has 7 tables in total:
        * Customer table contains columns:
            + CustomerId (pk): unique id of a customer
            + FullName: full name of the customer
            + Email: email address of the customer
            + Phone: phone number of the customer
            + State: state win which the customer lives
            + City: city win which the customer lives
            + Gender: gender of the customer
            + Occupation: occupation of the customer
            + IncomeLevel: income group the customer belongs to
        * Order table contains columns:
            + OrderId (pk): unique id of an order
            + CustomerId: id of the customer who made the order
            + OrderDate: date when the order is made
        * Sales table contains columns:
            + SaleId (pk): unique id of a sale
            + TransactionDate: date when the payment is made
            + TransactionTime: time when the payment is made
            + InvoiceNum: number of the invoice to the sale
            + SalesChannel: How the sale was made
            + QuantitySold: the quantity of product included in the sale
            + TotalAmount: total price of the sale
            + CustomerId: id of the customer who generates the sale
            + ProductId: id of the product in the sale
        * Purchase table contains columns:
            + PurchaseId (pk): unique id of a purchase
            + ProductId: id of the product purchased
            + PurchaseDate: date when the product is purchased
            + QuantityPurchased: the quantity of product included in the purchase
            + PurchasePrice: unit cost of the product purchased
            + SupplierName: name of the supplier of the product
            + SupplierContactInfo: contact information of the supplier
        * Product table contains columns:
            + ProductId (pk): unique id of the product
            + ProductName: name of the product
            + SKU: a unique code that identifies a product in a business's inventory
            + Price: unit price of the product
            + DiscountedPrice: unit price on-sale
            + BrandId: id of the brand the product belongs to
            + CategoryId: id of the category the product belongs to
        * Category table contains columns:
            + CategoryId (pk): unique id of the category
            + CategoryName: name of the category
        * Brand table contains columns:
            + BrandId (pk): unique id of the brand
            + BrandName: name of the brand
        We have detected spreadsheets:\n
        '''

        text2 = ''
        for i in range(self.df_count):
            text2 += f'''
            {columns[i][0]} has {columns[i][1]} columns: {columns[i][2]}.\n
            '''
        
        text3 = '''
        select only the spreadsheet columns that you think matches the database table columns.
        output the selected column names from spreadsheets STRICTLY in the form of (exclude "" in your response):
        "Customer:[col1, col2, ...]
        Order:[col1, col2, ...]
        ...
        Brand:[col1, col2, ...]" 
        Requirements:
            * replace the colname you did not find by "null"
            * column count must match
            * only include column names you found in spreadsheet columns
            * do not include column names database tables if you cannot find it in spreadsheet columns
            * do not create new colnames! 
            * colnames must in the same order as listed above both within each table and between tables
            * do not include space between ":" and "[" in your response!
        '''
        return text1 + text2 + text3

    # Prompt to the LLM
    def __ask(self, prompt:str) -> str:
        response = self.model.generate_content(contents=prompt)
        return response.text
    
    # Column extraction
    def select_col(self)->dict[str, list[str]]:
        prompt = self.__gen_prompt(self.__get_cols())
        cols_in_str = self.__ask(prompt)
        cols = {}
        # Split by lines and parse each line
        for line in cols_in_str.strip().splitlines():
            key, values = line.split(':')
            values_list = values.strip('[]').split(', ')
            #values_list = [None if v == 'null' else v for v in values_list]
            cols[key] = values_list
        return cols
    
    # Output the filtered data
    def __store_tables(self, tables:dict[str, pd.DataFrame]):
        os.makedirs('filtered_data', exist_ok=True)
        for tb_name, tb in tables.items():
            tb.to_csv('filtered_data/'+tb_name+'.csv')

    # Change dictionary into dataframe
    def __dict_to_df(self, dictionary:dict[str, pd.DataFrame])->pd.DataFrame:
        table = {'table':[], 'colname':[]}
        for k, v in dictionary.items():
            for i in v.columns:
                table['table'].append(k)
                table['colname'].append(i)
        return pd.DataFrame(table).set_index('colname')
    
    # Compare when duplicate columns
    def __compare_node(self, benchmark:str, candidates:list[str])->str:
        winner = ''
        overlap_percent = 0
        for cand in candidates:
            matcher = SequenceMatcher(None, cand, benchmark)
            op = matcher.ratio()
            if op > overlap_percent:
                overlap_percent = op
                winner = cand
        return winner

    # Filter data
    def filter_cols(self, local:bool=True):
        # New columns in new tables
        selected_cols = self.select_col()

        # Old columns in old tables
        old_dfs = self.__dict_to_df(self.__dfs)

        result_set = {}
        for df_name, columns in selected_cols.items():
            null_count = 1
            this_df = pd.DataFrame()
            for colname in columns:
                # Manually adjust for LLM bias
                if colname not in old_dfs.index:
                    colname = 'null'

                if colname == 'null':
                    this_df['null'+str(null_count)] = np.nan
                    null_count+=1
                else:
                    matching = old_dfs.loc[colname, 'table']
                    if type(matching) != str:
                        matching = self.__compare_node(df_name, matching.tolist())
                    this_df[colname] = self.__dfs[matching][colname]
            result_set[df_name] = this_df

        if local:
            self.__store_tables(result_set)