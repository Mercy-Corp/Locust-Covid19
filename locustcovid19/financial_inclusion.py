# -*- coding: utf-8 -*-
"""
The aim of this module is to selected financial inclusion indicators per country. Indicators:
* Received wages in the past year (% age 15+)
* Used the internet to buy something online in the past year(% age 15+)
* Used a debit or credit card to make a purchase in the past year (% age 15+)
* Used a mobile phone or the internet to access a financial institution account in the past year (% age 15+)
                           ]
Data source: https://datacatalog.worldbank.org/dataset/global-financial-inclusion-global-findex-database

Created on Mon Sep 21 18:14:40 2020
Latest update Wed Sep 23 10:00:40 2020

@author: ioanna.papachristou@accenture.com
"""

import os
import pandas as pd
import numpy as np
from utils.flat_files import FlatFiles
import yaml

class FinancialInclusion:
    '''
    This class creates the production table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        df = pd.read_csv(self.path_in + '/social_cohesion/financial_inclusion/FINDEX_Data.csv')
        self.fin_inclusion_df = df.iloc[:-5]
        self.flats = FlatFiles(self.path_in, self.path_out)

    def create_measure_df(self):
        '''
        Creates the measures df.
        :return: A dataframe with the 4 measure ids and types we need.
        '''
        # initialize list of lists
        measure_id_data = [[46, 'Received wages in the past year (% age 15+)'],
                           [47, 'Used the internet to buy something online in the past year(% age 15+)'],
                           [48, 'Used a debit or credit card to make a purchase in the past year (% age 15+)'],
                           [49, 'Used a mobile phone or the internet to access a financial institution account in the past year (% age 15+)']
                           ]

        # Create the pandas DataFrame
        measure_df = pd.DataFrame(measure_id_data, columns=['measureID', 'indicator_name'])
        measure_df = measure_df

        return measure_df

    def change_columns_name(self, df):
        '''
        Changes the name of the year columns.
        :param df: The inclusion df.
        :return: A df with the columns named by years.
        '''
        new_columns = []
        for column in df.columns.tolist():
            if "[" in column:
                new_column = column.split(' ')[0]
                new_columns.append(new_column)
            else:
                new_columns.append(column)

        df.columns = new_columns

        return df

    def filter_years(self, df):
        '''

        :param df: The input df.
        :return: A df with the financial inclusion indicators for the existing years.
        '''
        inclusion_all_years = pd.DataFrame()

        for year in range(2000, 2030):
            if str(year) in df.columns:
                inclusion_year = df.copy()
                # Create year column
                inclusion_year['year'] = int(year)
                # Create value column
                inclusion_year['value'] = inclusion_year[str(year)]
                # Filter columns
                inclusion_year = inclusion_year[['Country Code', 'Series Name', 'year', 'value']]
                #  Append
                inclusion_all_years = inclusion_all_years.append(inclusion_year)

        return inclusion_all_years

    def add_ids_to_table(self):
        '''
        Merges with all other tables and extracts all ids.
        :return: The inclusion dataframe with all columns as defined in the data model.
        '''
        # Change column names
        inclusion = self.change_columns_name(self.fin_inclusion_df)

        # Merge inclusion with measure and add measureID
        inclusion = self.filter_years(inclusion).merge(self.create_measure_df(), left_on='Series Name', right_on ='indicator_name', how='left')

        # Create factID
        inclusion['factID'] = 'Fin_' + inclusion.index.astype(str)

        # Add dateID
        inclusion_df = self.flats.add_date_id(inclusion, 'year')

        # Add locationID
        inclusion_df.rename(columns={'Country Code': 'locationID'}, inplace=True)

        # Replace values
        inclusion_df = inclusion_df.replace(to_replace="..", value=np.nan)
        inclusion_df['value'] = inclusion_df['value'].astype(float)

        # Filter only needed columns to export
        inclusion_df = self.flats.select_columns_fact_table(inclusion_df)
        return inclusion_df

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        inclusion_df = self.add_ids_to_table()
        self.flats.export_to_parquet(inclusion_df, '/financial_inclusion_fact/financial_inclusion_table')

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting financial inclusion table ---------")

    inclusion = FinancialInclusion(INPUT_PATH, OUTPUT_PATH)

    # Export
    inclusion.export_files()
