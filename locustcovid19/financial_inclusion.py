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

@author: ioanna.papachristou@accenture.com
"""

import os
import pandas as pd
from utils.flat_files import FlatFiles
import yaml

# #local paths
# INPUT_PATH = r'data/input'
# OUTPUT_PATH = r'data/output'

class FinancialInclusion:
    '''
    This class creates the production table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.fin_inclusion_df = pd.read_csv(self.path_in + "/social_cohesion/financial_inclusion/FINDEX_Data.csv", sep=',', skipfooter = 5, engine = 'python')
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

    def add_ids_to_table(self):
        '''
        Merges with all other tables and extracts all ids.
        :return: The production dataframe with all columns as defined in the data model.
        '''
        # Merge production with measure and add measureID
        inclusion = self.fin_inclusion_df.merge(self.create_measure_df(), left_on='Series Name', right_on ='indicator_name', how='left')

        # Create factID
        inclusion['factID'] = 'Fin_' + inclusion.index.astype(str)

        # Merge with dates and add dateID
        inclusion_2011 = inclusion.drop(['2014 [YR2014]', '2017 [YR2017]'], axis=1)
        inclusion_2011.rename(columns={'2011 [YR2011]': 'value'}, inplace=True)
        inclusion_2011['year'] = 2011

        inclusion_2014 = inclusion.drop(['2011 [YR2011]', '2017 [YR2017]'], axis=1)
        inclusion_2014.rename(columns={'2014 [YR2014]': 'value'}, inplace=True)
        inclusion_2014['year'] = 2014

        inclusion_2017 = inclusion.drop(['2014 [YR2014]', '2011 [YR2011]'], axis=1)
        inclusion_2017.rename(columns={'2017 [YR2017]': 'value'}, inplace=True)
        inclusion_2017['year'] = 2017

        inclusion_df = pd.DataFrame()
        inclusion_df = inclusion_df.append([inclusion_2011, inclusion_2014, inclusion_2017], ignore_index=True)
        inclusion_df = self.flats.add_date_id(inclusion_df, 'year')

        # Add locationID
        inclusion_df.rename(columns={'Country Code': 'locationID'}, inplace=True)

        # Replace values
        inclusion_df.replace(to_replace="..", value="")

        # Filter only needed columns to export
        inclusion_df = self.flats.select_columns_fact_table(inclusion_df)
        return inclusion_df

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        inclusion_df = self.add_ids_to_table()
        #self.flats.export_csv_w_date(inclusion_df, '/financial_inclusion_table') # for testing purposes only
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