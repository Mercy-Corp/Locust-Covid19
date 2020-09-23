# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the refugees table.
Data available from: https://www.unhcr.org/refugee-statistics/

Created on Thu Jun 25 12:18:40 2020

@author: ioanna.papachristou@accenture.com
"""

import os
import yaml
import pandas as pd
from utils.flat_files import FlatFiles

COUNTRIES = ["Kenya", "Somalia", "Ethiopia", "Uganda", "South Sudan", "Sudan"]

class RefugeesTable:
    '''
    This class creates the refugees table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.refugees_df = pd.read_csv(self.path_in + "/social_cohesion/refugees/population.csv", skiprows=14, sep=",", encoding='utf-8')
        self.flats = FlatFiles(self.path_in, self.path_out)

    def convert_to_numeric(self, df):
        '''
        Converts columns to numeric.
        :param df: The data frame.
        :return: A df with the selected columns transformed to numeric.
        '''
        columns = ['Refugees under UNHCR’s mandate', 'Asylum-seekers',
       'IDPs of concern to UNHCR', 'Venezuelans displaced abroad',
       'Stateless persons', 'Others of concern']

        for column in columns:
            df[column] = pd.to_numeric(df[column])

        return df

    def add_ids(self):
        '''
        Adds the fact tables ids.
        :return:  A filtered dataframe with the columns we need for fact tables.
        '''
        refugees_df = self.convert_to_numeric(self.refugees_df)

        # Calculate value as the sum of all refugees (excluded 'Venezuelans displaced abroad' - all nans)
        refugees_df['value'] = refugees_df['Refugees under UNHCR’s mandate'] + refugees_df['Asylum-seekers'] + refugees_df['IDPs of concern to UNHCR'] + refugees_df['Stateless persons'] + refugees_df['Others of concern']

        # Create measureID
        refugees_df['measureID'] = 39

        # Create factID
        refugees_df['factID'] = 'REF_' + refugees_df.index.astype(str)

        # Add dateID
        refugees_df['dateID'] = refugees_df['Year'].astype(str) + '0101'
        refugees_df['dateID'] = refugees_df['dateID'].astype(int)

        # Add locationID
        refugees_df['locationID'] = refugees_df['Country of asylum (ISO)']

        # Filter only needed columns to export
        refugees_df = self.flats.select_columns_fact_table(refugees_df)

        return refugees_df

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        refugees_df = self.add_ids()
        #self.flats.export_csv_w_date(refugees_df, '/refugees_table')
        self.flats.export_to_parquet(refugees_df, '/refugees_fact/refugees_table')

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting refugees table ---------")

    refugees = RefugeesTable(INPUT_PATH, OUTPUT_PATH)

    # Create dataframe
    refugees_df = refugees.add_ids()
    print(refugees_df.shape)
    print(refugees_df.head())

    # Export
    refugees.export_files()
