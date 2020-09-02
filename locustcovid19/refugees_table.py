# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the refugees table.
Data available from: https://www.unhcr.org/refugee-statistics/

Created on Thu Jun 25 12:18:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
from utils_flat_files import FlatFiles


#S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

COUNTRIES = ["Kenya", "Somalia", "Ethiopia", "Uganda", "South Sudan", "Sudan"]

class RefugeesTable:
    '''
    This class creates the dislacements table.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.refugees_df = pd.read_csv(self.path_in + "social_cohesion/refugees/population.csv", skiprows=14, sep=",", encoding='utf-8')
        self.flats = FlatFiles(self.path_in, self.path_out)

    def convert_to_numeric(self, df):
        columns = ['Refugees under UNHCR’s mandate', 'Asylum-seekers',
       'IDPs of concern to UNHCR', 'Venezuelans displaced abroad',
       'Stateless persons', 'Others of concern']

        for column in columns:
            df[column] = pd.to_numeric(df[column])

        return df

    def add_ids(self):
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
        #self.flats.export_csv_w_date(refugees_df, 'refugees_table')
        #self.flats.export_parquet_w_date(refugees_df, 'refugees_table')
        self.flats.export_parquet_w_date(refugees_df, 'refugees_fact/refugees_table')

if __name__ == '__main__':

    print("------- Extracting refugees table ---------")

    refugees = RefugeesTable()

    # Create dataframe
    refugees_df = refugees.add_ids()
    print(refugees_df.shape)
    print(refugees_df.head())

    # Export
    refugees.export_files()