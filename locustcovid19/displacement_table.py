# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the displacements table.
Data available from: https://data.worldbank.org/indicator/VC.IDP.NWDS

Created on Thu Jun 24 09:36:40 2020
Latest update on Wed Sep 23 11:30:40 2020

@author: ioanna.papachristou@accenture.com
"""

import os
import yaml
import pandas as pd
from utils.flat_files import FlatFiles

COUNTRIES = ["Kenya", "Somalia", "Ethiopia", "Uganda", "South Sudan", "Sudan"]

# #local paths
# INPUT_PATH = r'data/input'
# OUTPUT_PATH = r'data/output'

class DisplacementTable:
    '''
    This class creates the displacements table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.displacement_df = pd.read_csv(self.path_in + "/social_cohesion/displacement/displacements.csv", skiprows=4, sep=",", encoding='utf-8')
        self.flats = FlatFiles(self.path_in, self.path_out)

    def filter_years(self, df):
        '''

        :param df: The input df.
        :return: A df with the displacements for the existing years.
        '''
        displacements_all_years = pd.DataFrame()

        for year in range(2000, 2030):
            if str(year) in df.columns:
                displacements_year = df.copy()
                # Create year column
                displacements_year['year'] = int(year)
                # Create value column
                displacements_year['value'] = displacements_year[str(year)]
                # Filter columns
                displacements_year = displacements_year[['Country Code', 'year', 'value']]
                #  Append
                displacements_all_years = displacements_all_years.append(displacements_year)

        return displacements_all_years

    def filter_data(self):
        '''
        Filters the dataset.
        :return: A df with the displacement for the selected years and countries.
        '''
        displacements = self.displacement_df
        # Filter countries
        displacements = displacements[displacements['Country Name'].isin(COUNTRIES)]

        #Filter years
        displacements_all_years = self.filter_years(displacements)
        return displacements_all_years

    def add_ids_to_table(self):
        '''
        Merges with all other tables and extracts all ids.
        :return: The production dataframe with all columns as defined in the data model.
        '''
        displacements = self.filter_data().reset_index(drop=True)

        # Create measureID
        displacements['measureID'] = 34

        # Create factID
        displacements['factID'] = 'DISP_' + displacements.index.astype(str)

        # Add dateID
        displacements['dateID'] = displacements['year'].astype(str) + '0101'
        displacements['dateID'] = displacements['dateID'].astype(int)
        #displacements = self.flats.add_date_id(displacements, 'year')

        # Add locationID
        displacements['locationID'] = displacements['Country Code']

        # Filter only needed columns to export
        displacements_df = self.flats.select_columns_fact_table(displacements)
        return displacements_df

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        displacement_df = self.add_ids_to_table()
        #self.flats.export_csv_w_date(displacement_df, '/displacement_table')
        self.flats.export_to_parquet(displacement_df, '/displacement_fact/displacement_table')

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting displacements table ---------")

    displacements = DisplacementTable(INPUT_PATH, OUTPUT_PATH)

    # Create dataframe
    displacement_df = displacements.add_ids_to_table()
    print(displacement_df.shape)
    print(displacement_df.head())

    # Export
    displacements.export_files()
