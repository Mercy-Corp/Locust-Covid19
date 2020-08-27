# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the displacement table.
Data available from: https://data.worldbank.org/indicator/VC.IDP.NWDS

Created on Thu Jun 24 09:36:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
from utils_flat_files import FlatFiles

#S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

COUNTRIES = ["Kenya", "Somalia", "Ethiopia", "Uganda", "South Sudan", "Sudan"]

YEARS = [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]

class DisplacementTable:
    '''
    This class creates the dislacements table.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.displacement_df = pd.read_csv(self.path_in + "social_cohesion/displacement/API_VC.IDP.NWDS_DS2_en_csv_v2_1223058.csv", skiprows=4, sep=",", encoding='utf-8')
        self.flats = FlatFiles(self.path_in, self.path_out)

    def filter_year(self, displacements_df, year):
        # Create year column
        displacements_df['year'] = str(year)
        #Create value column
        displacements_df['value'] = displacements_df[str(year)]
        #Filter columns
        displacements_year = displacements_df[['Country Code', 'year', 'value']]
        return displacements_year

    def filter_data(self):
        displacements = self.displacement_df
        # Filter countries
        displacements = displacements[displacements['Country Name'].isin(COUNTRIES)]

        #Filter years
        displacements_all_years = pd.DataFrame()
        for year in YEARS:
            displ_year = self.filter_year(displacements, year)
            displacements_all_years = displacements_all_years.append(displ_year)

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
        displacements['dateID'] = displacements['year'].astype(str)+ '0101'
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
        self.flats.export_csv_w_date(displacement_df, 'displacement_table')
        self.flats.export_parquet_w_date(displacement_df, 'displacement_table')
        #self.flats.export_parquet_w_date(displacement_df, 'displacement_fact/displacement_table')

if __name__ == '__main__':

    print("------- Extracting displacements table ---------")

    displacements = DisplacementTable()

    # Create dataframe
    displacement_df = displacements.add_ids_to_table()
    print(displacement_df.shape)
    print(displacement_df.head())

    # Export
    displacements.export_files()
