# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the production table.

Created on Thu Jun 30 15:36:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
from utils_flat_files import FlatFiles

# #S3 paths
# INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
# OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

class ProductionTable:
    '''
    This class creates the production table.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.production_df = pd.read_csv(self.path_in + "FAOSTAT_data_6-30-2020.csv", sep=",")
        self.locations = pd.read_csv(self.path_out + "location_table.csv", sep = "|")[['locationID', 'name']]


    def create_measure_df(self):
        '''

        :return: A dataframe with the 5 measure ids and types we need.
        '''
        # initialize list of lists
        measure_id_data = [[12, 'Maize'], [13, 'Rice, paddy'], [14, 'Millet'], [15, 'Beans, dry'], [16, 'Wheat']]
        # Create the pandas DataFrame
        measure_df = pd.DataFrame(measure_id_data, columns=['measureID', 'Item'])
        self.measure_df = measure_df
        self.measure_df.head()
        return self.measure_df


    def add_ids_to_table(self):
        '''
        Merges with all other tables and extracts all ids.
        :return: The production dataframe with all columns as defined in the data model.
        '''
        # Merge production with measure and add measureID
        production = self.production_df.merge(self.create_measure_df(), on='Item', how='left')

        # Create factID
        production['factID'] = 'Dem_' + production['Area Code'].astype(str) + '_' + production[
            'Item Code'].astype(str) + '_' + production['Year Code'].astype(str)

        # Merge with dates and add dateID
        production = FlatFiles().add_date_id(production, 'Year')

        # Merge with locations and add locationID
        production = production.merge(self.locations, left_on='Area', right_on='name', how='left')

        # Add value column (production units = tonnes)
        production['value'] = production['Value']

        # Filter only needed columns to export
        production = FlatFiles().select_columns_fact_table(production)
        return production


if __name__ == '__main__':

    print("------- Extracting production table ---------")

    prod_table = ProductionTable()

    # Create dataframe
    production_df = prod_table.add_ids_to_table()

    # Export
    FlatFiles().export_output_w_date(production_df, 'production_table')


