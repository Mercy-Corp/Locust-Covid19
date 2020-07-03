# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the production table.

Created on Thu Jun 30 15:36:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
import time

#S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'

# #local paths
# INPUT_PATH = r'data/input/'
# OUTPUT_PATH = r'data/output/'

class ProductionTable:
    '''
    This class creates the production table.
    '''
    def __init__(self, INPUT_PATH, OUTPUT_PATH):
        self.path_in = INPUT_PATH
        self.path_out = OUTPUT_PATH
        self.production_df = pd.read_csv(self.path_in + "FAOSTAT_data_6-30-2020.csv", sep=",")
        self.dates = pd.read_csv(self.path_out + 'date_23_06-2020.csv', sep=",")
        self.dates['date'] = pd.to_datetime(self.dates['date'])
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
        production['Year'] = pd.to_datetime([f'{y}-01-01' for y in production.Year])
        production = production.merge(self.dates, left_on='Year', right_on='date', how='left')

        # Merge with locations and add locationID
        production = production.merge(self.locations, left_on='Area', right_on='name', how='left')

        # Add value column (production units = tonnes)
        production['value'] = production['Value']

        # Filter only needed columns to export
        production = production[['factID', 'measureID', 'dateID', 'locationID', 'value']]
        return production

    def date_today(self):
        # Add today's date
        todaysDate = time.strftime("%Y-%m-%d")
        return todaysDate

    def export(self, df):
        '''

        :param df: The dataframe to export.
        :return: Exports table to a) parquet and b) csv format
        '''

        # Export to parquet
        date = self.date_today()
        df.to_parquet(self.path_out + 'production_table_' + date +'.parquet', compression='uncompressed',
                                 index=False)

        # Export to csv
        df.to_csv(self.path_out + 'production_table_' + date +'.csv', sep='|', encoding='utf-8', index=False)

        print("Production table exported")


if __name__ == '__main__':

    print("------- Extracting production table ---------")

    prod_table = ProductionTable(INPUT_PATH, OUTPUT_PATH)

    # Create dataframe
    production_df = prod_table.add_ids_to_table()

    # Export
    prod_table.export(production_df)


