# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the production table.

Created on Thu Jun 30 15:36:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd

class ProductionTable: #TODO describe functions
    '''
    This class creates the production table.
    '''
    def __init__(self, path):
        self.path = path
        self.production_df = pd.read_csv(self.path + "input/FAOSTAT_data_6-30-2020.csv", sep=",") #TODO imports from parquet format?
        self.dates = pd.read_csv(self.path + "output/date_23_06-2020.csv")
        self.locations = pd.read_csv(self.path + "output/location_table.csv", sep="|")[['locationID', 'name']]


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


    def year_to_datetime(self, production_df, column):
        '''

        :param production_df: The production df
        :param column: The column we want to apply the function
        :return: The production df with the year column turned into datetime of  format yyyy-01-01.
        '''
        # Transform year column to datetime
        self.production_df[column] = pd.to_datetime([f'{y}-01-01' for y in production_df[column]])
        return production_df


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
        production['Year'] = self.year_to_datetime(production, 'Year')
        production = production.merge(self.dates, left_on='Year', right_on='date', how='left')

        # Merge with locations and add locationID
        production = production.merge(self.locations, left_on='Area', right_on='name', how='left')

        # Add value column (production units = tonnes)
        production['value'] = production['Value']

        # Filter only needed columns to export
        production = production[['factID', 'measureID', 'dateID', 'locationID', 'value']]
        return production

    def export(self, df):
        '''

        :param df: The dataframe to export.
        :return: Exports table to a) parquet and b) csv format
        '''
        # Export to parquet
        df.to_parquet(self.path + 'output/production_table_20200630.parquet',
                                 compression='uncompressed', index=False) #TODO automatise date at the end of the name
        # Export to csv
        df.to_csv(self.path + 'output/production_table_20200630.csv', sep='|', encoding='utf-8', index=False)

        print("Production table exported")


if __name__ == '__main__':

    print("------- Extracting production table ---------")

    path = r'./data/'

    prod_table = ProductionTable(path)

    # Create dataframe
    production_df = prod_table.add_ids_to_table()

    # Export
    prod_table.export(production_df)


