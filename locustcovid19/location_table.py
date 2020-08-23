# -*- coding: utf-8 -*-
"""
The aim of this module is to prepare the location table.
Data available from: https://gadm.org/download_country_v3.html

Created on Thu Jun 18 09:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

import yaml
import pandas as pd
import geopandas as gpd
from utils.shapefiles import Shapefiles

# S3 paths

#with open("config/application.yaml", "r") as ymlfile:
#    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

#INPUT_PATH = cfg["data"]['landing']
#OUTPUT_PATH = cfg["data"]['reporting']
#print(INPUT_PATH)

#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-reporting/'

class LocationTable:
    '''
    This class creates the location table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.countries = Shapefiles(self.path_in, self.path_out, 0).create_sub_tables()[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0"]]
        self.regions1 = Shapefiles(self.path_in, self.path_out, 1).create_sub_tables()[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1"]]
        self.regions2 = Shapefiles(self.path_in, self.path_out, 2).create_sub_tables()[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1", "GID_2", "NAME_2"]]
        self.regions3 = Shapefiles(self.path_in, self.path_out, 3).create_sub_tables()[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1", "GID_2", "NAME_2",
             "GID_3", "NAME_3"]]

    def concat_sub_tables(self):
        '''
        Concatenates the list of geodataframes into a single geodataframe.
        :return: A dataframe of all hierarchies containing only the necessary columns.
        '''
        gdf_all_list = [self.countries, self.regions1, self.regions2, self.regions3]
        gdf_all = gpd.GeoDataFrame(pd.concat(gdf_all_list, ignore_index=True))
        df_all = gdf_all[
            ['locationID', 'name', 'type', 'hierarchy', 'GID_0', 'GID_1', 'GID_2', 'GID_3', 'NAME_0', 'NAME_1',
             'NAME_2',
             'NAME_3']]
        return df_all

    def export_to_parquet(self, file_name):
        '''
        Exports the dataframe to a parquet format.
        :param file_name: the name of the file to be exported
        '''
        df = self.concat_sub_tables()
        df.to_parquet(self.path_out+'location_dim/'+file_name+".parquet", index=False)
        print("Location table extracted to parquet format.")

    def export_to_csv(self, file_name):
        '''
        Exports the df to a csv format.
        :param file_name: the name of the file to be exported
        :return:
        '''
        df = self.concat_sub_tables()
#        df.to_csv(self.path_out+'location_dim/'+file_name+'.csv', sep='|', encoding='utf-8', index=False)
#        print("Location table extracted to csv format.")

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg["data"]['landing']
    OUTPUT_PATH = cfg["data"]['reporting']
    print(INPUT_PATH)

    print("------- Extracting location table ---------")

    loc_table = LocationTable()
    # Create geodataframe
    gdf_all = loc_table.concat_sub_tables()
    # Export table to csv
#    loc_table.export_to_csv('location_table')
    loc_table.export_to_parquet('location_table')
