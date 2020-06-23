# -*- coding: utf-8 -*-
"""
The aim of this module is to prepare the location table.

Created on Thu Jun 18 09:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

import pandas as pd
import geopandas as gpd
from my_package.utils_shapefiles import Shapefiles

PATH = r'./data/'

class LocationTable:
    '''
    This class creates the location table.
    '''
    def __init__(self, path):

        self.path = path
        self.countries = Shapefiles(self.path,0).create_sub_tables(Shapefiles(self.path,0).shapefiles_list())[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0"]]
        self.regions1 = Shapefiles(self.path,1).create_sub_tables(Shapefiles(self.path,1).shapefiles_list())[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1"]]
        self.regions2 = Shapefiles(self.path,2).create_sub_tables(Shapefiles(self.path,2).shapefiles_list())[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1", "GID_2", "NAME_2"]]
        self.regions3 = Shapefiles(self.path,3).create_sub_tables(Shapefiles(self.path,3).shapefiles_list())[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1", "GID_2", "NAME_2",
             "GID_3", "NAME_3"]]

    def concat_sub_tables(self):
        '''
        Concatenates the list of geodataframes into a single geodataframe.
        :return: A geodataframe of all hierarchies containing only the necessary columns.
        '''
        gdf_all_list = [self.countries, self.regions1, self.regions2, self.regions3]
        gdf_all = gpd.GeoDataFrame(pd.concat(gdf_all_list, ignore_index=True))
        gdf_all = gdf_all[
            ['locationID', 'name', 'type', 'hierarchy', 'GID_0', 'GID_1', 'GID_2', 'GID_3', 'NAME_0', 'NAME_1',
             'NAME_2',
             'NAME_3']]
        return gdf_all

    def export_to_parquet(self, gdf, file_name):
        '''
        Exports a geodataframe to a parquet format.
        :param gdf: The geodataframe to be exported
        :param file_name: the name of the file to be exported
        '''
        gdf.to_parquet(self.path+"output/"+file_name+".parquet", compression='uncompressed')
        print("Location table extracted")


if __name__ == '__main__':

    print("------- Extracting location table ---------")

    path = r'./data/'

    loc_table = LocationTable(path)
    # Create geodataframe
    gdf_all = loc_table.concat_sub_tables()
    # Export table to parquet
    loc_table.export_to_parquet(gdf_all, 'location_table')
