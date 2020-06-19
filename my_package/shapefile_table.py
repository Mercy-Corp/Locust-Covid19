# -*- coding: utf-8 -*-
"""
The aim of this module is to prepare the location table.

Created on Thu Jun 19 11:04:40 2020

@author: ioanna.papachristou@accenture.com
"""

import pandas as pd
import geopandas as gpd
from my_package.utils_shapefiles import Shapefiles

PATH = r'./data/'

class ShapefileTable:
    def __init__(self):
        self.path = PATH
        self.countries = Shapefiles(0).create_sub_tables(Shapefiles(0).shapefiles_list())[["locationID", "geometry"]]
        self.regions1 = Shapefiles(1).create_sub_tables(Shapefiles(1).shapefiles_list())[["locationID", "geometry"]]
        self.regions2 = Shapefiles(2).create_sub_tables(Shapefiles(2).shapefiles_list())[["locationID", "geometry"]]
        self.regions3 = Shapefiles(3).create_sub_tables(Shapefiles(3).shapefiles_list())[["locationID", "geometry"]]

    def concat_sub_tables(self):
        '''
        Concatenates the list of geodataframes into a single geodataframe.
        :return: A geodataframe of all hierarchies containing only the necessary columns.
        '''
        gdf_all_list = [self.countries, self.regions1, self.regions2, self.regions3]
        #print(gdf_all_list[0].columns)
        gdf_all = gpd.GeoDataFrame(pd.concat(gdf_all_list, ignore_index=True))
        return gdf_all

    def export_to_shp(self, gdf, file_name):
        '''
        Exports a geodataframe to a shp format.
        :param gdf: The geodataframe to be exported
        :param file_name: the name of the file to be exported
        '''
        gdf.to_file(self.path+"output/"+file_name+".shp", driver='ESRI Shapefile')
        print("Shapefile table extracted")


if __name__ == '__main__':

    print("------- Extracting shapefile table ---------")

    shp_table = ShapefileTable()
    gdf_all = shp_table.concat_sub_tables()
    #Export table to shp
    shp_table.export_to_shp(gdf_all, 'shapefile_table')


