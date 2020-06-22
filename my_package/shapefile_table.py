# -*- coding: utf-8 -*-
"""
The aim of this module is to prepare the shapefile table.

Created on Thu Jun 19 11:04:40 2020

@author: ioanna.papachristou@accenture.com
"""

import pandas as pd
import geopandas as gpd
from my_package.utils_shapefiles import Shapefiles

class ShapefileTable:
    def __init__(self, path):
        self.path = path
        self.countries = Shapefiles(self.path,0).create_sub_tables(Shapefiles(self.path,0).shapefiles_list())[["locationID", "geometry"]]
        self.regions1 = Shapefiles(self.path,1).create_sub_tables(Shapefiles(self.path,1).shapefiles_list())[["locationID", "geometry"]]
        self.regions2 = Shapefiles(self.path,2).create_sub_tables(Shapefiles(self.path,2).shapefiles_list())[["locationID", "geometry"]]
        self.regions3 = Shapefiles(self.path,3).create_sub_tables(Shapefiles(self.path,3).shapefiles_list())[["locationID", "geometry"]]

    def concat_sub_tables(self):
        '''
        Concatenates the list of geodataframes into a single geodataframe.
        :return: A geodataframe of all hierarchies containing only the necessary columns.
        '''
        gdf_all_list = [self.countries, self.regions1, self.regions2, self.regions3]
        #print(gdf_all_list[0].columns)
        gdf_all = gpd.GeoDataFrame(pd.concat(gdf_all_list, ignore_index=True))
        gdf_all.crs = {"init": "epsg:4326"}

        #Calculate areas in units = degrees
        gdf_all['area'] = gdf_all['geometry'].area
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

    path = r'./data/'

    shp_table = ShapefileTable(path)
    gdf_all = shp_table.concat_sub_tables()
    #Export table to shp
    shp_table.export_to_shp(gdf_all, 'shapefile_table')

