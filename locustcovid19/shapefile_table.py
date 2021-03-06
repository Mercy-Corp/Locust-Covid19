# -*- coding: utf-8 -*-
"""
The aim of this module is to prepare the shapefile table.
Data available from: https://gadm.org/download_country_v3.html

Created on Thu Jun 19 11:04:40 2020

@author: ioanna.papachristou@accenture.com
"""

import os
import yaml
import pandas as pd
import geopandas as gpd
from utils.shapefiles import Shapefiles

class ShapefileTable:
    '''
    This class creates the shapefile table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.countries = Shapefiles(self.path_in, self.path_out, 0).create_sub_tables()[["locationID", "geometry"]]
        self.regions1 = Shapefiles(self.path_in, self.path_out, 1).create_sub_tables()[["locationID", "geometry"]]
        self.regions2 = Shapefiles(self.path_in, self.path_out, 2).create_sub_tables()[["locationID", "geometry"]]
        self.regions3 = Shapefiles(self.path_in, self.path_out, 3).create_sub_tables()[["locationID", "geometry"]]

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
        gdf = self.concat_sub_tables()
        gdf.to_file(self.path_out + '/shape_boundary/' + file_name + ".shp", driver='ESRI Shapefile')
        print("Shapefile table extracted")


if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open("config/application.yaml", "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg["data"]['landing']
    OUTPUT_PATH = cfg["data"]['reporting']
    print(INPUT_PATH)

    print("------- Extracting shapefile table ---------")

    shp_table = ShapefileTable(INPUT_PATH, OUTPUT_PATH)
    #shp_table = ShapefileTable()
    gdf_all = shp_table.concat_sub_tables()
    #Export table to shp
    shp_table.export_to_shp(gdf_all, 'shapefile_table')
