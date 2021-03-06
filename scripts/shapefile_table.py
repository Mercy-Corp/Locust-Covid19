# -*- coding: utf-8 -*-
"""
The aim of this module is to prepare the shapefile table.

Created on Thu Jun 19 11:04:40 2020

@author: ioanna.papachristou@accenture.com
"""

import pandas as pd
import geopandas as gpd
from utils_shapefiles import Shapefiles
import boto3
client = boto3.client('s3')

s3 = boto3.resource('s3', region_name="us-east-1")

# S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/shape_boundary/'

# #local paths
#INPUT_PATH = r'data1/input/'
#OUTPUT_PATH = r'data1/output/'

class ShapefileTable:
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.countries = Shapefiles(self.path_in,0).create_sub_tables(Shapefiles(self.path_in,0).shapefiles_list())[["locationID", "geometry"]]
        self.regions1 = Shapefiles(self.path_in,1).create_sub_tables(Shapefiles(self.path_in,1).shapefiles_list())[["locationID", "geometry"]]
        self.regions2 = Shapefiles(self.path_in,2).create_sub_tables(Shapefiles(self.path_in,2).shapefiles_list())[["locationID", "geometry"]]
        self.regions3 = Shapefiles(self.path_in,3).create_sub_tables(Shapefiles(self.path_in,3).shapefiles_list())[["locationID", "geometry"]]

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
        gdf.to_file(self.path_out+file_name+".shp", driver='ESRI Shapefile')
        print("Shapefile table extracted")
        #gdf.to_file(file_name+".shp", driver= 'ESRI Shapefile')
        #extensions = ['.dbf', '.cpg','.prj','.shp','.shx']
        #for ent in extensions:
         #   s3.meta.client.upload_file(f'shapefile_table{ent}', 'mercy-locust-covid19-out-dev', f'shape_boundary/shapefile_table{ent}')
        #print("Shapefile table extracted")

if __name__ == '__main__':

    print("------- Extracting shapefile table ---------")

    shp_table = ShapefileTable()
    gdf_all = shp_table.concat_sub_tables()
    #Export table to shp
    shp_table.export_to_shp(gdf_all, 'shapefile_table')
