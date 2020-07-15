# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the cropland area per district.

Created on Mon Jul 13 11:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
import geopandas as gpd
from utils_flat_files import FlatFiles

#S3 paths
# INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
# OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

class Forageland:
    '''
    This class calculates the forageland area per district.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

        # Import districts
        self.shp2_Kenya = gpd.read_file(self.path_in + "gadm36_KEN_2.shp")[['GID_2', 'geometry']]
        self.shp2_Somalia = gpd.read_file(self.path_in + "gadm36_SOM_2.shp")[['GID_2', 'geometry']]
        self.shp2_Ethiopia = gpd.read_file(self.path_in + "gadm36_ETH_2.shp")[['GID_2', 'geometry']]
        self.shp2_Uganda = gpd.read_file(self.path_in + "gadm36_UGA_2.shp")[['GID_2', 'geometry']]

        # Import forageland vector
        self.forageland_v = gpd.read_file(self.path_in + "forageland/forageland_vector.shp")
        self.forageland_v.crs = {"init": "epsg:4326"}

    def get_districts(self):
        '''
        
        :return: A geodataframe with all districts of the 4 countries concatenated.
        '''
        district_level = [self.shp2_Kenya, self.shp2_Ethiopia, self.shp2_Somalia, self.shp2_Uganda]
        gdf_districts = gpd.GeoDataFrame(pd.concat(district_level, ignore_index=True))
        gdf_districts.crs = {"init": "epsg:4326"}
        return gdf_districts


    def forageland_district(self):
        '''

        :return: A geodataframe with the forageland intersected by district.
        '''
        forageland = self.forageland_v
        gdf_districts = self.get_districts()
        forageland_district = gpd.overlay(forageland, gdf_districts, how='intersection')
        return forageland_district

    def forageland_area(self):
        '''

        :return: A geodataframe with the area of forageland per district calculated.
        '''
        forageland_district = gpd.GeoDataFrame(self.forageland_district())
        forageland_district['area_inter'] = forageland_district.geometry.area
        return forageland_district

    def add_fact_ids(self):
        '''
        Adds the fact tables ids
        :return:  A filtered dataframe by the columns we need for fact tables.
        '''
        forageland_district = gpd.GeoDataFrame(self.forageland_area())
        forageland_district['measureID'] = 28
        forageland_district['factID'] = 'FOR_' + forageland_district['fid'].astype(str)
        forageland_district['year'] = 2015
        forageland_district['date'] = pd.to_datetime([f'{y}-01-01' for y in forageland_district.year])
        forageland_district['locationID'] = forageland_district['GID_2']
        forageland_district['value'] = forageland_district['area_inter']

        # Add dateID
        forageland_gdf = FlatFiles().add_date_id(forageland_district, column = 'year')

        # Select fact table columns
        forageland_df = FlatFiles().select_columns_fact_table(df = forageland_gdf)
        
        return forageland_df

    def export_table(self, filename):
        '''

        :return: The Forageland table in both a parquet and csv format with the date added in the name.
        '''
        forageland_df = self.add_fact_ids()
        FlatFiles().export_output_w_date(forageland_df, filename)
        
if __name__ == '__main__':

    print("------- Extracting forageland area per district table ---------")
    Forageland().export_table('Forageland')