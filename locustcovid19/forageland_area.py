# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the cropland area per district.
Data source: http://www.fao.org/geonetwork/srv/en/main.search?title=land%20cover

Created on Mon Jul 13 11:16:40 2020
Last updated Wed Aug 26 17:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import os
import pandas as pd
import geopandas as gpd
from utils.flat_files import FlatFiles
import yaml
from rasterstats import zonal_stats
import warnings
warnings.filterwarnings("ignore")

#S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-reporting/'

#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class Forageland:
    '''
    This class calculates the forageland area per district.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(path_in, path_out)
        '''
        # Import forageland vector
        self.forageland_v = gpd.read_file(self.path_in + "/forageland/forageland_vector.shp")
        self.forageland_v.crs = {"init": "epsg:4326"}
        '''
        # Forageland 2003 raster path
        self.raster_path = self.path_in + '/forageland/forageland2003.tif'

    def read_boundaries_shp(self, country, hierarchy):
        '''

        :param country: The reference country
        :param hierarchy: The boundaries level, 0 for countries, 1 for regions, 2 for districts.
        :return: A geodataframe with 2 columns: locationID and geometry.
        '''
        gdf_country = gpd.read_file(self.path_in + "/Spatial/gadm36_" + country + "_" + str(hierarchy) + ".shp")
        GID_column = 'GID_' + str(hierarchy)
        gdf_country = gdf_country[[GID_column, 'geometry']]
        gdf_country = gdf_country.rename(columns={GID_column: 'locationID'})

        return gdf_country

    def get_districts(self):
        '''

        :return: A geodataframe with all districts of the 4 countries concatenated.
        '''
        gdf_districts = gpd.GeoDataFrame()
        for country in COUNTRIES_IDS:
            gdf_district = self.read_boundaries_shp(country, 2)
            gdf_districts = gdf_districts.append(gdf_district)
        gdf_districts.crs = {"init": "epsg:4326"}
        return gdf_districts

    def get_stats(self):
        '''

        :param raster: The geotiff indicating the cropland area
        :return: A df with two columns, district id and cropland area.
        '''
        gdf_districts = self.get_districts()
        gdf_districts['area'] = gdf_districts.geometry.area # Calculate area of each district.
        stats = zonal_stats(gdf_districts.geometry, self.raster_path,  layer="polygons", stats="count", categorical=True) # Cross districts with foragelands
        gdf_districts['forageland_count'] = pd.DataFrame.from_dict(stats)[1] # Count areas of values == 1, foragelands, per district
        gdf_districts['area_count'] = pd.DataFrame.from_dict(stats)["count"] # Total number of pixels per district
        gdf_districts = gdf_districts[gdf_districts['forageland_count'].notnull()] # drop nulls
        gdf_districts['forageland_area'] = gdf_districts['forageland_count'] * gdf_districts['area'] / gdf_districts[
            'area_count']
        #Filter columns
        df_districts = gdf_districts[['locationID', 'forageland_area']]

        return df_districts

    # def forageland_district(self):
    #     '''
    #
    #     :return: A geodataframe with the forageland intersected by district.
    #     '''
    #     forageland = self.forageland_v
    #     gdf_districts = self.get_districts()
    #     forageland_district = gpd.overlay(forageland, gdf_districts, how='intersection')
    #     return forageland_district
    #
    # def forageland_area(self):
    #     '''
    #
    #     :return: A geodataframe with the area of forageland per district calculated.
    #     '''
    #     forageland_district = gpd.GeoDataFrame(self.forageland_district())
    #     forageland_district['area_inter'] = forageland_district.geometry.area
    #     return forageland_district

    def add_fact_ids(self):
        '''
        Adds the fact tables ids
        :return:  A filtered dataframe by the columns we need for fact tables.
        '''
        forageland_district = self.get_stats()
        forageland_district['measureID'] = 28
        forageland_district['factID'] = 'FOR_' + forageland_district.index.astype(str)
        forageland_district['year'] = 2003
        #forageland_district['date'] = pd.to_datetime([f'{y}-01-01' for y in forageland_district.year])
        forageland_district['value'] = forageland_district['forageland_area']

        # Add dateID
        forageland_district = self.flats.add_date_id(forageland_district, column = 'year')

        # Select fact table columns
        forageland_df = self.flats.select_columns_fact_table(df = forageland_district)
        
        return forageland_df

    def export_table(self, filename):
        '''

        :return: The Forageland table in both a parquet and csv format with the date added in the name.
        '''
        forageland_df = self.add_fact_ids()
        self.flats.export_to_parquet(forageland_df, filename)
        #self.flats.export_csv_w_date(forageland_df, filename)
        
if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting forageland area per district table ---------")
    Forageland(INPUT_PATH, OUTPUT_PATH).export_table('/forageland_fact/forageland')
    #Forageland(INPUT_PATH, OUTPUT_PATH).export_table('forageland')
