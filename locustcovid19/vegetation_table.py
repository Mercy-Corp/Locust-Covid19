# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the NDVI vegetation index per district.
Data source: https://earlywarning.usgs.gov/fews/product/448

Created on Mon Sep 10 11:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
import geopandas as gpd
from utils.flat_files import FlatFiles
from rasterstats import zonal_stats
#import yaml
#import warnings
#warnings.filterwarnings("ignore")

#local paths
INPUT_PATH = r'data/input'
OUTPUT_PATH = r'data/output'

COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class VegetationTable:
    '''
    This class calculates the NDVI vegetation index per district.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)

        # NDVI raster path
        self.raster_path = self.path_in + '/vegetation/ea2023m.tif'

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

    def get_all_countries(self, hierarchy):
        '''

        :return: A geodataframe with all districts of the 6 countries concatenated.
        '''
        gdf_countries = gpd.GeoDataFrame()
        for country in COUNTRIES_IDS:
            gdf_country = self.read_boundaries_shp(country, hierarchy)
            gdf_countries = gdf_countries.append(gdf_country)
        gdf_countries.crs = {"init": "epsg:4326"}
        return gdf_countries

    def get_stats(self):
        '''

        :return: A df with two columns, district id and cropland area.
        '''
        gdf_districts = self.get_all_countries(2)
        stats = zonal_stats(gdf_districts.geometry, self.raster_path,  layer="polygons", stats="mean") # Cross districts with ndvi
        gdf_districts['avg_ndvi'] = pd.DataFrame(stats)
        #Filter columns
        df_districts = gdf_districts[['locationID', 'avg_ndvi']]

        return df_districts

    def export_table(self, filename):
        '''

        :return: The Forageland table in both a parquet and csv format with the date added in the name.
        '''
        forageland_df = self.get_stats()
        #self.flats.export_to_parquet(forageland_df, filename)
        self.flats.export_csv_w_date(forageland_df, filename)


if __name__ == '__main__':

    # filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    # with open(filepath, "r") as ymlfile:
    #     cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    #
    # INPUT_PATH = cfg['data']['landing']
    # OUTPUT_PATH = cfg['data']['reporting']
    # print(INPUT_PATH)

    print("------- Extracting vegetation index per district table ---------")
    #Call the class
    vegetation = VegetationTable(INPUT_PATH, OUTPUT_PATH)

    vegetation.export_table('/vegetation_table')