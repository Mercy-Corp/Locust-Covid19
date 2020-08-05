# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the population table.

Created on Thu Jul 02 17:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

#imports
import pandas as pd
import geopandas as gpd
from rasterstats import zonal_stats
from utils_flat_files import FlatFiles
import boto3
client = boto3.client('s3')

#S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

COUNTRIES = ["KEN", "SOM", "ETH", "UGA", "SDN", "SSD"]

class PopulationTable:
    '''
    This class creates the population table for the expected year.
    '''
    def __init__(self, year, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.year = year

    def read_district_shp(self, country):
        gdf_country = gdp.read_file(self.path_in + "Spatial/gadm36_" + country + "_2.shp")[['GID_2', 'geometry']]
        return gdf_country

    def read_population_raster(self, country):
        raster_country = self.path_in + "population/" + country + "_pop_" + str(self.year) + ".tif"
        return raster_country

    def calc_pop_density(self, country):
        gdf_country = self.read_district_shp(country)
        raster_country = self.read_population_raster(country)
        pop_density_country = zonal_stats(gdf_country.geometry, raster_country, layer="polygons", stats=['sum'])
        return pop_density_country

    def population_table(self):
        '''

        :return: the geodataframe with the population column
        '''
        countries_list = []
        for country in COUNTRIES:
            print(country)
            gdf_country = self.read_district_shp(country)
            gdf_country['population'] = self.calc_pop_density(country)
            countries_list.append(gdf_country)

        population_gdf = gpd.GeoDataFrame(pd.concat(countries_list, ignore_index=True))
        return population_gdf

    def add_ids_to_table(self):
        '''

        :return: The population fact table
        '''

        population_gdf = self.population_table()

        # Add locationID
        population_gdf['locationID'] = population_gdf['GID_2']

        # Add value column
        population_gdf['value'] = population_gdf['population']

        # Add measureID
        population_gdf['measureID'] = 26

        # Add year, to be transformed to dateId in later step
        population_gdf['year'] = self.year

        # Add factID
        population_gdf['factID'] = 'Pop_' + population_gdf['locationID'].astype(str) + "_" + population_gdf[
            'year'].astype(str)

        # Add dateID
        population_gdf = FlatFiles().add_date_id(population_gdf, column = 'year')

        # Select fact table columns
        population_df = FlatFiles().select_columns_fact_table(df = population_gdf)

        return population_df

    def export_population(self):
        '''

        :return: Exports population fact table to parquet format.
        '''
        population_df = self.add_ids_to_table()
        file_name = 'population_fact/population_table_' + str(self.year)
        population_df.to_parquet(self.path_out + file_name + ".parquet", index=False)
        print("Dataframe exported to parquet format")

if __name__ == '__main__':

    print("------- Extracting population tables ---------")
    PopulationTable(2000).export_population()
    #PopulationTable(2014).export_population()
    #PopulationTable(2015).export_population()
    #PopulationTable(2016).export_population()
    #PopulationTable(2017).export_population()
    #PopulationTable(2018).export_population()
    #PopulationTable(2019).export_population()
    #PopulationTable(2020).export_population()
