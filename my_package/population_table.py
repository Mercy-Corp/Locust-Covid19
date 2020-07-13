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

#S3 paths
# INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
# OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

class PopulationTable:
    '''
    This class creates the 2015 population table.
    '''
    def __init__(self, year, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.year = year
        self.production_df = pd.read_csv(self.path_in + "FAOSTAT_data_6-30-2020.csv", sep=",")
        self.shapefile_table = gpd.read_file(self.path_out + "shapefile_table.shp")

        self.gdf_Kenya = gpd.read_file(self.path_in + "gadm36_KEN_2.shp")[['GID_2', 'geometry']]
        self.gdf_Somalia = gpd.read_file(self.path_in + "gadm36_SOM_2.shp")[['GID_2', 'geometry']]
        self.gdf_Ethiopia = gpd.read_file(self.path_in + "gadm36_ETH_2.shp")[['GID_2', 'geometry']]
        self.gdf_Uganda = gpd.read_file(self.path_in + "gadm36_UGA_2.shp")[['GID_2', 'geometry']]

        self.raster_Uganda = self.path_in + "population/UGA_pop_" + str(self.year) + ".tif"
        self.raster_Kenya = self.path_in + "population/KEN_pop_" + str(self.year) + ".tif"
        self.raster_Somalia = self.path_in + "population/SOM_pop_" + str(self.year) + ".tif"
        self.raster_Ethiopia = self.path_in + "population/ETH_pop_" + str(self.year) + ".tif"

        self.pop_density_Uganda = zonal_stats(self.gdf_Uganda.geometry, self.raster_Uganda, layer="polygons", stats=['sum'])
        self.pop_density_Kenya = zonal_stats(self.gdf_Kenya.geometry, self.raster_Kenya, layer="polygons", stats=['sum'])
        self.pop_density_Somalia = zonal_stats(self.gdf_Somalia.geometry, self.raster_Somalia, layer="polygons", stats=['sum'])
        self.pop_density_Ethiopia = zonal_stats(self.gdf_Ethiopia.geometry, self.raster_Ethiopia, layer="polygons", stats=['sum'])

    def population_table(self):
        '''

        :return: the geodataframe with the population column
        '''
        # Add population as a column in our geodataframe
        self.gdf_Uganda['population'] = pd.DataFrame(self.pop_density_Uganda)
        self.gdf_Kenya['population'] = pd.DataFrame(self.pop_density_Kenya)
        self.gdf_Somalia['population'] = pd.DataFrame(self.pop_density_Somalia)
        self.gdf_Ethiopia['population'] = pd.DataFrame(self.pop_density_Ethiopia)

        countries_list = [self.gdf_Ethiopia, self.gdf_Somalia, self.gdf_Kenya, self.gdf_Uganda]
        population_gdf = gpd.GeoDataFrame(pd.concat(countries_list, ignore_index=True))
        return population_gdf

    def add_ids_to_table(self):

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
        population_df = self.add_ids_to_table()
        filename = 'population_table_' + str(self.year)
        FlatFiles().export_output(population_df, filename)

if __name__ == '__main__':

    print("------- Extracting population tables ---------")
    PopulationTable(2000).export_population()
    PopulationTable(2014).export_population()
    PopulationTable(2015).export_population()
    PopulationTable(2016).export_population()
    PopulationTable(2017).export_population()
    PopulationTable(2018).export_population()
    PopulationTable(2020).export_population()
