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
from utils.flat_files import FlatFiles
import yaml
# import boto3
# client = boto3.client('s3')

#local paths
# INPUT_PATH = r'data/input'
# OUTPUT_PATH = r'data/output'

COUNTRIES = ["KEN", "SOM", "ETH", "UGA", "SDN", "SSD"]

class PopulationTable:
    '''
    This class creates the population table for the expected year.
    '''
    def __init__(self, year, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.year = year
        self.flats = FlatFiles(path_in, path_out)

    def read_district_shp(self, country):
        '''

        :param country: The reference country
        :return: A geodataframe with 2 columns: district id and geometry.
        '''
        gdf_country = gpd.read_file(self.path_in + "Spatial/gadm36_" + country + "_2.shp")[['GID_2', 'geometry']]
        return gdf_country

    def read_population_raster(self, country):
        '''

        :param country: The reference country
        :return: The raster with the population density for the reference country.
        '''
        raster_country = self.path_in + "population/" + country + "_pop_" + str(self.year) + ".tif"
        return raster_country

    def calc_population(self, country):
        '''

        :param country: The reference country
        :return: The population for the given country
        '''
        gdf_country = self.read_district_shp(country)
        raster_country = self.read_population_raster(country)
        pop_density_country = zonal_stats(gdf_country.geometry, raster_country, layer="polygons", stats='sum')
        return pop_density_country


    def population_table(self):
        '''

        :return: the geodataframe with the population column
        '''
        countries_list = []
        for country in COUNTRIES:
            print("Preparing {} population table for {}.".format(self.year, country))
            gdf_country = self.read_district_shp(country)
            gdf_country['population'] = pd.DataFrame(self.calc_population(country))
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
        population_gdf = self.flats.add_date_id(population_gdf, column = 'year')

        # Select fact table columns
        population_df = self.flats.select_columns_fact_table(df = population_gdf)

        return population_df

    def export_population(self):
        '''

        :return: Exports population fact table to parquet format.
        '''
        population_df = self.add_ids_to_table()
        file_name = '/population_fact/population_table_' + str(self.year)
        #file_name = '/population_table_' + str(self.year)
        population_df.to_parquet(self.path_out + file_name + ".parquet", index=False)
        print("Dataframe exported to parquet format")

    def export_intermediate_pop(self):
        population_df = self.add_ids_to_table()
        filename = '/population_table_Sudan_' + str(self.year)
        self.flats.export_to_csv(population_df, filename)
        self.flats.export_to_parquet(population_df, filename)

    def append_populations_year(self):
        population_initial = pd.read_csv(self.path_out + '/population_table_' + str(self.year) + ".csv", sep="|")
        population_Sudan= self.add_ids_to_table()
        #population_Sudan= pd.read_csv(self.path_out + '/population_table_Sudan_' + str(self.year) + ".csv", sep="|")
        population_total = population_initial.append(population_Sudan)
        return population_total

    def export_total_population(self):
        '''

        :return: Exports population fact table to parquet format.
        '''
        population_df = self.append_populations_year()
        file_name = '/population_fact/population_table_' + str(self.year)
        #file_name = '/population_table_all_countries_' + str(self.year)
        population_df.to_parquet(self.path_out + file_name + ".parquet", index=False)
        print("Dataframe exported to parquet format")

if __name__ == '__main__':

    print("------- Extracting population tables ---------")
    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print(INPUT_PATH)

    #PopulationTable(2000).export_population()
    #PopulationTable(2014).export_population()
    #PopulationTable(2015).export_population()
    #PopulationTable(2016).export_population()
    #PopulationTable(2017).export_population()
    #PopulationTable(2018).export_population()
    PopulationTable(2019).export_population()
    #PopulationTable(2020).export_population()
