# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the production table.

Created on Thu Jul 02 17:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

#imports
import pandas as pd
import geopandas as gpd
import rasterio
from rasterstats import zonal_stats
from utils_shapefiles import Shapefiles

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
    def __init__(self, INPUT_PATH, OUTPUT_PATH):
        self.path_in = INPUT_PATH
        self.path_out = OUTPUT_PATH
        self.production_df = pd.read_csv(self.path_in + "FAOSTAT_data_6-30-2020.csv", sep=",")
        self.dates = pd.read_csv(self.path_out + 'date_23_06-2020.csv', sep = ",")
        self.dates['date'] = pd.to_datetime(self.dates['date'])
        self.shapefile_table = gpd.read_file(self.path_out + "shapefile_table.shp")

        self.gdf_Kenya = gpd.read_file(self.path_in + "gadm36_KEN_2.shp")[['GID_2', 'geometry']]
        self.gdf_Somalia = gpd.read_file(self.path_in + "gadm36_SOM_2.shp")[['GID_2', 'geometry']]
        self.gdf_Ethiopia = gpd.read_file(self.path_in + "gadm36_ETH_2.shp")[['GID_2', 'geometry']]
        self.gdf_Uganda = gpd.read_file(self.path_in + "gadm36_UGA_2.shp")[['GID_2', 'geometry']]

        self.raster_Uganda = self.path_in + "population/UGA_ppp_v2b_2015_UNadj.tif"
        self.raster_Kenya = self.path_in + "population/KEN_popmap15adj_v2b.tif"
        self.raster_Somalia = self.path_in + "population/SOM15adjv2.tif"
        self.raster_Ethiopia = self.path_in + "population/ETH15adjv5.tif"

        self.pop_density_Uganda = zonal_stats(self.gdf_Uganda.geometry, self.raster_Uganda, layer="polygons", stats=['sum'])
        self.pop_density_Kenya = zonal_stats(self.gdf_Kenya.geometry, self.raster_Kenya, layer="polygons", stats=['sum'])
        self.pop_density_Somalia = zonal_stats(self.gdf_Somalia.geometry, self.raster_Somalia, layer="polygons", stats=['sum'])
        self.pop_density_Ethiopia = zonal_stats(self.gdf_Ethiopia.geometry, self.raster_Ethiopia, layer="polygons", stats=['sum'])

    def population_table(self):
        # Add it as a column in our geodataframe
        self.gdf_Uganda['population'] = pd.DataFrame(self.pop_density_Uganda)
        self.gdf_Kenya['population'] = pd.DataFrame(self.pop_density_Kenya)
        self.gdf_Somalia['population'] = pd.DataFrame(self.pop_density_Somalia)
        self.gdf_Ethiopia['population'] = pd.DataFrame(self.pop_density_Ethiopia)

        countries_list = [self.gdf_Ethiopia, self.gdf_Somalia, self.gdf_Kenya, self.gdf_Uganda]
        population_gdf = gpd.GeoDataFrame(pd.concat(countries_list, ignore_index=True))
        return population_gdf

    def add_ids_to_table(self):

        population_gdf = self.population_table()

        population_gdf['locationID'] = population_gdf['GID_2']
        population_gdf['value'] = population_gdf['population']
        population_gdf['measureID'] = 26
        population_gdf['year'] = 2015
        population_gdf['factID'] = 'Pop_' + population_gdf['locationID'].astype(str) + "_" + population_gdf[
            'year'].astype(str)
        population_gdf['year'] = pd.to_datetime([f'{y}-01-01' for y in population_gdf.year])
        population_gdf = population_gdf.merge(self.dates, left_on='year', right_on='date', how='left')
        population_df = population_gdf[['factID', 'measureID', 'dateID', 'locationID', 'value']]

        return population_df

    def export_to_parquet(self, df, file_name):
        '''
        Exports a dataframe to a parquet format.
        :param df: The dataframe to be exported
        :param file_name: the name of the file to be exported
        '''
        df.to_parquet(self.path_out+file_name+".parquet", compression='uncompressed', index=False)
        print("Table extracted to parquet")

    def export_to_csv(self, df, file_name):
        '''
        Exports a dataframe to a parquet format.
        :param df: The dataframe to be exported
        :param file_name: the name of the file to be exported
        '''
        # Export to csv
        df.to_csv(self.path_out + file_name + '.csv', sep='|', encoding='utf-8', index=False)
        print("Table extracted to csv")

if __name__ == '__main__':

    print("------- Extracting population table ---------")

    pop_table = PopulationTable(INPUT_PATH, OUTPUT_PATH)

    # Create dataframe
    population_df = pop_table.add_ids_to_table()

    # Export
    pop_table.export_to_parquet(population_df, 'population_table_2015')
    pop_table.export_to_csv(population_df, 'population_table_2015')