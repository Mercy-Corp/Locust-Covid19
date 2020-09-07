# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the famine vulnerability table.
Data available from: https://fews.net/fews-data/333

Created on Thu Sep 03 12:56:40 2020

@author: ioanna.papachristou@accenture.com
"""

import pandas as pd
import geopandas as gpd
from utils.flat_files import FlatFiles
import glob
import yaml

COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class FamineTable:
    '''
    This class creates the famine vulnerability table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)

    def read_famine_data(self):
        '''
        Reads and appends all shp following the format 'famine/EA_*_CS.shp' adding a date column based on the name.
        :return: A geodataframe of all famine related historical data.
        '''
        gdf_list = []
        for file in glob.glob(self.path_in + 'famine/EA_*_CS.shp'):
            # Split by "_"
            date, file_type = file.split('.')[0].split('_')[1:]
            gdf = (gpd.read_file(file)
                  .assign(date=date+'01'))
            gdf_list.append(gdf)

        famine = pd.concat(gdf_list)

        return famine

    def read_boundaries_shp(self, country, hierarchy):
        '''

        :param country: The reference country
        :param hierarchy: The boundaries level, 0 for countries, 1 for regions, 2 for districts.
        :return: A geodataframe with 2 columns: locationID and geometry.
        '''
        gdf_country = gpd.read_file(self.path_in + "Spatial/gadm36_" + country + "_" + str(hierarchy) + ".shp")
        GID_column = 'GID_' + str(hierarchy)
        gdf_country = gdf_country[[GID_column, 'geometry']]
        gdf_country = gdf_country.rename(columns={GID_column: 'locationID'})

        return gdf_country

    def get_districts(self):
        '''
        Reads and appends all gdfs per country at a district level.
        :return: A gdf of all districts of the countries encountered in COUNTRIES_IDS.
        '''
        all_districts = gpd.GeoDataFrame()
        for country in COUNTRIES_IDS:
            gdf_district = self.read_boundaries_shp(country, 2)
            all_districts = all_districts.append(gdf_district)
        all_districts.crs = {"init": "epsg:4326"}

        return all_districts

    def rename_columns(self, gdf):
        '''

        :param gdf: The geodataframe.
        :return: The geodataframe with it's column names changed
        '''
        gdf.rename(columns={'CS': 'IPC', 'ML1': 'IPC', 'ML2': 'IPC', 'HA0': 'HA', 'HA1': 'HA', 'HA2': 'HA'}, inplace=True)
        gdf.crs = {"init": "epsg:4326"}
        return gdf

    def intersect_w_districts(self):
        '''

        :return: A geodataframe with the famine intersected by district.
        '''
        famine = self.rename_columns(self.read_famine_data())

        gdf_districts = self.get_districts()
        #gdf_districts.to_crs(famine)
        famine_district = gpd.overlay(famine, gdf_districts, how='intersection')
        return famine_district

    def add_ids(self):
        '''
        Adds the fact table ids to the table.
        :return: The famine dataframe with the fact table ids.
        '''
        famine_gdf = self.intersect_w_districts()

        # Add dateID
        famine_gdf['dateID'] = famine_gdf['date'].astype(int)

        # Add value & factID per table (occurencies and deaths)
        famine_gdf['value'] = famine_gdf['IPC']
        famine_gdf['measureID'] = 42
        famine_gdf['factID'] = 'FAM' + famine_gdf.index.astype(str)

        # Filter only needed columns to export
        famine_df = famine_gdf[['factID', 'measureID', 'dateID', 'locationID', 'value']]

        return famine_df

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        famine_df = self.add_ids()
        #self.flats.export_csv_w_date(famine_df, 'famine_table')
        #self.flats.export_parquet_w_date(famine_df, 'famine_table')
        self.flats.export_parquet_w_date(famine_df, 'famine_fact/famine_table')


if __name__ == '__main__':

    print("------- Extracting famine vulnerability table ---------")

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    module = cfg['module']
    print(INPUT_PATH)

    famine = FamineTable(INPUT_PATH, OUTPUT_PATH)

    # # Create dataframe
    # famine_df = famine.add_ids()
    # print(famine_df.shape)
    # print(famine_df.columns)
    # print(famine_df.head())

    # Export
    famine.export_files()
