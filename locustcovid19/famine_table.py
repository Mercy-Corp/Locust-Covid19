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
import os
from boto3 import client
client = client('s3')

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

#        path_in = 's3://mercy-locust-covid19-landing-test'
        path_in = str(self.path_in)

        resp = client.list_objects_v2(Bucket=path_in[5:], Prefix='famine')
#        resp = client.list_objects_v2(Bucket='mercy-locust-covid19-landing')
        keys = []
        all_files = []
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        for i in keys:
            if 'famine/EA_' in i:
              if '.shp' in i and '.shp.xml' not in i:
                 s = path_in + '/' + str(i)
                 all_files.append(s)

#        print('all_files loop')

        for file in all_files:
            # Split by "_"
            print("... Reading file: " + file)
            date, file_type = file.split('.')[0].split('_')[1:]
#            print('first line')
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
        gdf_country = gpd.read_file(self.path_in + "/Spatial/gadm36_" + country + "_" + str(hierarchy) + ".shp")
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
        print("... Intersecting IPC indicator with districts.")
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

        # Filter out values > 5 indicating parks, water, no data. Include only IPC values.
        famine_filtered = famine_df[famine_df['value'] <= 5]

        return famine_filtered

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        famine_df = self.add_ids()
        #self.flats.export_csv_w_date(famine_df, 'famine_table')
        self.flats.export_to_parquet(famine_df, '/famine_fact/famine_table')


if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)
    print("------- Extracting famine vulnerability table ---------")

    famine = FamineTable(INPUT_PATH, OUTPUT_PATH)

    # # Create dataframe
    # famine_df = famine.add_ids()
    # print(famine_df.shape)
    # print(famine_df.columns)
    # print(famine_df.head())

    # Export
    famine.export_files()
