# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the conflict events table.
Data available from: https://ucdp.uu.se/downloads/index.html#ged_global

Created on Thu Jun 24 13:36:40 2020

@author: ioanna.papachristou@accenture.com
"""

import os
import yaml
import pandas as pd
from utils.flat_files import FlatFiles
import numpy as np
import geopandas as gpd
from datetime import datetime
from shapely import wkt

COUNTRIES = ["Kenya", "Somalia", "Ethiopia", "Uganda", "South Sudan", "Sudan"]
COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class ConflictsTable:
    '''
    This class creates the conflict events table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)

    def load_conflicts(self):
        '''
        Loads conflicts and transforms it to a geodataframe.
        :return: The conflicts geodataframe.
        '''
        conflicts_df = pd.read_csv(self.path_in + "/social_cohesion/conflicts/ged201.csv", sep=",", encoding='utf-8')
        conflicts_df['geometry'] = conflicts_df['geom_wkt'].apply(wkt.loads)
        conflicts_gdf = gpd.GeoDataFrame(conflicts_df, crs='epsg:4326')
        return conflicts_gdf

    def filter_data(self):
        '''
        Filters conflicts countries, year span and columns.
        :return: A geodataframe with the filtered data.
        '''
        conflicts = self.load_conflicts()
        # Filter countries
        conflicts = conflicts[conflicts['country'].isin(COUNTRIES)]

        #Filter years
        conflicts = conflicts[conflicts['year'] >= 2000]

        #Filter columns
        conflicts = conflicts[['id', 'country', 'date_start', 'best', 'geometry']]

        return conflicts

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
        Uses the read_boundary_shp function to read and concatenate all boundaries.
        :return: A geodataframe with all districts of the 6 countries concatenated.
        '''
        all_districts = gpd.GeoDataFrame()
        for country in COUNTRIES_IDS:
            gdf_district = self.read_boundaries_shp(country, 2)
            all_districts = all_districts.append(gdf_district)
        all_districts.crs = {"init": "epsg:4326"}
        return all_districts

    def add_ids(self):
        '''
        Adds the fact tables ids.
        :return:  A filtered dataframe with the columns we need for fact tables.
        '''
        conflicts = self.filter_data()

        # Spatial join with districts and add locationID
        districts = self.get_districts()
        conflicts = gpd.sjoin(districts, conflicts, how='right', op='contains')

        # Add dateID
        conflicts['date'] = pd.to_datetime(conflicts['date_start'])
        conflicts["date"] = [d.date() for d in conflicts["date"]]
        #conflicts['date'] = conflicts['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        conflicts['dateID'] = conflicts['date'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
        conflicts['dateID'] = conflicts['dateID'].astype(int)

        # Add value & factID per table (occurencies and deaths)
        occurencies = conflicts.copy().reset_index(drop=True)
        occurencies['value'] = 1
        occurencies['measureID'] = 32
        occurencies['factID'] = 'CONFL_OCC_' + occurencies.index.astype(str)
        deaths = conflicts.copy().reset_index(drop=True)
        deaths['value'] = deaths['best']
        deaths['measureID'] = 40
        deaths['factID'] = 'CONFL_DEATHS_' + deaths.index.astype(str)

        # Filter only needed columns to export
        occurencies_df = self.flats.select_columns_fact_table(occurencies)
        deaths_df = self.flats.select_columns_fact_table(deaths)

        # Append the 2 tables
        conflicts_df = occurencies_df.append(deaths_df)
        return conflicts_df

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        conflicts_df = self.add_ids()
        #self.flats.export_csv_w_date(conflicts_df, 'conflict_table')
        #self.flats.export_parquet_w_date(conflicts_df, 'conflict_table')
        self.flats.export_to_parquet(conflicts_df, '/conflict_fact/conflict_table')


if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting conflict events table ---------")

    conflicts = ConflictsTable(INPUT_PATH, OUTPUT_PATH)

    # Create dataframe
    conflicts_df = conflicts.add_ids()
    print(conflicts_df.shape)
    print(conflicts_df.head())

    # Export
    conflicts.export_files()
