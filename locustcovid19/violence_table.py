# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the violence against civilians table.
Data available from: https://acleddata.com/data-export-tool/

Created on Thu Sep 02 12:21:40 2020

@author: ioanna.papachristou@accenture.com
"""

import os
import pandas as pd
import geopandas as gpd
from utils.flat_files import FlatFiles
from datetime import datetime
from shapely.geometry import Point
import time
import yaml

COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class ViolenceTable:
    '''
    This class creates the violence against civilians table.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)
        self.violence = pd.read_csv(self.path_in + '/social_cohesion/violence/violence.csv', sep=',',
                                    encoding='utf-8')[['event_date', 'latitude', 'longitude', 'fatalities', 'timestamp']]

    def coord_to_geometry(self):
        '''
        Transforms coordinates to geometry.
        :return: A geodataframe with the geometry column.
        '''
        violence_df = self.violence

        # creating a geometry column
        geometry = [Point(xy) for xy in zip(violence_df['longitude'], violence_df['latitude'])]

        # Coordinate reference system : WGS84
        crs = {'init': 'epsg:4326'}

        # Creating a Geographic data frame
        violence_gdf = gpd.GeoDataFrame(violence_df, crs=crs, geometry=geometry)

        return violence_gdf

    def read_boundaries_shp(self, country, hierarchy):
        '''
        Reads the boundary files for the selected country and hierarchy.
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
        Uses the read_boundary_shp function to read and concatenate all boundaries
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
        :return:  A filtered dataframe by the columns we need for fact tables.
        '''
        violence = self.coord_to_geometry()

        # Spatial join with districts and add locationID
        districts = self.get_districts()
        violence = gpd.sjoin(districts, violence, how='right', op='contains')

        # Add dateID
        violence['date'] = pd.to_datetime(violence['event_date'])
        violence['date'] = [d.date() for d in violence['date']]
        violence['dateID'] = violence['date'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
        violence['dateID'] = violence['dateID'].astype(int)

        # Add value & factID per table (occurencies and deaths)
        occurencies = violence.copy().reset_index(drop=True)
        occurencies['value'] = 1
        occurencies['measureID'] = 31
        occurencies['factID'] = 'VIOL_OCC_' + occurencies.index.astype(str)
        deaths = violence.copy().reset_index(drop=True)
        deaths['value'] = deaths['fatalities']
        deaths['measureID'] = 43
        deaths['factID'] = 'VIOL_DEATHS_' + deaths.index.astype(str)

        # Filter only needed columns to export
        occurencies_df = occurencies[['factID', 'measureID', 'dateID', 'locationID', 'value']]
        deaths_df = deaths[['factID', 'measureID', 'dateID', 'locationID', 'value']]

        # Append the 2 tables
        violence_df = occurencies_df.append(deaths_df)
        return violence_df

    def export_files(self):
        '''
        Exports to parquet format.
        '''
        violence_df = self.add_ids()
        #self.export_csv_w_date(violence_df, 'violence_table')
        #self.export_parquet_w_date(violence_df, 'violence_table')
        self.flats.export_to_parquet(violence_df, '/violence_fact/violence_table')

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)
    
    print("------- Extracting violence against civilians table ---------")

    violence = ViolenceTable(INPUT_PATH, OUTPUT_PATH)

    # Create dataframe
    #violence_df = violence.add_ids()
    #print(violence_df.shape)
    #print(violence_df.columns)
    #print(violence_df.head())

    # Export
    violence.export_files()
