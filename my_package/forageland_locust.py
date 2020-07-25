# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the forageland area affected by locust per district.

Created on Wed Jul 15 08:54:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
import geopandas as gpd
import geopandas
from utils_flat_files import FlatFiles

#S3 paths
# INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
# OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

class ForagelandLocust:
    '''
    This class calculates the forageland area affected by locust per district.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.dates = pd.read_csv(self.path_out + 'date_23_06-2020.csv', sep=",")
        self.dates['date'] = pd.to_datetime(self.dates['date'], format = '%d-%m-%Y')

        # Import districts
        self.shp2_Kenya = gpd.read_file(self.path_in + "gadm36_KEN_2.shp")[['GID_2', 'geometry']]
        self.shp2_Somalia = gpd.read_file(self.path_in + "gadm36_SOM_2.shp")[['GID_2', 'geometry']]
        self.shp2_Ethiopia = gpd.read_file(self.path_in + "gadm36_ETH_2.shp")[['GID_2', 'geometry']]
        self.shp2_Uganda = gpd.read_file(self.path_in + "gadm36_UGA_2.shp")[['GID_2', 'geometry']]

        # Import forageland vector
        self.forageland_v = gpd.read_file(self.path_in + "forageland/forageland_vector.shp")
        self.forageland_v.crs = {"init": "epsg:4326"}

        # Import locust gdf
        self.locust_gdf = gpd.read_file(self.path_in + "Swarm_Master.shp")

    def get_districts(self):
        '''

        :return: A geodataframe with all districts of the 4 countries concatenated.
        '''
        district_level = [self.shp2_Kenya, self.shp2_Ethiopia, self.shp2_Somalia, self.shp2_Uganda]
        gdf_districts = gpd.GeoDataFrame(pd.concat(district_level, ignore_index=True))
        gdf_districts.crs = {"init": "epsg:4326"}
        return gdf_districts

    def filter_data(self):
        '''

        :return: A gdf filtered by countries, dates and columns.
        '''

        locust_gdf = self.locust_gdf
        # Filter dates
        locust_gdf['STARTDATE'] = pd.to_datetime(locust_gdf['STARTDATE'], format='%Y-%m-%d')
        locust_gdf_filtered = locust_gdf[(locust_gdf['STARTDATE'] > pd.Timestamp(2000, 1, 1)) & (locust_gdf['STARTDATE'] < pd.Timestamp.today())]

        # Filter countries
        selected_countries = ['SO', 'KE', 'ET', 'UG']
        locust_gdf_filtered = locust_gdf_filtered[locust_gdf_filtered.COUNTRYID.isin(selected_countries)]

        # Filter columns
        locust_gdf_filtered = locust_gdf_filtered[
            ['OBJECTID', 'STARTDATE', 'LOCNAME', 'AREAHA', 'LOCRELIAB', 'COUNTRYID', 'LOCUSTID', 'REPORTID', 'ACOMMENT',
             'LOCPRESENT', 'geometry']]

        return locust_gdf_filtered

    def calc_buffer(self):
        '''

        :return: A gdf with locust buffers of 25km reprojected and calculated in degrees.
        '''
        locust_gdf_filtered = self.filter_data()
        # Reproject to calculate in meters
        cpr_gdf = locust_gdf_filtered.to_crs({'init': 'epsg:32636'})
        buffer_length_in_meters = (25 * 1000) # buffer: 25km
        cpr_gdf['geometry'] = cpr_gdf.geometry.buffer(buffer_length_in_meters)
        cpr_gdf['area_m'] = cpr_gdf.geometry.area

        # Change again system to calculate in degrees
        cpr_gdf = cpr_gdf.to_crs({'init': 'epsg:4326'})
        cpr_gdf['area_d'] = cpr_gdf.geometry.area
        cpr_gdf.head(3)

        return cpr_gdf

    def groupby_month(self):
        '''

        :return: A gdf grouped by month and year
        '''

        locust_grouped = self.calc_buffer()
        locust_grouped.index = pd.to_datetime(locust_grouped['STARTDATE'], format='%Y-%m-%d')
        locust_grouped = locust_grouped[['OBJECTID', 'geometry']]
        locust_grouped = locust_grouped.groupby(by=[locust_grouped.index.month, locust_grouped.index.year])
        return locust_grouped

    def overlapping_buffers(self):
        '''
        Unions the overlapping buffers grouped by month and year.
        :return: A df with the final buffer geometries and dates
        '''
        locust_grouped = self.groupby_month()

        # Initialise empty df
        locust_buffers = pd.DataFrame()

        #Iterate over groups
        for group_name, df_group in locust_grouped:
            # Union of overlapping buffers
            geoms = df_group.geometry.unary_union
            df_group = geopandas.GeoDataFrame(geometry=[geoms])
            df_group = df_group.explode().reset_index(drop=True)

            # add date column
            df_group['date'] = str(group_name[1]) + '-' + str(group_name[0]) + '-01'
            df_group['date'] = pd.to_datetime(df_group['date'], format='%Y-%m-%d')

            # append to dataframe
            locust_buffers = locust_buffers.append(df_group)

        return locust_buffers

    def loc_buffers_to_gdf(self):
        '''
        Transforms df to gdf.
        :return: A gdf of the locust final buffers.
        '''
        locust_buffer = self.overlapping_buffers()
        crs = {'init': 'epsg:4326'}
        geometry = locust_buffer['geometry']
        locust_buffers_gdf = gpd.GeoDataFrame(locust_buffer, crs=crs, geometry=geometry)
        return locust_buffers_gdf

    def intersect(self):
        '''
        Intersects the buffers with the districts and the forageland.
        :return: A gdf with locust affected foragelands per district
        '''
        gdf_districts = self.get_districts()
        locust_buffers_gdf = self.loc_buffers_to_gdf()

        # intersect with districts
        locust_distr = gpd.overlay(locust_buffers_gdf, gdf_districts, how='intersection')
        # intersect with forageland
        forage_locust_district = gpd.overlay(self.forageland_v, locust_distr, how='intersection')

        return forage_locust_district

    def area_forage_affected_locust(self):
        '''
        Calculates the area affected by locust
        :return: A gdf including the area in degrees
        '''
        forage_locust_district = self.intersect()
        forage_locust_district['forage_locust_area'] = forage_locust_district.geometry.area

        return forage_locust_district

    def add_fact_ids(self):
        '''
        Adds ids of fact tables
        :return: A df with the standardised columns of fact tables.
        '''
        forage_locust_district = self.area_forage_affected_locust()
        forage_locust_district['measureID'] = 29
        forage_locust_district['factID'] = 'FOR_LOC_DIS' + forage_locust_district.index.astype(str)
        forage_locust_district['locationID'] = forage_locust_district['GID_2']
        forage_locust_district['value'] = forage_locust_district['forage_locust_area']

        # Add dateID
        forageland_loc_gdf = forage_locust_district.merge(self.dates, on='date', how='left')

        # Select fact table columns
        forageland_loc_df = FlatFiles().select_columns_fact_table(df=forageland_loc_gdf)

        return forageland_loc_df

    def export_table(self, filename):
        '''

        :return: The Forageland table in both a parquet and csv format with the date added in the name.
        '''
        forageland_loc_df = self.add_fact_ids()
        FlatFiles(INPUT_PATH, OUTPUT_PATH).export_output_w_date(forageland_loc_df, filename)

if __name__ == '__main__':

    print("------- Extracting forageland area affected by locust per district table ---------")
    ForagelandLocust().export_table('Forage_impact_locust_district')


