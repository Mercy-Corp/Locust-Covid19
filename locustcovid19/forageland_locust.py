# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the forageland area affected by locust per district.
Forageland data source: http://www.fao.org/geonetwork/srv/en/main.search?title=land%20cover
Locust data source: https://locust-hub-hqfao.hub.arcgis.com/datasets/swarms-1/data?geometry=-91.702%2C-8.959%2C143.142%2C46.416&showData=true

Created on Wed Jul 15 08:54:40 2020
Last update Wed Aug 26 17:58:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import os
import pandas as pd
import geopandas as gpd
import geopandas
import yaml
from utils.flat_files import FlatFiles
from rasterstats import zonal_stats
import warnings
warnings.filterwarnings("ignore")

COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class ForagelandLocust:
    '''
    This class calculates the forageland area affected by locust per district.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.dates = pd.read_csv(self.path_out + '/Date_Dim/Date_Dim.csv', sep=",")
        self.dates['date'] = pd.to_datetime(self.dates['date'], format = '%m/%d/%Y')
        self.flats = FlatFiles(path_in, path_out)

        # # Import forageland vector
        # self.forageland_v = gpd.read_file(self.path_in + "forageland/forageland_vector.shp")
        # self.forageland_v.crs = {"init": "epsg:4326"}

        # Forageland 2003 raster path
        self.raster_path = self.path_in + '/forageland/forageland2003.tif'

        # Import locust gdf
        self.locust_gdf = gpd.read_file(self.path_in + "/swarm/Swarm_Master.shp")
        #print(self.locust_gdf.COUNTRYID.unique()) # to select new countries

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

        :return: A geodataframe with all districts of the 4 countries concatenated.
        '''
        gdf_districts = gpd.GeoDataFrame()
        for country in COUNTRIES_IDS:
            gdf_district = self.read_boundaries_shp(country, 2)
            gdf_districts = gdf_districts.append(gdf_district)
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
        selected_countries = ['SO', 'KE', 'ET', 'UG', 'SU', 'SS']
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
            df_group = gpd.GeoDataFrame(geometry=[geoms])
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
        Intersects the buffers with the districts.
        :return: A gdf with locust affected districts.
        '''
        gdf_districts = self.get_districts()
        gdf_districts['area_districts'] = gdf_districts.geometry.area
        locust_buffers_gdf = self.loc_buffers_to_gdf()

        # intersect with districts
        locust_distr = gpd.overlay(locust_buffers_gdf, gdf_districts, how='intersection')
        # intersect with forageland
        #forage_locust_district = gpd.overlay(self.forageland_v, locust_distr, how='intersection')

        return locust_distr

    def area_districts_affected_locust(self):
        '''
        Calculates the area affected by locust
        :return: A gdf including the area in degrees
        '''
        locust_district = self.intersect()
        locust_district['area_locust'] = locust_district.geometry.area

        return locust_district

    def load_forageland_area(self):
        forageland_area = pd.read_parquet(self.path_out + '/forageland_fact/forageland.parquet', engine='pyarrow')[['locationID', 'value']]
        #forageland_area = pd.read_parquet(self.path_out + 'forageland_fact/forageland.parquet', engine='pyarrow')[['locationID', 'value']]
        forageland_area = forageland_area.rename(columns={'value': 'forageland_area'})
        return forageland_area

    def calc_forageland_locust(self):
        '''
        Calculates the forageland area affected by locust per district.
        :param raster: The geotiff indicating the cropland area
        :return: A df with two columns, district id and forageland area affected by locust.
        '''
        districts_locust = self.area_districts_affected_locust()
        districts_forageland = self.load_forageland_area()
        stats = zonal_stats(districts_locust.geometry, self.raster_path,  layer="polygons", stats="count", categorical=True)
        districts_locust['forageland_locust_count'] = pd.DataFrame.from_dict(stats)[1] # 1: count of pixels with value == 1, foragelands.
        districts_locust['locust_count'] = pd.DataFrame.from_dict(stats)["count"] # count of the total number of pixels in the affected area by locust
        districts_locust = districts_locust[districts_locust['forageland_locust_count'].notnull()] #drop nulls
        districts_locust['forageland_area_locust'] = districts_locust['forageland_locust_count'] * districts_locust['area_locust'] / districts_locust[
            'locust_count']

        districts_locust = districts_locust.merge(self.load_forageland_area(), on = 'locationID', how = 'left')

        # Correct DQ issues with some of the prices (detected in Ethiopia)
        districts_locust.loc[(districts_locust['forageland_area_locust'] > districts_locust['forageland_area']), 'forageland_area_locust'] = districts_locust.loc[(districts_locust['forageland_area_locust'] > districts_locust['forageland_area']), 'forageland_area']

        #Filter columns
        districts_locust_df = districts_locust[['locationID', 'forageland_area_locust', 'date']]

        return districts_locust_df

    def add_fact_ids(self):
        '''
        Adds ids of fact tables
        :return: A df with the standardised columns of fact tables.
        '''
        forage_locust_district = self.calc_forageland_locust()
        forage_locust_district['measureID'] = 29
        forage_locust_district['factID'] = 'FOR_LOC_DIS' + forage_locust_district.index.astype(str)
        forage_locust_district['value'] = forage_locust_district['forageland_area_locust']

        # Add dateID
        forageland_loc_gdf = forage_locust_district.merge(self.dates, on='date', how='left')

        # Select fact table columns
        forageland_loc_df = self.flats.select_columns_fact_table(df=forageland_loc_gdf)

        return forageland_loc_df

    def export_table(self, filename):
        '''
        Exports result table
        :return: The Forageland table in both a parquet and csv format with the date added in the name.
        '''
        forageland_loc_df = self.add_fact_ids()
        self.flats.export_to_parquet(forageland_loc_df, filename)
        #self.flats.export_csv_w_date(forageland_loc_df, filename)

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting forageland area affected by locust per district table ---------")
    ForagelandLocust(INPUT_PATH, OUTPUT_PATH).export_table('/forageland_locust_fact/forage_impact_locust_district')
    #ForagelandLocust(INPUT_PATH, OUTPUT_PATH).export_table('forage_impact_locust_district')
