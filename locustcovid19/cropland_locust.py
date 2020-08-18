# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the cropland area affected by locust per district.
Locust data source: https://locust-hub-hqfao.hub.arcgis.com/datasets/swarms-1/data?geometry=-91.702%2C-8.959%2C143.142%2C46.416&showData=true


Created on Wed Jul 15 11:29:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
import geopandas as gpd
import geopandas
from datetime import datetime
from rasterstats import zonal_stats
from utils.flat_files import FlatFiles
import glob
import warnings
import yaml
warnings.filterwarnings("ignore")
import boto3
client = boto3.client('s3')

#S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-reporting/'

#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

RASTER_NAMES = ["N00E30", "S10E40", "S10E30", "S10E20", "N10E50", "N10E40", "N10E30", "N00E50", "N00E40", "N00E20", "N20E30", "N20E20", "N10E20"] #if project extended to more countries, their corresponding geotiffs refering to croplands could be added here in the list
#RASTER_NAMES = ["S10E30", "S10E20", "N10E50", "N10E40", "N10E30", "N00E50", "N00E40", "N00E20", "N20E30", "N20E20", "N10E20"]

class CroplandLocust:
    '''
    This class calculates the cropland area affected by locust per district.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        #self.dates = pd.read_csv(self.path_out + 'date_23_06-2020.csv', sep=",")
        #self.dates['date'] = pd.to_datetime(self.dates['date'], format = '%d-%m-%Y')
        self.flats = FlatFiles(self.path_in, self.path_out)

        # Import districts
        self.shp2_Kenya = gpd.read_file(self.path_in + "Spatial/gadm36_KEN_2.shp")[['GID_2', 'geometry']]
        self.shp2_Somalia = gpd.read_file(self.path_in + "Spatial/gadm36_SOM_2.shp")[['GID_2', 'geometry']]
        self.shp2_Ethiopia = gpd.read_file(self.path_in + "Spatial/gadm36_ETH_2.shp")[['GID_2', 'geometry']]
        self.shp2_Uganda = gpd.read_file(self.path_in + "Spatial/gadm36_UGA_2.shp")[['GID_2', 'geometry']]
        self.shp2_Sudan = gpd.read_file(self.path_in + "Spatial/gadm36_SDN_2.shp")[['GID_2', 'geometry']]
        self.shp2_SSudan = gpd.read_file(self.path_in + "Spatial/gadm36_SSD_2.shp")[['GID_2', 'geometry']]

        # # Import cropland vector
        # self.crops = gpd.read_file(self.path_in + "cropland/Crops_vectorized.shp")

        # Import locust gdf
        self.locust_gdf = gpd.read_file(self.path_in + "swarm/Swarm_Master.shp")

    # def filter_crops(self):
    #     '''
    #
    #     :return: The crops vector filtered by the crops id.
    #     '''
    #
    #     crops = self.crops
    #     crops = crops[crops['Crops'] == 1]
    #     crops.Crops = 12
    #     return crops

    def get_districts(self):
        '''

        :return: A geodataframe with all districts of the 4 countries concatenated.
        '''
        district_level = [self.shp2_Kenya, self.shp2_Ethiopia, self.shp2_Somalia, self.shp2_Uganda, self.shp2_Sudan,
                          self.shp2_SSudan]
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
        locust_gdf_filtered = locust_gdf_filtered[['OBJECTID', 'STARTDATE', 'geometry']]

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

        :return: A gdf grouped by month and year.
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
            df_group['date'] = '01-' + str(group_name[0]) + '-' + str(group_name[1])
            df_group['date'] = pd.to_datetime(df_group['date'], format='%d-%m-%Y')

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
        locust_buffers_gdf = self.loc_buffers_to_gdf()
        #crops_v = self.filter_crops()

        # intersect with districts
        locust_distr = gpd.overlay(locust_buffers_gdf, gdf_districts, how='intersection')
        ## intersect with cropland
        #crops_locust_district = gpd.overlay(crops_v, locust_distr, how='intersection')

        return locust_distr

    def area_districts_affected_locust(self):
        '''
        Calculates the area affected by locusts per district.
        :return: A gdf including the area in degrees.
        '''
        locust_district = self.intersect()
        locust_district['area'] = locust_district.geometry.area

        return locust_district

    def get_stats(self, raster):
        '''
        Calculates the croplands affected by locust per district, using the method count of zonal_stats.
        :param raster: The geotiff indicating the cropland area
        :return: A df with two columns, district id and cropland area.
        '''

        raster_path = self.path_in + "cropland/GFSAD30AFCE_2015_" + raster + "_001_2017261090100.tif"
        locust_distr = self.area_districts_affected_locust()

        stats = zonal_stats(locust_distr.geometry, raster_path, stats="count", categorical=True)

        if raster == "N00E50":
            try:
                locust_distr['croplands_count'] = pd.DataFrame.from_dict(stats)[1]
            except:
                print("Raster " + raster + " does not contain any croplands impacted by locust for the selected districts and dates.")
                return
        elif raster in RASTER_NAMES:
            try:
                locust_distr['croplands_count'] = pd.DataFrame.from_dict(stats)[2]
            except:
                print("Raster " + raster + " does not contain any croplands impacted by locust for the selected districts and dates.")
                return
        else:
            print("Raster not in RASTER_NAMES.")
            return

        # Calculating the area affected by locust per district
        locust_distr['area_count'] = pd.DataFrame.from_dict(stats)["count"]
        locust_distr = locust_distr[locust_distr['croplands_count'].notnull()]
        locust_distr['crops_locust_area'] = locust_distr['croplands_count'] * locust_distr['area'] / locust_distr[
            'area_count']
        #print(locust_distr.columns)
        crops_locust_district = locust_distr[['GID_2', 'crops_locust_area', 'date']]
        file_name = "/cropland/crops_locust_distr_" + raster
        crops_locust_district.to_csv(self.path_in + file_name + '.csv', sep='|', encoding='utf-8', index=False)
        print(raster + " exported.")
        return crops_locust_district

    def extract_crops_locust(self):
        '''
        Function to run all zonal statistics for all rasters by calling get_stats.
        :return: An appended df of all zonal statistics.
        '''
        df = pd.DataFrame()

        for raster in RASTER_NAMES:
            print(raster + " raster being processed.")
            df_of_raster = self.get_stats(raster)
            df = df.append(df_of_raster)

        return df

    def load_extracted_crops(self):
        '''
        Loads all intermediate files (csvs) on zonal statistics per raster.
        :return: A concatenated df of all zonal statistics.
        '''
        resp = client.list_objects_v2(Bucket='mercy-locust-covid19-in-dev')
        keys = []
        all_files = []
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        for i in keys:
            if 'inbound/sourcedata/cropland/crops_locust_distr' in i:
              s = 's3://mercy-locust-covid19-in-dev/' + str(i)
              all_files.append(s)
        #all_files = glob.glob(self.path_in + "cropland/crops_locust_distr_*" + ".csv")
        #mercy-locust-covid19-in-dev/inbound/sourcedata/cropland/crops_locust_distr_*.csv
        df_from_each_file = (pd.read_csv(f, sep = "|") for f in all_files)
        print(all_files)
        concatenated_df = pd.concat(df_from_each_file, ignore_index=True)
        return concatenated_df

    # def area_crops_affected_locust(self):
    #     '''
    #     Calculates the area affected by locust
    #     :return: A gdf including the area in degrees
    #     '''
    #     crops_locust_district = self.intersect()
    #     crops_locust_district['crops_locust_area'] = crops_locust_district.geometry.area
    #
    #     return crops_locust_district

    def add_fact_ids(self):
        '''
        Adds fact tables' ids to our df.
        :return: A df with the standardised columns of fact tables.
        '''
        crops_locust_district = self.load_extracted_crops()
        crops_locust_district['measureID'] = 30
        crops_locust_district['factID'] = 'CROP_LOC_DIS' + crops_locust_district.index.astype(str)
        crops_locust_district['locationID'] = crops_locust_district['GID_2']
        crops_locust_district['value'] = crops_locust_district['crops_locust_area']
        #crops_locust_district['date'] = pd.to_datetime(crops_locust_district['date'], format = '%Y-%m-%d')

        crops_locust_district['date'] = crops_locust_district['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        crops_locust_district['dateID'] = crops_locust_district['date'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
        crops_locust_district['dateID'] = crops_locust_district['dateID'].astype(int) #Athena accepts a bigint to prepare the view
        #print(crops_locust_district[['date', 'dateID']].head())

        # Add dateID
        #crops_loc_df = crops_locust_district.merge(self.dates, on='date', how='left')

        # Select fact table columns
        crops_locust_district = crops_locust_district[['factID', 'measureID', 'dateID', 'locationID', 'value']]

        return crops_locust_district

    def export_table(self, filename):
        '''

        :return: The Cropland table in both a parquet and csv format with the date added in the name.
        '''
        crops_loc_df = self.add_fact_ids()
        self.flats.export_parquet_w_date(crops_loc_df, filename)
        #self.flats.export_csv_w_date(crops_loc_df, filename) #only for testing purposes

if __name__ == '__main__':

    with open("config/application.yaml", "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg["data"]['landing']
    OUTPUT_PATH = cfg["data"]['reporting']
    print(INPUT_PATH)


    print("------- Extracting cropland area affected by locust per district table ---------")
    crop_loc = CroplandLocust()
#    crop_loc.load_extracted_crops()
    crop_loc.extract_crops_locust()
    #crop_loc.export_table('Crops_impact_locust_district') #local export
    crop_loc.export_table('cropland_locust_fact/Crops_impact_locust_district') #export to S3


