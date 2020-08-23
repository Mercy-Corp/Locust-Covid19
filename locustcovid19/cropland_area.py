# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the cropland area per district.

Created on Mon Jul 13 11:16:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
import geopandas as gpd
from utils.flat_files import FlatFiles
from rasterstats import zonal_stats
import glob
import boto3
import yaml
client = boto3.client('s3')
import warnings
warnings.filterwarnings("ignore")

#S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-landing/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-reporting/'

#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

#with open("config/application.yaml", "r") as ymlfile:
#    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

#INPUT_PATH = cfg["data"]['landing']
#OUTPUT_PATH = cfg["data"]['reporting']
#print(INPUT_PATH)

RASTER_NAMES = ["N00E30", "S10E40", "S10E30", "S10E20", "N10E50", "N10E40", "N10E30", "N00E50", "N00E40", "N00E20", "N20E30", "N20E20", "N10E20"] #if project extended to more countries, their corresponding geotiffs refering to croplands could be added here in the list
#RASTER_NAMES = ["N10E30", "N00E50", "N00E40", "N00E20", "N20E30", "N20E20", "N10E20"]

class Cropland:
    '''
    This class calculates the cropland area per district.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)

        # Import districts
        self.shp2_Kenya = gpd.read_file(self.path_in + "Spatial/gadm36_KEN_2.shp")[['GID_2', 'geometry']]
        self.shp2_Somalia = gpd.read_file(self.path_in + "Spatial/gadm36_SOM_2.shp")[['GID_2', 'geometry']]
        self.shp2_Ethiopia = gpd.read_file(self.path_in + "Spatial/gadm36_ETH_2.shp")[['GID_2', 'geometry']]
        self.shp2_Uganda = gpd.read_file(self.path_in + "Spatial/gadm36_UGA_2.shp")[['GID_2', 'geometry']]
        self.shp2_Sudan = gpd.read_file(self.path_in + "Spatial/gadm36_SDN_2.shp")[['GID_2', 'geometry']]
        self.shp2_SSudan = gpd.read_file(self.path_in + "Spatial/gadm36_SSD_2.shp")[['GID_2', 'geometry']]

        # Import cropland vector
        #self.crops = gpd.read_file(self.path_in + "crops/Crops_vectorized.shp")


    def get_districts(self):
        '''
        
        :return: A geodataframe with all districts of the 4 countries concatenated.
        '''
        district_level = [self.shp2_Kenya, self.shp2_Ethiopia, self.shp2_Somalia, self.shp2_Uganda, self.shp2_Sudan, self.shp2_SSudan]
        gdf_districts = gpd.GeoDataFrame(pd.concat(district_level, ignore_index=True))
        gdf_districts['area'] = gdf_districts.geometry.area
        gdf_districts.crs = {"init": "epsg:4326"}
        return gdf_districts

    def get_stats(self, raster):
        '''

        :param raster: The geotiff indicating the cropland area
        :return: A df with two columns, district id and cropland area.
        '''
        print("Processing raster "+ raster)
        raster_path = self.path_in + "cropland/GFSAD30AFCE_2015_" + raster + "_001_2017261090100.tif"
        gdf_districts = self.get_districts()

        stats = zonal_stats(gdf_districts.geometry, raster_path,  layer="polygons", stats="count", categorical=True)

        if raster == "N00E50":
            gdf_districts['croplands_count'] = pd.DataFrame.from_dict(stats)[1]
        #else:
         #   gdf_districts['croplands_count'] = pd.DataFrame.from_dict(stats)[2]

        elif raster in RASTER_NAMES:
            try:
                gdf_districts['croplands_count'] = pd.DataFrame.from_dict(stats)[2]
            except:
                print("Raster " + raster + " does not contain any croplands impacted by locust for the selected districts and dates.")
                return
        else:
            print("Raster not in RASTER_NAMES.")
            return

        #print(raster)
        gdf_districts['area_count'] = pd.DataFrame.from_dict(stats)["count"]
        gdf_districts = gdf_districts[gdf_districts['croplands_count'].notnull()]
        gdf_districts['croplands_area'] = gdf_districts['croplands_count'] * gdf_districts['area'] / gdf_districts[
            'area_count']

        df_districts = gdf_districts[['GID_2', 'croplands_area']]
        file_name = "/cropland/crops_" + raster
        df_districts.to_csv(self.path_in + file_name + '.csv', sep='|', encoding='utf-8', index=False)
        print(raster + " raster exported.")

        return df_districts

    def extract_crops(self):
        '''
        Function to run all zonal statistics for all rasters by calling get_stats.
        :return: An appended df of all zonal statistics.
        '''
        df = pd.DataFrame()

        for raster in RASTER_NAMES:
            df_of_raster = self.get_stats(raster)
            df = df.append(df_of_raster)

        return df

    def load_extracted_crops(self):
        '''
        Loads all intermediate files (csvs) on zonal statistics per raster.
        :return: A concatenated df of all zonal statistics.
        '''

        resp = client.list_objects_v2(Bucket='mercy-locust-covid19-landing')
        keys = []
        all_files = []
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        for i in keys:
            if 'cropland/crops_' in i:
              s = 's3://mercy-locust-covid19-landing/' + str(i)
              all_files.append(s)

        df_from_each_file = (pd.read_csv(f, sep = "|") for f in all_files)
        concatenated_df = pd.concat(df_from_each_file, ignore_index=True)
        return concatenated_df


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

    # def crops_district(self):
    #     '''
    #
    #     :return: A geodataframe with the crops intersected by district.
    #     '''
    #     crops = gpd.GeoDataFrame(self.filter_crops())
    #     gdf_districts = self.get_districts()
    #     crops_district = gpd.overlay(crops, gdf_districts, how='intersection')
    #     return crops_district
    #
    # def crops_area(self):
    #     '''
    #
    #     :return: A geodataframe with the area of crops per district calculated.
    #     '''
    #     crops_district = gpd.GeoDataFrame(self.crops_district())
    #     crops_district['area_inter'] = crops_district.geometry.area
    #     return crops_district

    def add_fact_ids(self):
        '''
        Adds the fact tables ids
        :return:  A filtered dataframe by the columns we need for fact tables.
        '''
        crops_district = self.load_extracted_crops()
        crops_district['measureID'] = 27
        crops_district['factID'] = 'CROP_' + crops_district.index.astype(str)
        crops_district['year'] = 2015
        crops_district['locationID'] = crops_district['GID_2']
        crops_district['value'] = crops_district['croplands_area']

        # Add dateID
        crops_gdf = self.flats.add_date_id(crops_district, column = 'year')

        # Select fact table columns
        crops_df = self.flats.select_columns_fact_table(df = crops_gdf)
        
        return crops_df

    def export_table(self, filename):
        '''

        :return: The Cropland table in a parquet format with the date added in the name.
        '''
        crops_df = self.add_fact_ids()
        self.flats.export_parquet_w_date(crops_df, filename)
        #self.flats.export_csv_w_date(crops_df, filename) #only for testing purposes
        
if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg["data"]['landing']
    OUTPUT_PATH = cfg["data"]['reporting']
    print(INPUT_PATH)

    print("------- Extracting cropland area per district table ---------")
    #Call the class
    cropland_area = Cropland()
    #Extract all zonal statistics per raster
    cropland_area.extract_crops()
    #Load zonal statistics and prepare & export the cropland fact table
    #cropland_area.export_table("Cropland") #local export
    cropland_area.export_table("cropland_fact/Cropland") #export to S3
