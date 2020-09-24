# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the NDVI vegetation growth indicator per district.
Data source: https://earlywarning.usgs.gov/fews/product/448

Created on Mon Sep 10 11:16:40 2020
Latest update on Fri Sep 18 13:11:40 2020

@author: ioanna.papachristou@accenture.com
"""

import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.mask import mask
from utils.flat_files import FlatFiles
from rasterstats import zonal_stats
from utils.s3_local import s3_local
from utils.s3_latest import s3_latest
from utils.removefile import removefile
import numpy as np
import copy
import os
import re
import yaml

COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class VegetationTable:
    '''
    This class calculates the avg NDVI vegetation index per district for a given date.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)
        self.localdir = '/home/ec2-user/Locust-Covid19/'
          
        try:
           os.makedirs(self.localdir + 'vegetation')
           print("Directory created ")
        except FileExistsError:
           print("Directory already exists")


        filepath = s3_latest(self.path_in, 'vegetation', 'ea')
        print("filepath")
        print(filepath)
        filename = filepath.split('/')[-1]
        self.filename = filename
        print(filename)
        self.period = str(re.findall('\d+', filename)[0])
        print('period')
        print(self.period)
        newname = filename.replace('.tif', '_out.tif')
        print(newname)
        # NDVI raster path
        self.raster_path = s3_local(self.path_in, 'vegetation/' + filename, self.localdir)
        self.raster_path_out = self.localdir + 'vegetation/' + newname

    def clip_raster(self):
        '''
        Clips the raster based on a vector
        :return: A clipped raster
        '''
        # Load merged countries' boundaries
        countries = self.get_all_countries(1)
        #rdf = gpd.GeoDataFrame(pd.concat(dataframesList, ignore_index=True))

        data = rasterio.open(self.raster_path)

        out_img, out_transform = mask(data, shapes=countries.geometry, crop=True)

        # Copy the metadata
        out_meta = data.meta.copy()
        print(out_meta)

        # Parse EPSG code
        epsg_code = int(data.crs.data['init'][5:])
        print(epsg_code)

        # Update the metadata with new dimensions, transform (affine) and CRS (as Proj4 text)
        out_meta.update({"driver": "GTiff",
                         "height": out_img.shape[1],
                         "width": out_img.shape[2],
                         "transform": out_transform,
                         "crs": pycrs.parser.from_epsg_code(epsg_code).to_proj4()}
                        )

        # save the clipped raster to disk
        with rasterio.open(out_tif, "w", **out_meta) as dest:
            dest.write(out_img)

    def filter_n_norm_raster(self):
        # Filter raster
        print('Starts filter_n')
        with rasterio.open(self.raster_path, 'r+') as raster:
            print("... filtering and normalising raster.")
            ndvi = raster.read()  # read all raster values
            # transform values < 100 and > 200 to nan
            ndvi_filtered = copy.copy(ndvi)
            ndvi_filtered[ndvi < 100] = 0.0
            ndvi_filtered[ndvi > 200] = 0.0
            ndvi_filtered = ndvi_filtered.astype('float32')
            ndvi_filtered[ndvi_filtered == 0.0] = np.nan

            # normalise values
            # x normalized = (x – x minimum) / (x maximum – x minimum)
            ndvi_normed = (ndvi_filtered - 100) / (200 - 100)

            # Replace nan with 0s
            #ndvi_final = np.nan_to_num(ndvi_normed, copy=True, nan=0.0, posinf=None, neginf=None)
            ndvi_final = ndvi_normed

            # Save new raster
            number_of_bands, height, width = ndvi_final.shape
            raster_new = rasterio.open(self.raster_path_out, 'w',
                                       driver='GTiff',
                                       height=height,
                                       width=width,
                                       count = number_of_bands,
                                       dtype = ndvi_final.dtype,
                                       nodata=0,
                                       crs=raster.crs,
                                       transform=raster.transform,
                                       compress='lzw')
            raster_new.write(ndvi_final)
            raster_new.close()

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

    def get_all_countries(self, hierarchy):
        '''

        :param hierarchy: The boundaries level, 0 for countries, 1 for regions, 2 for districts.
        :return: A geodataframe with all districts of the 6 countries concatenated.
        '''

        gdf_countries = gpd.GeoDataFrame()
        for country in COUNTRIES_IDS:
            gdf_country = self.read_boundaries_shp(country, hierarchy)
            gdf_countries = gdf_countries.append(gdf_country)
        gdf_countries.crs = {"init": "epsg:4326"}
        return gdf_countries

    def get_stats(self):
        '''

        :return: A df with two columns, district id and cropland area.
        '''
        # Filter and normalise raster
        print("Calls filter")
        self.filter_n_norm_raster()

        gdf_districts = self.get_all_countries(2)

        print("... calculating zonal statistics.")
        stats = zonal_stats(gdf_districts.geometry, self.raster_path_out,  layer="polygons", stats="mean") # Cross districts with normalised ndvi
        gdf_districts['avg_ndvi'] = pd.DataFrame(stats)
        #Filter columns
        df_districts = gdf_districts[['locationID', 'avg_ndvi']]

        return df_districts

    def extract_date(self):
        '''
        Extracts year and period from filename.
        :return: A string of the year and the period.
        '''
        base = os.path.basename(self.raster_path)
        filename = os.path.splitext(base)[0]
        file_number = re.sub("[^0-9]", "", filename)
        file_year = file_number[:2]
        year = str(2000 + int(file_year))

        file_period = file_number[2:]

        return year, file_period

    def add_fact_ids(self):
        '''
        Adds the fact tables ids.
        :return:  A filtered dataframe by the columns we need for fact tables.
        '''
        vegetation_district = self.get_stats()
        # Adding measureID
        vegetation_district['measureID'] = 45
        # Adding factID
        vegetation_district = vegetation_district.reset_index(drop=True)
        vegetation_district['factID'] = 'VEG_' + vegetation_district.index.astype(str)
        # Adding dateID
        vegetation_district['year'], vegetation_district['period'] = self.extract_date()

        replacements = {'01': '0101', '02': '0111', '03': '0121', '04': '0201', '05': '0211', '06': '0221', '07': '0301',
                        '08': '0311', '09': '0321', '10': '0401', '11': '0411', '12': '0421', '13': '0501', '14': '0511',
                        '15': '0521', '16': '0601', '17': '0611', '18': '0621', '19': '0701', '20': '0711', '21': '0721',
                        '22': '0801', '23': '0811', '24': '0821', '25': '0901', '26': '0911', '27': '0921', '28': '1001',
                        '29': '1011', '30': '1021', '31': '1101', '32': '1111', '33': '1121', '34': '1201', '35': '1211',
                        '36': '1221'}

        vegetation_district['period'].replace(replacements, inplace=True)
        vegetation_district['date'] = vegetation_district['year'] + vegetation_district['period']
        vegetation_district['dateID'] = vegetation_district['date'].astype(int)

        #Adding value column
        vegetation_district['value'] = vegetation_district['avg_ndvi']

        # Select fact table columns
        vegetation_df = self.flats.select_columns_fact_table(df=vegetation_district)

        return vegetation_df

    def export_table(self):
        '''

        :return: The Vegetation index table in a parquet format with the period added in its name.
        '''
        forageland_df = self.add_fact_ids()
        self.flats.export_to_parquet(forageland_df, '/vegetation_fact/vegetation_table_' + self.period)


if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting vegetation index per district table ---------")

    VegetationTable(INPUT_PATH, OUTPUT_PATH).export_table()
