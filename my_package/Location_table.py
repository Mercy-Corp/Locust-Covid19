# -*- coding: utf-8 -*-
"""
The aim of this module is to prepare the location table.

Created on Thu Jun 18 09:16:40 2020

@author: ioanna.papachristou@accenture.co
"""

from pathlib import Path
import pandas as pd
import geopandas as gpd
from fastparquet import ParquetFile

PATH = r'./data/'


def import_shapefile(country, hierarchy):
    shp = gpd.read_file(PATH + "input/gadm36" + str(country) + str(hierarchy) + ".shp")
    return shp

def shapefiles_list(hierarchy):

    if hierarchy == 0:
        Kenya_0 = import_shapefile(country="_KEN_", hierarchy=hierarchy)
        Ethiopia_0 = import_shapefile(country="_ETH_", hierarchy=hierarchy)
        Somalia_0 = import_shapefile(country="_SOM_", hierarchy=hierarchy)
        gdf_list = [Kenya_0, Ethiopia_0, Somalia_0]

    elif hierarchy == 1:
        Kenya_1 = import_shapefile(country="_KEN_", hierarchy=hierarchy)
        Ethiopia_1 = import_shapefile(country="_ETH_", hierarchy=hierarchy)
        Somalia_1 = import_shapefile(country="_SOM_", hierarchy=hierarchy)
        gdf_list = [Kenya_1, Ethiopia_1, Somalia_1]

    elif hierarchy == 2:
        Kenya_2 = import_shapefile(country="_KEN_", hierarchy=hierarchy)
        Ethiopia_2 = import_shapefile(country="_ETH_", hierarchy=hierarchy)
        Somalia_2 = import_shapefile(country="_SOM_", hierarchy=hierarchy)
        gdf_list = [Kenya_2, Ethiopia_2, Somalia_2]

    else:
        Kenya_3 = import_shapefile(country="_KEN_", hierarchy=hierarchy)
        Ethiopia_3 = import_shapefile(country="_ETH_", hierarchy=hierarchy)
        gdf_list = [Kenya_3, Ethiopia_3]

    return gdf_list

def create_sub_tables(shp_list, hierarchy):
    # Concatenate shp
    gdf = gpd.GeoDataFrame(pd.concat(shp_list, ignore_index=True, sort=False))
    # Add new columns
    gdf["locationID"] = gdf["GID_" + str(hierarchy)]
    gdf["name"] = gdf["NAME_" + str(hierarchy)]
    gdf["hierarchy"] = hierarchy
    gdf["type"] = ""
    if hierarchy == 0:
        gdf["type"] = "Country"
    elif hierarchy == 1:
        gdf["type"] = "Region1"
    elif hierarchy == 2:
        gdf["type"] = "Region2"
    elif hierarchy == 3:
        gdf["type"] = "Region3"
    return gdf


class LocationTable:
    def __init__(self):

        self.path = PATH
        self.gdf_countries_list = shapefiles_list(0)
        self.gdf_regions1_list = shapefiles_list(1)
        self.gdf_regions2_list = shapefiles_list(2)
        self.gdf_regions3_list = shapefiles_list(3)
        self.countries = create_sub_tables(self.gdf_countries_list, 0)
        self.countries = self.countries[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0"]]
        self.regions1 = create_sub_tables(self.gdf_regions1_list, 1)
        self.regions1 = self.regions1[["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1"]]
        self.regions2 = create_sub_tables(self.gdf_regions2_list, 2)
        self.regions2 = self.regions2[
            ["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1", "GID_2", "NAME_2"]]
        self.regions3 = create_sub_tables(self.gdf_regions3_list,3)
        self.regions3 = self.regions3[
            ["locationID", "name", "hierarchy", "type", "GID_0", "NAME_0", "GID_1", "NAME_1", "GID_2", "NAME_2",
             "GID_3",
             "NAME_3"]]

    def concat_sub_tables(self):
        gdf_all_list = [self.countries, self.regions1, self.regions2, self.regions3]
        gdf_all = gpd.GeoDataFrame(pd.concat(gdf_all_list, ignore_index=True))
        gdf_all = gdf_all[
            ['locationID', 'name', 'type', 'hierarchy', 'GID_0', 'GID_1', 'GID_2', 'GID_3', 'NAME_0', 'NAME_1',
             'NAME_2',
             'NAME_3']]
        return gdf_all

    def export_to_parquet(self, gdf, file_name):
        gdf.to_parquet(self.path+file_name+".parquet", compression='uncompressed')
        print("Location table extracted")



if __name__ == '__main__':

    print("------- Extracting location table ---------")
    loc_table = LocationTable()
    gdf_all = loc_table.concat_sub_tables()
    # Export table to parquet
    loc_table.export_to_parquet(gdf_all, 'output/location_table')
