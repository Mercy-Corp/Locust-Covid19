# -*- coding: utf-8 -*-
"""
The aim of this module is to list functions related to shapefiles.

Created on Thu Jun 19 11:14:40 2020

@author: ioanna.papachristou@accenture.com
"""

import geopandas as gpd
import pandas as pd

PATH = r'./data/'

class Shapefiles:
    '''
    Functions related to shapefiles.
    '''
    def __init__(self, hierarchy):
        '''

        :param hierarchy: 0 for the country level, 1-3 for the rest of regional levels
        '''
        self.path = PATH

        if hierarchy > 3 or hierarchy < 0:
            raise ValueError("'hierarchy' should be an integer between 0 and 3, where 0 refers to the country level.")
        else:
            self.hierarchy = hierarchy

    def import_shapefile(self, country):
        '''
        Imports a shp with the boundaries.
        :param country: the referring country of the shp. Choose between: "_KEN_", "_ETH_", "_SOM_" or "_UGA_"
        :param hierarchy: 0 for the country level, 1-3 for the rest of regional levels
        :return: A shapefile
        '''
        shp = gpd.read_file(self.path + "input/gadm36" + str(country) + str(self.hierarchy) + ".shp")
        return shp

    def shapefiles_list(self):
        '''
        Creates a list of shapefiles according to hierarchy.
        :return: A list of geodataframes for all three countries (KEN, ETH, SOM) per hierarchy.
        '''

        if self.hierarchy == 0:
            Kenya_0 = self.import_shapefile(country="_KEN_")
            Ethiopia_0 = self.import_shapefile(country="_ETH_")
            Somalia_0 = self.import_shapefile(country="_SOM_")
            Uganda_0 = self.import_shapefile(country="_UGA_")
            gdf_list = [Kenya_0, Ethiopia_0, Somalia_0, Uganda_0]

        elif self.hierarchy == 1:
            Kenya_1 = self.import_shapefile(country="_KEN_")
            Ethiopia_1 = self.import_shapefile(country="_ETH_")
            Somalia_1 = self.import_shapefile(country="_SOM_")
            Uganda_1 = self.import_shapefile(country="_UGA_")
            gdf_list = [Kenya_1, Ethiopia_1, Somalia_1, Uganda_1]

        elif self.hierarchy == 2:
            Kenya_2 = self.import_shapefile(country="_KEN_")
            Ethiopia_2 = self.import_shapefile(country="_ETH_")
            Somalia_2 = self.import_shapefile(country="_SOM_")
            Uganda_2 = self.import_shapefile(country="_UGA_")
            gdf_list = [Kenya_2, Ethiopia_2, Somalia_2, Uganda_2]

        elif self.hierarchy == 3:
            Kenya_3 = self.import_shapefile(country="_KEN_")
            Ethiopia_3 = self.import_shapefile(country="_ETH_")
            Uganda_3 = self.import_shapefile(country="_UGA_")
            gdf_list = [Kenya_3, Ethiopia_3, Uganda_3]

        return gdf_list

    def create_sub_tables(self, shp_list):
        """
        Creates sub-tables per hierarchy to be used for the final location table.
        :param shp_list: A list of shapefiles
        :return: A geodataframe of all location data for the selected hierarchy
        """
        # Concatenate shp
        gdf = gpd.GeoDataFrame(pd.concat(shp_list, ignore_index=True, sort=False))
        # Add new columns

        gdf["locationID"] = gdf["GID_" + str(self.hierarchy)]
        gdf["name"] = gdf["NAME_" + str(self.hierarchy)]
        gdf["hierarchy"] = self.hierarchy
        gdf["type"] = ""
        if self.hierarchy == 0:
            gdf["type"] = "Country"
        elif self.hierarchy == 1:
            gdf["type"] = "Region1"
        elif self.hierarchy == 2:
            gdf["type"] = "Region2"
        elif self.hierarchy == 3:
            gdf["type"] = "Region3"
        return gdf

if __name__ == '__main__':

    print("------- Extracting lists of shapefiles geodataframes ---------")
    countries_list = Shapefiles(0).shapefiles_list()
    regions1_list = Shapefiles(1).shapefiles_list()
    regions2_list = Shapefiles(2).shapefiles_list()
    regions3_list = Shapefiles(3).shapefiles_list()
    #regions4 = Shapefiles(4).shapefiles_list() #only for testing, should raise value error
    #print(regions3)

    print("------- Extracting geodataframes ---------")
    countries = Shapefiles(0).create_sub_tables(countries_list)
    print(countries.head())

