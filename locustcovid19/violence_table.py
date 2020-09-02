# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the violence against civilians table.
Data available from: https://ucdp.uu.se/downloads/index.html#ged_global

Created on Thu Sep 02 12:21:40 2020

@author: ioanna.papachristou@accenture.com
"""

# Imports
import pandas as pd
#from utils.flat_files import FlatFiles
import numpy as np
import geopandas as gpd
from datetime import datetime
from shapely.geometry import Point

#S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

COUNTRIES = ["Kenya", "Somalia", "Ethiopia", "Uganda", "South Sudan", "Sudan"]
COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class ViolenceTable:
    '''
    This class creates the violence against civilians table.
    '''
    def __init__(self, path_in=INPUT_PATH, path_out=OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.violence = pd.read_csv(self.path_in + "social_cohesion/violence/violence.csv", sep=",", encoding='utf-8')

    def coord_to_geometry(self):
        violence_df = self.violence

        # creating a geometry column
        geometry = [Point(xy) for xy in zip(violence_df['longitude'], violence_df['latitude'])]

        # Coordinate reference system : WGS84
        crs = {'init': 'epsg:4326'}

        # Creating a Geographic data frame
        violence_gdf = gpd.GeoDataFrame(violence_df, crs=crs, geometry=geometry)

        return violence_gdf

if __name__ == '__main__':

    print("------- Extracting violence aganinst civilians table ---------")

    violence = ViolenceTable()

    # Create dataframe
    violence_df = violence.coord_to_geometry()
    print(violence_df.shape)
    print(violence_df.head())

    # Export
    #violence_df.export_files()