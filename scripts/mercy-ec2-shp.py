# -*- coding: utf-8 -*-
"""
@author: rashmi.upreti@accenture.com
"""

import pandas as pd
import geopandas as gpd
# Read East Africa geo location data from S3 bucket
gdf1 = gpd.read_file("s3://mercy-locust-covid19-in-dev/inbound/east-africa202003 (1)/EA_202003_ML1.shp")
gdf2 = gpd.read_file("s3://mercy-locust-covid19-in-dev/inbound/gadm36_ETH_shp/gadm36_ETH_0.shp")


# Merge the both the data set into one
gdf = gpd.GeoDataFrame(pd.concat([gdf1, gdf2]))

print(gdf)

# Make the concated data set into one result shape file 
gdfw =gdf.to_file("s3://mercy-locust-covid19-in-dev/outbound/mercy-corp-shape-result/")








