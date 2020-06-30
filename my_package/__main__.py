# This file is executed when the module is run from the command line:
# python -m my_package
from my_package.location_table import LocationTable
from my_package.shapefile_table import ShapefileTable
from my_package.production_table import ProductionTable

import os

if __name__ == '__main__':

    path = r'./data/'
    # Create Input and Output folder structure
    directories = [path + "input", path + "output"]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # 1. Creation of location table

    print("------- Extracting location table ---------")

    loc_table = LocationTable(path)
    location_table = loc_table.concat_sub_tables()      # Create geodataframe
    loc_table.export_to_parquet(location_table, 'location_table')       # Export table to parquet format

    # 2. Creation of shapefile table

    print("------- Extracting shapefile table ---------")

    shp_table = ShapefileTable(path)
    gdf_all = shp_table.concat_sub_tables()
    shp_table.export_to_shp(gdf_all, 'shapefile_table')     # Export table to shp

    # 3. Creation of production table

    print("------- Extracting production table ---------")
    #Load class
    prod_table = ProductionTable(path)
    # Create dataframe
    production_df = prod_table.add_ids_to_table()
    # Export
    prod_table.export(production_df)