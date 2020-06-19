# This file is executed when the module is run from the command line:
# python -m my_package
from my_package.example import greet
from my_package.location_table import LocationTable
from my_package.shapefile_table import ShapefileTable
import os

if __name__ == '__main__':
    # A dummy main
    print('Running module')

    greet('developer')

    print("It worked!")

    path = r'./data/'
    # Create Input and Output folder structure
    directories = [path + "data/input", path + "data/output"]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # 1. Creation of location table

    print("------- Extracting location table ---------")

    loc_table = LocationTable()
    location_table = loc_table.concat_sub_tables()      # Create geodataframe
    loc_table.export_to_parquet(location_table, 'location_table')       # Export table to parquet format

    # 2. Creation of shapefile table

    print("------- Extracting shapefile table ---------")

    shp_table = ShapefileTable()
    gdf_all = shp_table.concat_sub_tables()
    shp_table.export_to_shp(gdf_all, 'shapefile_table')     # Export table to shp