# This file is executed when the module is run from the command line:
# python -m my_package
from location_table import LocationTable
from shapefile_table import ShapefileTable
from production_table import ProductionTable
from population_table import PopulationTable
from utils_flat_files import FlatFiles

import os

# S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'

# #local paths
# INPUT_PATH = r'data/input/'
# OUTPUT_PATH = r'data/output/'

if __name__ == '__main__':

    # path = r'./data/'
    # # Create Input and Output folder structure
    # directories = [path + "input", path + "output"]
    # for directory in directories:
    #     if not os.path.exists(directory):
    #         os.makedirs(directory)

    # 1. Creation of location table

    print("------- Extracting location table ---------")

    loc_table = LocationTable(INPUT_PATH, OUTPUT_PATH)
    location_table = loc_table.concat_sub_tables()      # Create geodataframe
    loc_table.export_to_parquet(location_table, 'location_table')       # Export table to parquet format

    # 2. Creation of shapefile table

    print("------- Extracting shapefile table ---------")

    shp_table = ShapefileTable(INPUT_PATH, OUTPUT_PATH)
    gdf_all = shp_table.concat_sub_tables()
    shp_table.export_to_shp(gdf_all, 'shapefile_table')     # Export table to shp

    # 3. Creation of production table

    print("------- Extracting production table ---------")
    #Load class
    prod_table = ProductionTable(INPUT_PATH, OUTPUT_PATH)
    # Create dataframe
    production_df = prod_table.add_ids_to_table()
    # Export
    flatfiles = FlatFiles(INPUT_PATH, OUTPUT_PATH)
    flatfiles.export_output_w_date(production_df, 'production_table')

    # 4. Creation of population table

    print("------- Extracting population table ---------")

    pop_table = PopulationTable(INPUT_PATH, OUTPUT_PATH)
    # Create dataframe
    population_df = pop_table.add_ids_to_table()
    # Export
    flatfiles.export_output(population_df, 'population_table_2015')
