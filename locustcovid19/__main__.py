"""
This file is executed when the module is run from the command line.
This file runs the following:
\n-Extracts location table-
\n-Extracts shapefile table-
\n-Extracts production table-
\n-Extracts population tables-
\n-Extracts measures table-
\n-Extracts demand table-

"""
# python -m my_package
from location_table import LocationTable
from shapefile_table import ShapefileTable
from production_table import ProductionTable
from population_table import PopulationTable
from utils.flat_files import FlatFiles
from measure_table import MeasuresTable
from demand_table import DemandTable
from cropland_area import Cropland
from forageland_area import Forageland
from forageland_locust import ForagelandLocust
from cropland_locust import CroplandLocust
import os
import yaml

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    module = cfg['module']
    print(INPUT_PATH)

    if module == 'location':

       # 1. Creation of location table
       print("------- Extracting location table ---------")

       loc_table = LocationTable(INPUT_PATH, OUTPUT_PATH)
       loc_table.export_to_parquet('location_table')       # Export table to parquet format

    elif module == 'shapefile': 
   
       # 2. Creation of shapefile table
       print("------- Extracting shapefile table ---------")

       shp_table = ShapefileTable(INPUT_PATH, OUTPUT_PATH)
       gdf_all = shp_table.concat_sub_tables()
       shp_table.export_to_shp(gdf_all, 'shapefile_table')     # Export table to shp

    elif module == 'production':

       # 3. Creation of production table
       print("------- Extracting production table ---------")
       #Load class
       prod_table = ProductionTable(INPUT_PATH, OUTPUT_PATH)
       # Export
       prod_table.export_files()

    elif module == 'population':

       # 4. Creation of population table
       print("------- Extracting population tables ---------")
       years = [2000, 2014, 2015, 2016, 2017, 2018, 2020]

       for year in years:
           print("Population {}:".format(year))
           PopulationTable(year).export_population()

    elif module == 'measure':    

       # 5. Creation of measures table
       print("------- Extracting measure table ---------")
       #Load class
       measure_table = MeasuresTable(INPUT_PATH,OUTPUT_PATH)
       #Create dataframe
       measures_df = measure_table.measures_df
       #Export
       FlatFiles().export_to_parquet(measures_df,"measures")

    elif module == 'demand':

       # 6. Creation of demand table
       print("------- Extracting demand table ---------")
       #Load class
       demand_table = DemandTable(INPUT_PATH,OUTPUT_PATH)
       #Create dataframe
       demand_df = demand_table.create_demand_table()
       #Export
       FlatFiles().export_to_parquet(demand_df,"demand")
       # out = FlatFiles().export_output_w_date(demand_df, "Demand")

    elif module == 'cropland':

       # 7. Calculation of cropland
       print("------- Extracting cropland area per district table ---------")
       Cropland(INPUT_PATH, OUTPUT_PATH).export_table("cropland_fact/Cropland")

    elif module == 'forageland':

       # 8. Calculation of forageland
       print("------- Extracting forageland area per district table ---------")
       Forageland(INPUT_PATH, OUTPUT_PATH).export_table("forageland_fact/Forageland")

    elif module == 'foragelandlocust':

       # 9. Calculation of forageland affected by locust
       print("------- Extracting forageland area affected by locust per district table ---------")
       ForagelandLocust(INPUT_PATH, OUTPUT_PATH).export_table('forageland_locust_fact/Forage_impact_locust_district')

    elif module == 'croplandlocust':

       # 10 Calculation of cropland affected by locust
       print("------- Extracting cropland area affected by locust per district table ---------")
       CroplandLocust(INPUT_PATH, OUTPUT_PATH).export_table('cropland_locust_fact/Crops_impact_locust_district')

    elif module == 'violence':

       # 11 Calculation of cropland affected by locust
       print("------- Extracting violence against civilians table ---------")
       ViolenceTable(INPUT_PATH, OUTPUT_PATH).export_files()

    else:

       print('Invalid module: {}'.format(module))
