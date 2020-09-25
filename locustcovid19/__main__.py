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
from price_table import PricesTable
from displacement_table import DisplacementTable
from refugees_table import RefugeesTable
from conflicts_table import ConflictsTable
from violence_table import ViolenceTable
from famine_table import FamineTable
from risk_indicators import RiskTables
from vegetation_index import VegetationTable
from financial_inclusion import FinancialInclusion
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
       LocationTable(INPUT_PATH, OUTPUT_PATH).export_to_parquet('location_table')
       #LocationTable(INPUT_PATH, OUTPUT_PATH).export_to_csv('location_table')

    elif module == 'shapefile': 
   
       # 2. Creation of shapefile table
       print("------- Extracting shapefile table ---------")

       shp_table = ShapefileTable(INPUT_PATH, OUTPUT_PATH)
       gdf_all = shp_table.concat_sub_tables()
       shp_table.export_to_shp(gdf_all, 'shapefile_table')     # Export table to shp

    elif module == 'production':

       # 3. Creation of production table
       print("------- Extracting production table ---------")
       ProductionTable(INPUT_PATH, OUTPUT_PATH).export_files()

    elif module == 'population':

       # 4. Creation of population table
       print("------- Extracting population tables ---------")
       years = [2020]

       for year in years:
           print("Population {}:".format(year))
           PopulationTable(year, INPUT_PATH, OUTPUT_PATH).export_population()

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
       DemandTable(INPUT_PATH, OUTPUT_PATH).create_demand_table()

    elif module == 'price':

       print("------- Extracting prices table ---------")
       #prices = PricesTable().filter_prices()
       #prices = PricesTable().location_id_to_markets()
       PricesTable(INPUT_PATH, OUTPUT_PATH).export_table('/price_fact/price_table')

    elif module == 'cropland':

       # 7. Calculation of cropland
       print("------- Extracting cropland area per district table ---------")
       Cropland(INPUT_PATH, OUTPUT_PATH).export_table("/cropland_fact/cropland")

    elif module == 'forageland':

       # 8. Calculation of forageland
       print("------- Extracting forageland area per district table ---------")
       Forageland(INPUT_PATH, OUTPUT_PATH).export_table("/forageland_fact/forageland")

    elif module == 'foragelandlocust':

       # 9. Calculation of forageland affected by locust
       print("------- Extracting forageland area affected by locust per district table ---------")
       ForagelandLocust(INPUT_PATH, OUTPUT_PATH).export_table('/forageland_locust_fact/forage_impact_locust_district')

    elif module == 'croplandlocust':

       # 10 Calculation of cropland affected by locust
       print("------- Extracting cropland area affected by locust per district table ---------")
       CroplandLocust(INPUT_PATH, OUTPUT_PATH).export_table('/cropland_locust_fact/crops_impact_locust_district')

    elif module == 'displacements':
       print("------- Extracting displacements table ---------")
       DisplacementTable(INPUT_PATH, OUTPUT_PATH).export_files()

    elif module == 'refugees':

       print("------- Extracting refugees table ---------")
       RefugeesTable(INPUT_PATH, OUTPUT_PATH).export_files()

    elif module == 'conflicts':

       print("------- Extracting conflict events table ---------")
       ConflictsTable(INPUT_PATH, OUTPUT_PATH).export_files()

    elif module == 'violence':

       print("------- Extracting violence against civilians table ---------")
       ViolenceTable(INPUT_PATH, OUTPUT_PATH).export_files()

    elif module == 'famine':

       print("------- Extracting famine vulnerability table ---------")
       FamineTable(INPUT_PATH, OUTPUT_PATH).export_files()

    elif module == 'risk.locust':

       print("------- Extracting locust risk table ---------")
       RiskTables(INPUT_PATH, OUTPUT_PATH).export_files('locust')

    elif module == 'risk.rvf':

       print("------- Extracting RVF risk table ---------")
       RiskTables(INPUT_PATH, OUTPUT_PATH).export_files('RVF')

    elif module == 'vegetation':

       print("------- Extracting vegetation index per district table ---------")
       VegetationTable(INPUT_PATH, OUTPUT_PATH).export_table()

    elif module == 'fin_inclusion':

       print("------- Extracting financial inclusion table ---------")
       FinancialInclusion(INPUT_PATH, OUTPUT_PATH).export_files()

    else:

       print('Invalid module: {}'.format(module))
