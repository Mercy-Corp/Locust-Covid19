"""
The aim of this module is to extract the demand table.
The prediction of current demand is based on demand in 2000 and population
values from 2014-2020.

Created on Fri Jul 17 10:59:01 2020
Last modified on Wed Aug 12 09:17:01 2020

@author: linnea.evanson@accenture.com, ioanna.papachristou@accenture.com
"""

import pandas as pd
import numpy as np
from utils_flat_files import FlatFiles
from scipy.optimize import curve_fit
import re
import geopandas as gpd
from rasterstats import zonal_stats
#import boto3
#client = boto3.client('s3')

# #S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

COUNTRIES = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

COMMODITIES = ['beef', 'egg', 'mlk', 'mut', 'pork', 'pou']

COMMODITIES_DICT = {'beef': {'cmdt_name': 'beef consumption', 'id': 22},
                    'egg': {'cmdt_name': 'egg consumption', 'id': 21},
                    'mlk': {'cmdt_name': 'milk consumption', 'id': 20},
                    'mut': {'cmdt_name': 'mutton consumption', 'id': 24},
                    'pork': {'cmdt_name': 'pork consumption', 'id': 25},
                    'pou': {'cmdt_name': 'poultry consumption', 'id': 23}}

class DemandTable:
    '''
    This class creates the demand table.
    '''
    def __init__(self, path_in=INPUT_PATH, path_out=OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(path_in, path_out)

    def load_population(self, year):
        '''

        :param year: The year of the population file.
        :return: The file related to the population we would like to load.
        '''
        population_year = pd.read_csv(self.path_out + "population_fact/population_table_all_countries_" + str(year) + ".csv", sep='|')

        return population_year


    def load_rasters(self, cmdt):
        '''

        :param cmdt: The commodity to load.
        :return: The raster with the population density for the reference country.
        '''
        raster_cmdt = self.path_in + "demand/" + cmdt + "_cons00.tif"

        return raster_cmdt

    def read_boundaries_shp(self, country, hierarchy):
        '''

        :param country: The reference country
        :param hierarchy: The boundaries level, 0 for countries, 1 for regions, 2 for districts.
        :return: A geodataframe with 2 columns: locationID and geometry.
        '''
        gdf_country = gpd.read_file(self.path_in + "Spatial/gadm36_" + country + "_" + str(hierarchy) + ".shp")
        GID_column = 'GID_' + str(hierarchy)
        gdf_country = gdf_country[[GID_column, 'geometry']]
        gdf_country = gdf_country.rename(columns={GID_column: 'locationID'})

        return gdf_country

    def calc_commodity(self, gdf_country, cmdt):
        '''

        :param gdf_country: A geodataframe with the administrative boundaries in a country.
        :param cmdt: The commodity
        :return: The sum of demand per administrative boundary in the selected country.
        '''

        raster_cmdt = self.load_rasters(cmdt)
        demand_country = zonal_stats(gdf_country.geometry, raster_cmdt, layer="polygons", stats='sum')
        return demand_country

    def demand_table(self):
        '''

        :return: the geodataframe with the demand column
        '''
        countries_list = []

        for country in COUNTRIES:
            print("Preparing demand table for {}".format(country))
            gdf_country = self.read_boundaries_shp(country, 1)
            gdf_country['year'] = 2000

            commodities_gdf = gpd.GeoDataFrame()
            for cmdt in COMMODITIES:
                print("Calculating {} commodity...".format(cmdt))
                commodity_gdf = gdf_country
                commodity_gdf['measureID'] = COMMODITIES_DICT[cmdt]['id']
                commodity_gdf['Cons00'] = pd.DataFrame(self.calc_commodity(commodity_gdf, cmdt))
                commodity_gdf['dm_commodity_name'] = COMMODITIES_DICT[cmdt]['cmdt_name']
                commodities_gdf = commodities_gdf.append(commodity_gdf)

            countries_list.append(commodities_gdf)

        demand_gdf = gpd.GeoDataFrame(pd.concat(countries_list, ignore_index=True))

        # For test purposes only:
        #demand = demand_gdf.drop(['geometry'], axis=1)
        #demand.to_csv(self.path_in + 'demand/demand_districts.csv', sep='|', encoding='utf-8', index=False)

        return demand_gdf

    def split_locationID(self,locationID):
        '''
        Splits the locationID into its country, area, district, and region components.

        :param locationID: the locationID for one row.
        :returns country, area, district, and region strings.
        '''
        split = re.split('[_ .]', locationID)  # should have 4 strings
        country = split[0]
        area = split[1]
        district = split[2]
        region = split[3]
        return country, area, district, region

    def sum_regions(self,pop):
        '''
        Function to sum population of all districts within a region.

        :param pop: the file containing population data down to district level for all
        regions in all countries of interest for a particular year.
        :return the sum of the population on a region level and the locationIDs for those regions.
        '''
        pop = pop.dropna()

        splits = np.empty((len(pop['locationID']), 4), dtype=object)
        regions = []
        pop_sums = []
        i = 0
        temp = []
        for line in pop['locationID']:
            splits[i] = self.split_locationID(line)
            regions.append(splits[i][0] + '.' + splits[i][1] + '_' + splits[i][3])
            if i > 0:
                if splits[i][0] == splits[i - 1][0] and splits[i][1] == splits[i - 1][1] and splits[i][3] == \
                        splits[i - 1][3]:
                    temp.append(pop['value'][i])  # keep track of all districts in a region
                else:
                    if temp != []:
                        pop_sums.append(sum(np.float64(temp)))
                        temp = []  # set back to empty
                    else:
                        pop_sums.append(pop['value'][i])  # if the only district in its region
                        temp = []  # set back to empty
            i += 1
        # Get last line:
        lastline = pop['locationID'][len(pop['locationID'])]
        split = self.split_locationID(lastline)
        regions.append(split[0] + '.' + split[1] + '_' + split[3])
        pop_sums.append(pop['value'][i])

        return pop_sums, regions

    def func(self, x, r, initial):
        '''
        The exponential growth function used to predict consumption and population growth.

        :param x: the year for which we want a prediction.
        :param r: the growth rate.
        :param initial: the initial value of consumption
        :return: The consumption after x years.
        '''
        return initial * ((1 + r) ** x)  # same formula used above

    def get_consumption_preds(self,frames):
        '''
        Predicts consumption in 2014-2020 based on population values from the same period, and
        consumption in 2000 as the initial value.

        :param frames: the current dataframe with all locations and consumption types.
        :returns the consumption predictions for 2014-2020 per region and an updated dataframe with no null values.
        '''

        #Get population values on a region level
        pop00, regions = self.sum_regions(self.load_population(2000))
        pop14, regions14 = self.sum_regions(self.load_population(2014))
        pop16, regions16 = self.sum_regions(self.load_population(2016))
        pop17, regions17 = self.sum_regions(self.load_population(2017))
        pop18, regions18 = self.sum_regions(self.load_population(2018))
        pop20, regions20 = self.sum_regions(self.load_population(2020))
        '''
        districts = self.load_population(2020)['locationID'].unique()
        pop00 = self.load_population(2000)['locationID', 'value']
        pop14 = self.load_population(2014)['locationID', 'value']
        pop16 = self.load_population(2016)['locationID', 'value']
        pop17 = self.load_population(2017)['locationID', 'value']
        pop18 = self.load_population(2018)['locationID', 'value']
        pop20 = self.load_population(2020)['locationID', 'value']

        '''
        # Remove duplicates from region tags (regions for each year are the same so we need only take one year):
        all_regions = []
        [all_regions.append(x) for x in regions if x not in all_regions]


        #Fit a curve to the population points using scipy curve_fit
        time = [0, 14, 16, 17, 18, 20]
        curve_params = []
        for region in range(len(pop00)):
            series = [pop00[region], pop14[region], pop16[region], pop17[region], pop18[region], pop20[region]]
            params, cov = curve_fit(self.func, time, series, maxfev=1000)
            curve_params.append(params[0])

        # Work out consumption in 2020:
        demand = frames[pd.notnull(frames["locationID"])]  # remove any columns that are nan (do this after adding IDs)

        region_params = []
        for row in demand['locationID']:  # use demand not frames as we have already removed nan from demand
            index = all_regions.index(row)
            region_params.append(curve_params[index])  # save the curve for that region

        demand['region_params'] = region_params #so their indices match (especially for plotting)

        cons14, cons15, cons16, cons17, cons18, cons19, cons20 = [], [], [], [], [], [], []
        i = 0
        for row in demand['Cons00']:
            cons14.append(self.func(14, region_params[i], row))
            cons15.append(self.func(15, region_params[i], row))
            cons16.append(self.func(16, region_params[i], row))
            cons17.append(self.func(17, region_params[i], row))
            cons18.append(self.func(18, region_params[i], row))
            cons19.append(self.func(19, region_params[i], row))
            cons20.append(self.func(20, region_params[i], row))
            i += 1

        return demand, cons14, cons15, cons16, cons17, cons18, cons19, cons20

    def create_demand_table(self):
        '''
        Create the demand table by filtering input files, creating locationIDs, FactIDs, DateIDs,
        and predictions for consumption per region for the years 2014-2020.

        :return: demand_final: the final dataframe, with a value for consumption for each year.
        '''

        frames = self.demand_table()
        #frames = self.create_locationIDs(frames)

        demand, cons14, cons15, cons16, cons17, cons18, cons19, cons20 = self.get_consumption_preds(frames)

        # Concat each of the predictions for each year with demand so they have location ID and measureID and commodity name
        demand2 = demand[['measureID', 'locationID', 'Cons00', 'dm_commodity_name']]

        df00 = demand2
        df00 = df00.rename(columns={'Cons00': 'value'})

        demand2 = demand2.drop(['Cons00'], axis=1)
        df14 = pd.concat([pd.DataFrame(cons14, columns=['value']), demand2.reset_index(drop=True)], axis=1)
        df15 = pd.concat([pd.DataFrame(cons15, columns=['value']), demand2.reset_index(drop=True)], axis=1)
        df16 = pd.concat([pd.DataFrame(cons16, columns=['value']), demand2.reset_index(drop=True)], axis=1)
        df17 = pd.concat([pd.DataFrame(cons17, columns=['value']), demand2.reset_index(drop=True)], axis=1)
        df18 = pd.concat([pd.DataFrame(cons18, columns=['value']), demand2.reset_index(drop=True)], axis=1)
        df19 = pd.concat([pd.DataFrame(cons19, columns=['value']), demand2.reset_index(drop=True)], axis=1)
        df20 = pd.concat([pd.DataFrame(cons20, columns=['value']), demand2.reset_index(drop=True)], axis=1)

        # Create date IDs for each of the different year predictions:
        date00 = [2000 for i in range(len(df00['value']))]
        df00.insert(1, 'date', date00)
        df00 = self.flats.add_date_id(df00, 'date')

        date14 = [2014 for i in range(len(cons14))]
        df14.insert(1, 'date', date14)
        df14 = self.flats.add_date_id(df14, 'date')

        date15 = [2015 for i in range(len(cons15))]
        df15.insert(1, 'date', date15)
        df15 = self.flats.add_date_id(df15, 'date')

        date16 = [2016 for i in range(len(cons16))]
        df16.insert(1, 'date', date16)
        df16 = self.flats.add_date_id(df16, 'date')

        date17 = [2017 for i in range(len(cons17))]
        df17.insert(1, 'date', date17)
        df17 = self.flats.add_date_id(df17, 'date')

        date18 = [2018 for i in range(len(cons18))]
        df18.insert(1, 'date', date18)
        df18 = self.flats.add_date_id(df18, 'date')

        date19 = [2019 for i in range(len(cons19))]
        df19.insert(1, 'date', date19)
        df19 = self.flats.add_date_id(df19, 'date')

        date20 = [2020 for i in range(len(cons20))]
        df20.insert(1, 'date', date20)
        df20 = self.flats.add_date_id(df20, 'date')

        demand_final = pd.concat([df00, df14, df15, df16, df17, df18, df19, df20])
        demand_final = demand_final.drop(['date'], axis=1)

        # Reorder columns:
        demand_final = demand_final[['measureID', 'dateID', 'locationID', 'value', 'dm_commodity_name']]

        # Insert factID for each row:
        demand_final.insert(0, 'factID', [str('DEF_' + str(i + 1)) for i in
                                          range(len(demand_final['value']))])  # insert at first columns

        self.flats.export_output_w_date(demand_final, "demand_fact/demand_table")

        return demand_final


if __name__ == '__main__':
    print("------- Extracting demand table ---------")

    dem_table = DemandTable()

    # Create dataframe
    demand_df = dem_table.create_demand_table()
    print(demand_df.columns)
    print(demand_df.head())

    # Export files: use utils function to add today's date to the filename
    #FlatFiles().export_output_w_date(demand_final, "demand_table")