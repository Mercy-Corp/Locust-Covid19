"""
The aim of this module is to extract the demand table.
The prediction of current demand is based on demand in 2000 and population
values from 2014-2020.

Created on Fri Jul 17 10:59:01 2020

@author: linnea.evanson@accenture.com
"""

import pandas as pd
import numpy as np
from my_package.utils_flat_files import FlatFiles
from scipy.optimize import curve_fit
import re
#import boto3
#client = boto3.client('s3')

# #S3 paths
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'
#local paths
INPUT_PATH = r'../my_package/data/input/'
OUTPUT_PATH = r'../my_package/data/output/'

class DemandTable:
    '''
    This class creates the 2020 demand table.
    '''
    def __init__(self, path_in=INPUT_PATH, path_out=OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

        self.df_egg = pd.read_excel(self.path_in + 'UA_Eggs_AF.xlsx')
        self.df_milk = pd.read_excel(self.path_in + 'UA_Milk_AF.xlsx')
        self.df_mutt = pd.read_excel(self.path_in + 'UA_Mutt_AF.xlsx')
        self.df_pork = pd.read_excel(self.path_in + 'UA_Pork_AF.xlsx')
        self.df_pou = pd.read_excel(self.path_in + 'UA_Pou_AF.xlsx')
        self.df_beef = pd.read_excel(self.path_in + 'UrbanAreas_Beef_Africa.xlsx')

        self.location = pd.read_csv(self.path_in + 'cities_mapping_all_countries.csv', sep='|')

        self.popfile00 = str(self.path_out + "population_table_2000.csv")
        self.popfile14 = str(self.path_out + "population_table_2014.csv")
        self.popfile16 = str(self.path_out + "population_table_2016.csv")
        self.popfile17 = str(self.path_out + "population_table_2017.csv")
        self.popfile18 = str(self.path_out + "population_table_2018.csv")
        self.popfile20 = str(self.path_out + "population_table_2020.csv")

    def filter_input_tables(self):
        '''
        Filter for desired columns.

        :return: filtered, concatenated table from the 6 input files
        '''
        self.df_egg= self.df_egg.drop(['NEW_URBID','ISO3','Continent','SQKM_FINAL'], axis=1)
        self.df_milk= self.df_milk.drop(['NEW_URBID','ISO3','Continent','SQKM_FINAL','WB_Region_2010'], axis=1)
        self.df_mutt= self.df_mutt.drop(['NEW_URBID','ISO3','Continent','SQKM_FINAL','WB_Region_2010'], axis=1)
        self.df_pork= self.df_pork.drop(['NEW_URBID','ISO3','Continent','SQKM_FINAL','WB_Region_2010'], axis=1)
        self.df_pou= self.df_pou.drop(['NEW_URBID','ISO3','Continent','SQKM_FINAL','WB_Region_2010'], axis=1)
        self.df_beef= self.df_beef.drop(['NEW_URBID','ISO3','Continent','SQKM_FINAL','WB_Region_2010'], axis=1)

        countries=['Ethiopia','Kenya','Somalia','Uganda']

        self.df_egg=self.df_egg[self.df_egg['ADM0_Name'].isin(countries)]
        self.df_milk=self.df_milk[self.df_milk['ADM0_Name'].isin(countries)]
        self.df_mutt=self.df_mutt[self.df_mutt['ADM0_Name'].isin(countries)]
        self.df_pork=self.df_pork[self.df_pork['ADM0_Name'].isin(countries)]
        self.df_pou=self.df_pou[self.df_pou['ADM0_Name'].isin(countries)]
        self.df_beef=self.df_beef[self.df_beef['ADM0_Name'].isin(countries)]

        # Rename columns (Cons00 = Consumption in 2000, Cons30 = Predicted consumption in 2030)
        self.df_egg = self.df_egg.rename(columns={'Egg_Cons00': 'Cons00', 'Egg_Cons30': 'Cons30'})
        self.df_milk = self.df_milk.rename(columns={'Milk_Cons00': 'Cons00', 'Milk_Cons30': 'Cons30'})
        self.df_mutt = self.df_mutt.rename(columns={'Mut_Cons00': 'Cons00', 'Mut_Cons30': 'Cons30'})
        self.df_pork = self.df_pork.rename(columns={'Pork_Cons00': 'Cons00', 'Pork_Cons30': 'Cons30'})
        self.df_pou = self.df_pou.rename(columns={'Pou_Cons00': 'Cons00', 'Pou_Cons30': 'Cons30'})
        self.df_beef = self.df_beef.rename(columns={'Beef_Cons00': 'Cons00', 'Beef_Cons30': 'Cons30'})

        # Insert columns to name type of consumption
        self.df_egg.insert(7, 'dm_commodity_name', 'egg consumption')  # insert column at specified location (column 7 here)
        self.df_milk.insert(7, 'dm_commodity_name', 'milk consumption')
        self.df_mutt.insert(7, 'dm_commodity_name', 'mutton consumption')
        self.df_pork.insert(7, 'dm_commodity_name', 'pork consumption')
        self.df_pou.insert(7, 'dm_commodity_name', 'poultry consumption')
        self.df_beef.insert(7, 'dm_commodity_name', 'beef consumption')

        # Concatenate all food types
        frames = pd.concat([self.df_egg, self.df_milk, self.df_mutt, self.df_pork, self.df_pou, self.df_beef])  # called 'frames' for all dataframes

        #Create measureID
        conditions = [
            (frames['dm_commodity_name'] == 'egg consumption'),
            (frames['dm_commodity_name'] == 'milk consumption'),
            (frames['dm_commodity_name'] == 'mutton consumption'),
            (frames['dm_commodity_name'] == 'pork consumption'),
           (frames['dm_commodity_name'] == 'poultry consumption'),
            (frames['dm_commodity_name'] == 'beef consumption')]

        choices = ['21', '20', '24', '25', '23', '22']
        frames['measureID'] = np.select(conditions, choices) #assign an ID to each of the different types of conditions

        return frames

    def create_locationIDs(self,frames):
        '''
        Map locations within countries to their location ID and insert locationID column.

        :param frames: the dataframe of all filtered and concatenated input files.
        :return: the dataframe with additional column locationID.
        '''
        frames['NAME'] = frames['NAME'].str.lower()
        frames["NAME"] = frames["NAME"].str.strip()
        self.location['City'] = self.location['City'].str.lower()
        self.location["City"] = self.location["City"].str.strip()

        locID = []
        count = 0
        for line in frames['NAME']:  # match names of cities to location IDs
            mask = np.isin(self.location['City'], line)
            if sum(mask):
                flag = 0
                for i in range(len(self.location['City'])):
                    if flag == 1:
                        # nothing
                        0
                    elif self.location['City'][
                        i] == line:  # TODO : map all locations, currently some are removed that do not have a locationID
                        locID.append(self.location['locationID'][i])
                        flag = 1
            else:
                locID.append(np.nan)
            count += 1

        # Add locID to frames
        frames['locationID'] = locID

        return frames

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

    def sum_regions(self,filename):
        '''
        Function to sum population of all districts within a region.

        :param filename: name of the file containing population data down to district level for all
        regions in all countries of interest for a particular year.
        :return the sum of the population on a region level and the locationIDs for those regions.
        '''
        pop = pd.read_csv(filename, sep='|')
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
        Predict consumption in 2014-2020 based on population values from the same period, and
        consumption in 2000 as the initial value

        :param frames: the current dataframe with all locations and consumption types.
        :returns the consumption predictions for 2014-2020 per region and an updated dataframe with no null values.
        '''

        #Get population values on a region level
        pop00, regions = self.sum_regions(self.popfile00)
        pop14, regions14 = self.sum_regions(self.popfile14)
        pop16, regions16 = self.sum_regions(self.popfile16)
        pop17, regions17 = self.sum_regions(self.popfile17)
        pop18, regions18 = self.sum_regions(self.popfile18)
        pop20, regions20 = self.sum_regions(self.popfile20)

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

        frames = self.filter_input_tables()
        frames = self.create_locationIDs(frames)

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
        df00 = FlatFiles().add_date_id(df00, 'date')

        date14 = [2014 for i in range(len(cons14))]
        df14.insert(1, 'date', date14)
        df14 = FlatFiles().add_date_id(df14, 'date')

        date15 = [2015 for i in range(len(cons15))]
        df15.insert(1, 'date', date15)
        df15 = FlatFiles().add_date_id(df15, 'date')

        date16 = [2016 for i in range(len(cons16))]
        df16.insert(1, 'date', date16)
        df16 = FlatFiles().add_date_id(df16, 'date')

        date17 = [2017 for i in range(len(cons17))]
        df17.insert(1, 'date', date17)
        df17 = FlatFiles().add_date_id(df17, 'date')

        date18 = [2018 for i in range(len(cons18))]
        df18.insert(1, 'date', date18)
        df18 = FlatFiles().add_date_id(df18, 'date')

        date19 = [2019 for i in range(len(cons19))]
        df19.insert(1, 'date', date19)
        df19 = FlatFiles().add_date_id(df19, 'date')

        date20 = [2020 for i in range(len(cons20))]
        df20.insert(1, 'date', date20)
        df20 = FlatFiles().add_date_id(df20, 'date')

        demand_final = pd.concat([df00, df14, df15, df16, df17, df18, df19, df20])
        demand_final = demand_final.drop(['date'], axis=1)

        # Reorder columns:
        demand_final = demand_final[['measureID', 'dateID', 'locationID', 'value', 'dm_commodity_name']]

        # Insert factID for each row:
        demand_final.insert(0, 'factID', [str('DEF_' + str(i + 1)) for i in
                                          range(len(demand_final['value']))])  # insert at first columns

        return demand_final

if __name__ == '__main__':
    print("------- Extracting demand table ---------")

    dem_table = DemandTable()

    # Create dataframe
    demand_df = dem_table.create_demand_table()

    # Export files: use utils function to add today's date to the filename
    out = FlatFiles().export_output_w_date(demand_final, "Demand")
