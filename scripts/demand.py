"""
The aim of this module is to extract the demand table.

Created on Fri Jul 10 17:34:50 2020

@author: linnea.evanson@accenture.com
"""


import pandas as pd
import numpy as np
from utils_flat_files import FlatFiles

# #S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'
#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

class DemandTable:
    '''
    This class creates the 2020 demand table.
    '''
    def __init__(self, path_in=INPUT_PATH, path_out=OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

        self.df_egg = pd.read_excel(INPUT_PATH + 'UA_Eggs_AF.xlsx')
        self.df_milk = pd.read_excel(INPUT_PATH + 'UA_Milk_AF.xlsx')
        self.df_mutt = pd.read_excel(INPUT_PATH + 'UA_Mutt_AF.xlsx')
        self.df_pork = pd.read_excel(INPUT_PATH + 'UA_Pork_AF.xlsx')
        self.df_pou = pd.read_excel(INPUT_PATH + 'UA_Pou_AF.xlsx')
        self.df_beef = pd.read_excel(INPUT_PATH + 'UrbanAreas_Beef_Africa.xlsx')


    def filter_input_tables(self): #Filter for desired columns
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
        self.df_egg = self.df_egg.rename(columns={'Egg_Cons00': 'Cons00', 'Egg_Cons30': 'Cons30', 'Egg_UA_T': 'UA_T'})
        self.df_milk = self.df_milk.rename(columns={'Milk_Cons00': 'Cons00', 'Milk_Cons30': 'Cons30', 'Mlk_UA_T': 'UA_T'})
        self.df_mutt = self.df_mutt.rename(columns={'Mut_Cons00': 'Cons00', 'Mut_Cons30': 'Cons30', 'Mut_UA_T': 'UA_T'})
        self.df_pork = self.df_pork.rename(columns={'Pork_Cons00': 'Cons00', 'Pork_Cons30': 'Cons30', 'Pork_UA_T': 'UA_T'})
        self.df_pou = self.df_pou.rename(columns={'Pou_Cons00': 'Cons00', 'Pou_Cons30': 'Cons30', 'Pou_UA_T': 'UA_T'})
        self.df_beef = self.df_beef.rename(columns={'Beef_Cons00': 'Cons00', 'Beef_Cons30': 'Cons30', 'Beef_UA_T': 'UA_T'})

        # Insert columns to name type of consumption
        self.df_egg.insert(7, 'dm_commodity_name', 'egg consumption')  # insert column at specified location (column 7 here)
        self.df_milk.insert(7, 'dm_commodity_name', 'milk consumption')
        self.df_mutt.insert(7, 'dm_commodity_name', 'mutton consumption')
        self.df_pork.insert(7, 'dm_commodity_name', 'pork consumption')
        self.df_pou.insert(7, 'dm_commodity_name', 'poultry consumption')
        self.df_beef.insert(7, 'dm_commodity_name', 'beef consumption')

        # Concatenate all food types
        frames = pd.concat([self.df_egg, self.df_milk, self.df_mutt, self.df_pork, self.df_pou, self.df_beef])  # called 'frames' for all dataframes
        return frames

    def create_demand_table(self):
        frames = self.filter_input_tables()

        #Fit exponential growth on the two data points Cons00 and Cons30,
        #interpolate the amount consumed this year.

        initial = frames['Cons00']
        final = frames['Cons30']

        r = ((final/initial) ** (1/30) ) - 1
        x20 = initial * ((1+ r)**20)

        #Add this to frames:
        frames['value'] = x20 #value is current consumption

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

        #Now we need to map locations within countries to their location ID.
        location=pd.read_csv(INPUT_PATH + 'cities_mapping_all_countries.csv', sep='|')
        frames['NAME']= frames['NAME'].str.lower()
        frames["NAME"]= frames["NAME"].str.strip()
        location['City']= location['City'].str.lower()
        location["City"]= location["City"].str.strip()

        locID = []
        count = 0
        for line in frames['NAME']: #match names of cities to location IDs
            mask = np.isin(location['City'],line)
            if sum(mask):
                    flag = 0
                    for i in range(len(location['City'])):
                        if flag == 1:
                            #nothing
                            0
                        elif location['City'][i] == line: #TODO : map all locations, currently some are removed that do not have a locationID
                            locID.append(location['locationID'][i])
                            flag = 1
            else:
                locID.append(np.nan)
            count += 1

        #Add locID to frames
        frames['locationID'] = locID

        #Create a DateID, and factID using utils functions
        frames['date'] = [2020 for i in range(len(frames['value']))]
        frames = FlatFiles().add_date_id(frames,'date')

        frames['factID'] = [str('DEF_'+str(i+1)) for i in range(len(frames['value']))]

        #Create final dataframe with desired columns
        demand = frames[['factID','measureID','dateID','locationID','value','dm_commodity_name']]

        demand = demand[pd.notnull(demand["locationID"])] #remove any columns that are nan (do this after adding IDs)
        return demand

    if __name__ == '__main__':
        print("------- Extracting demand table ---------")

        dem_table = DemandTable()

        # Create dataframe
        demand_df = dem_table.create_demand_table()

        #Export files
        demand.to_csv(OUTPUT_PATH + 'Demand_csv.csv', index = False)
        demand.to_parquet(OUTPUT_PATH + 'Demand_parquet.parquet', index = False)

