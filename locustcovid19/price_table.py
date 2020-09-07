# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the price table.
Prices data source: https://data.humdata.org/dataset/wfp-food-prices
To check markets: https://dataviz.vam.wfp.org/economic_explorer/prices?adm0=253

Created on Thu Aug 06 17:14:40 2020

@author: ioanna.papachristou@accenture.com
"""

import yaml
import geopandas as gpd
import pandas as pd
from geopy.geocoders import Nominatim
from functools import partial
from geopy.extra.rate_limiter import RateLimiter
import pickle
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
from utils.flat_files import FlatFiles

COUNTRY_LIST = ['Uganda', 'Kenya', 'Somalia', 'Ethiopia', 'Sudan', 'South Sudan']
COUNTRIES_DICT = {'Uganda': 'UGA', 'Kenya': 'KEN', 'Somalia': 'SOM', 'Ethiopia': 'ETH', 'Sudan': 'SDN', 'South Sudan': 'SSD'}
'''
CURRENCIES_DICT = {'Ethiopia': {'ETB': 1, 'USD': 0.03}, 'Kenya': {'KES': 1, 'USD' : 0.0093}, 'Somalia': {'SOS':1, 'USD':0.0017},
                   'Uganda': {'UGX': 1, 'USD': 0.000271}, 'Sudan': {'SDG' : 1, 'USD' : 0.02}, 'South Sudan': {'SSD':1, 'USD':0.00768}}
'''

class PricesTable:
    '''
    This class extracts the markets of the prices and demand dfs. # TODO extract markets from demand df. Currently only working on prices.
    '''
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)
        self.dates = pd.read_csv(self.path_out + 'Date_Dim/Date_Dim.csv', sep=",")
        self.dates['date'] = pd.to_datetime(self.dates['date'])
        self.rates = pd.read_csv(self.path_in + 'currenciesconversion.csv', sep=';')

        # prices
        self.prices = pd.read_csv(self.path_in + 'price/wfpvam_foodprices.csv', sep=',')

    def filter_prices(self):
        #Load prices
        prices = self.prices
        #Filter countries
        prices = prices[prices['adm0_name'].isin(COUNTRY_LIST)]
        #Filter for retail only
        prices = prices[prices.pt_name == 'Retail']
        #Filter columns
        prices = prices.drop(['adm0_id', 'adm1_id', 'mkt_id', 'cm_id', 'cur_id', 'cur_name', 'pt_id', 'um_id',
                      'mp_commoditysource'], axis=1)
        #Filter commodities
        cm_name = ['Maize (white) - Retail', 'Maize - Retail', 'Rice - Retail',
                   'Milk - Retail', 'Milk (cow, fresh) - Retail',
                   'Beans - Retail', 'Beans (dry) - Retail', 'Beans (fava, dry) - Retail', 'Meat (beef) - Retail']
        prices = prices[prices['cm_name'].isin(cm_name)]

        #prices.to_csv('data/input/prices_filtered.csv', sep='|', encoding='utf-8', index=False)

        return prices

    def normalise_units(self):
        prices = self.filter_prices()

        # Replace KG and L to be able to split
        replacements = {'KG': '1 KG', 'L': '1 L'}
        prices['um_name'].replace(replacements, inplace=True)

        # Split to number of units and units
        prices[['number_units', 'units']] = prices.um_name.str.split(expand=True)

        # Transform number of units to numeric
        prices['number_units'] = pd.to_numeric(prices['number_units'])
        #print(prices[prices['number_units'] > 1].head())

        # Calculate the price of 1 unit
        prices['mp_price'] = prices['mp_price'] / prices['number_units']
        #print(prices[prices['number_units'] > 1].head())

        # If eggs, calculate price for 12
        #if prices[prices['cm_name'] == 'eggs']:
         #   prices['mp_price'] = prices['mp_price'] * 12

        return prices

    def normalise_currencies(self):
        prices = self.normalise_units()
        #load csv currency rates
        prices_norm = pd.merge(prices, self.rates, how='left', left_on='adm0_name',
                                          right_on='country')
        prices_norm['mp_price'] = prices_norm['mp_price'] * prices_norm['value USD']

        return prices_norm

    def read_region_shp(self, country_id):
        '''

        :param country: The reference country
        :return: A geodataframe with 2 columns: district id and geometry.
        '''
        if country_id == 'UGA':
            df_region3 = gpd.read_file(self.path_in + "Spatial/gadm36_" + country_id + "_3.shp")[['GID_3', 'NAME_3']]
            df_region3 = df_region3.rename(columns={'GID_3': 'locationID', 'NAME_3': 'adm_name'})

            df_region2 = gpd.read_file(self.path_in + "Spatial/gadm36_" + country_id + "_2.shp")[['GID_2', 'NAME_2']]
            df_region2 = df_region2.rename(columns={'GID_2': 'locationID', 'NAME_2': 'adm_name'})

            df_region1 = gpd.read_file(self.path_in + "Spatial/gadm36_" + country_id + "_1.shp")[['GID_1', 'NAME_1']]
            df_region1 = df_region1.rename(columns={'GID_1': 'locationID', 'NAME_1': 'adm_name'})

            df_region = df_region3.append(df_region2)
            df_region = df_region.append(df_region1)

        else:
            df_region = gpd.read_file(self.path_in + "Spatial/gadm36_" + country_id + "_1.shp")[
                ['GID_1', 'NAME_1']]
            df_region = df_region.rename(columns={'GID_1': 'locationID', 'NAME_1': 'adm_name'})

        return df_region

    def add_adm1_SSudan(self):
        # initialize list of lists
        data = [['Bentiu', 'Central Equatoria'], ['Yida', 'Unity'], ['Rubkona', 'Unity'], ['Aniet', 'Unity'],
                ['Konyokonyo', 'Central Equatoria'], ['Kapoeta South', 'Eastern Equatoria'],
                ['Aweil Town', 'North Bahr-al-Ghazal'], ['Kuajok', 'Warap'], ['Jau', 'West Bahr-al-Ghazal'],
                ['Malakal', 'Upper Nile'], ['Bunj', 'Upper Nile'], ['Suk Shabi', 'Upper Nile'], ['Melut', 'Upper Nile'],
                ['Rumbek', 'Lakes'], ['Minkaman', 'Lakes'], ['Torit', 'Eastern Equatoria'], ['Bor', 'Jungoli'],
                ['Wunrok', 'Warap'], ['Yambio', 'West Equatoria'], ['Makpandu', 'West Equatoria']]

        # Create the pandas DataFrame
        df = pd.DataFrame(data, columns=['mkt_name', 'adm1_name'])
        return df

    def add_location_id(self, country, country_id):
        prices = self.normalise_currencies()
        prices_country = prices[prices['adm0_name'] == country]

        adm1_replacements = {'Addis Ababa': 'Addis Abeba', 'Banadir' :'Banaadir', 'Galgaduud': 'Galguduud',
                             'Hiraan' : 'Hiiraan', 'Juba Hoose' : 'Jubbada Hoose', 'Shabelle Hoose' : 'Shabeellaha Hoose',
                             'Juba Dhexe': 'Jubbada Dhexe', 'Shabeellaha Dhexe': 'Shabeellaha Dhexe',
                             'Beneshangul Gumu': 'Benshangul-Gumaz', 'Gambela' : 'Gambela Peoples', 'Hareri' : 'Harari People',
                             'SNNPR': 'Southern Nations, Nationalities and Peoples', 'Jonglei': 'Jungoli',
                             'Northern Bahr El Ghazal':'North Bahr-al-Ghazal', 'Warrap':'Warap',
                             'Western Bahr El Ghazal':'West Bahr-al-Ghazal', 'Western Equatoria':'West Equatoria'}

        mkt_replacements = {'Hola (Tana River)': 'Tana River', 'Lodwar (Turkana)': 'Turkana', 'Marigat (Baringo)': 'Baringo'}

        prices_country['adm1_name'].replace(adm1_replacements, inplace=True)
        prices_country['mkt_name'].replace(mkt_replacements, inplace=True)

        regions_country = self.read_region_shp(country_id)

        if country_id == 'KEN':
            prices_country_location = pd.merge(prices_country, regions_country, how='left', left_on='mkt_name',
                                               right_on='adm_name')
        elif country_id == 'SSD':
            admin_SSUdan = self.add_adm1_SSudan()
            prices_country = prices_country.drop(['adm1_name'], axis = 1)
            prices_country = pd.merge(prices_country, admin_SSUdan, how = 'inner', on =['mkt_name'])
            prices_country_location = pd.merge(prices_country, regions_country, how='left', left_on='adm1_name',
                                              right_on='adm_name')

        else:
            prices_country_location = pd.merge(prices_country, regions_country, how = 'left', left_on = 'adm1_name', right_on = 'adm_name')
        print(prices_country_location.head())

        return prices_country_location
    '''
    def prices_to_USD(self, df, column, country):
        df[column] = df[column].astype(float)
        df[column] = df[column] * CURRENCIES_DICT[country]['USD']
        return df[column]
    '''
    def location_id_to_markets(self):
        prices_countries = pd.DataFrame()
        for country, country_id in COUNTRIES_DICT.items():
            print(country)
            country_df = self.add_location_id(country, country_id)
            #country_df['mp_price'] = self.prices_to_USD(country_df, 'mp_price', country)

            prices_countries = prices_countries.append(country_df)
            print(prices_countries.shape)
        print(prices_countries.columns)
        print(prices_countries.head())

        #prices_countries.to_csv(self.path_in + 'prices_districts.csv', sep='|', encoding='utf-8', index=False)
        return prices_countries

    def create_measure_df(self):
        '''

        :return: A dataframe with the measure ids of the commodities.
        '''
        # initialize list of lists
        measure_id_data = [[6, 'Maize (white) - Retail'], [6, 'Maize - Retail'], [7, 'Rice - Retail'],
                           [8, 'Meat (beef) - Retail'], [9, 'Milk - Retail'], [9, 'Milk (cow, fresh) - Retail'],
                           [11, 'Beans - Retail'], [11, 'Beans (dry) - Retail'], [11,'Beans (fava, dry) - Retail']]

        # Create the pandas DataFrame
        measure_df = pd.DataFrame(measure_id_data, columns=['measureID', 'item'])
        return measure_df
    '''
    def convert_prices_to_USD(self, price_df):
        price_df['mp_price'] = price_df['mp_price'].astype(float)
        for country in CURRENCIES_DICT:
            price_df[price_df['adm1_name'] == country]['mp_price'] = price_df[price_df['adm1_name'] == country]['mp_price'] * CURRENCIES_DICT[country]['USD']

        return price_df
    '''
    def add_ids_to_table(self):
        '''
        Merges with all other tables and extracts all ids.

        :return: The price dataframe with all columns as defined in the data model.
        '''
        price_table = self.location_id_to_markets()
        # Conert prices to USD
        #price_table = self.convert_prices_to_USD(price_table)

        # Merge production with measure and add measureID
        price_table = price_table.merge(self.create_measure_df(), left_on='cm_name', right_on = 'item', how='left')

        # Create factID
        price_table['factID'] = 'PRICE_'  + price_table.index.astype(str)

        # Create date & dateID column
        price_table['date'] = price_table['mp_year'].astype(str) + '-' + price_table['mp_month'].astype(str) + '-01'
        #price_table['date'] = pd.to_datetime(price_table['date'], format='%Y-%m-%d')
        price_table['date'] = price_table['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        price_table['dateID'] = price_table['date'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
        price_table['dateID'] = price_table['dateID'].astype(int)

        #df[column] = pd.to_datetime([f'{y}-01-01' for y in df[column]])
        #price_table = price_table.merge(self.dates, on='date', how='left')

        # Rename value (price units) & commodities columns
        price_table = price_table.rename(columns={'mp_price': 'value', 'cm_name': 'commodity_name'})

        # Filter only needed columns to export
        price_table_filtered = price_table[['factID', 'measureID', 'dateID', 'locationID', 'value', 'commodity_name']]
        return price_table_filtered

    def Numbeo_to_USD(self):
        numbeo_prices = pd.read_csv(self.path_in + 'Numbeo_pricecom_2020.csv', sep=';')
        numbeo_prices = numbeo_prices[numbeo_prices['value'].notna()]

        numbeo_norm = pd.merge(numbeo_prices, self.rates, how='left', on=['currency'])
        print(numbeo_norm.head())
        # For 'currency' == USD, 'value USD' = 1
        numbeo_norm['value USD'].fillna(1.0, inplace=True)
        numbeo_norm['value'] = numbeo_norm['value'] * numbeo_norm['value USD']

        # Filter only needed columns
        numbeo_norm_filtered = numbeo_norm[['factID', 'measureID', 'dateID', 'locationID', 'value', 'commodity_name']]

        return numbeo_norm_filtered

    def cross_price_w_numbeo(self):
        prices = self.add_ids_to_table()
        numbeo_prices = self.Numbeo_to_USD()

        #Uganda needs eggs (10), beef (8), milk (9) & rice (7) from Numbeo
        Uganda_numbeo = numbeo_prices[numbeo_prices['locationID'] == 'UGA']
        Uganda_numbeo = Uganda_numbeo[Uganda_numbeo['measureID'].isin([10, 8, 9, 7])]
        prices = prices.append(Uganda_numbeo)

        #Sudan needs eggs (10), beef (8), milk (9) & rice (7) from Numbeo
        Sudan_numbeo = numbeo_prices[numbeo_prices['locationID'] == 'SUD']
        Sudan_numbeo = Sudan_numbeo[Sudan_numbeo['measureID'].isin([10, 8, 9, 7])]
        prices = prices.append(Sudan_numbeo)

        #S. Sudan needs eggs (10) from Numbeo
        SSudan_numbeo = numbeo_prices[numbeo_prices['locationID'] == 'SSD']
        SSudan_numbeo = SSudan_numbeo[SSudan_numbeo['measureID'] == 10]
        prices = prices.append(SSudan_numbeo)

        #Somalia needs eggs (10) & beef (8) from Numbeo
        Somalia_numbeo = numbeo_prices[numbeo_prices['locationID'] == 'SOM']
        Somalia_numbeo = Somalia_numbeo[Somalia_numbeo['measureID'].isin([10, 8])]
        prices = prices.append(Somalia_numbeo)

        #Kenya eggs (10), beef (8) & rice (7) from Numbeo
        Kenya_numbeo = numbeo_prices[numbeo_prices['locationID'] == 'KEN']
        Kenya_numbeo = Kenya_numbeo[Kenya_numbeo['measureID'].isin([10, 8, 7])]
        prices = prices.append(Kenya_numbeo)

        #Ethiopia needs eggs (10) & beef (8)from Numbeo
        Ethiopia_numbeo = numbeo_prices[numbeo_prices['locationID'] == 'ETH']
        Ethiopia_numbeo = Ethiopia_numbeo[Ethiopia_numbeo['measureID'].isin([10, 8])]
        prices = prices.append(Ethiopia_numbeo)

        # Homogenise factID
        prices = prices.reset_index(drop=True)
        prices['factID'] = 'PRICE_' + prices.index.astype(str)

        return prices

    def add_missing_locIDs(self):
        prices = self.cross_price_w_numbeo()
        # Load location table
        location_table = pd.read_csv(self.path_out + 'location_dim/location_table.csv', sep="|")[['locationID', 'hierarchy']]
        # Filter for regions
        location_regions = location_table[location_table['hierarchy'] == 1]['locationID']

        # Cross with location table to include all locationIDs even if they do not have any entries.
        price_table = prices.merge(location_regions, on = 'locationID', how = 'outer')

        # Homogenise factID
        price_table = price_table.reset_index(drop=True)
        price_table['factID'] = 'PRICE_' + price_table.index.astype(str)

        # Fill NaNs - IDs cannot be NaNs --> error in Athena when we cross with other tables.
        price_table['measureID'].fillna(0, inplace=True) # random measureID for prices - 0 that corresponds to nothing
        price_table['measureID'] = price_table['measureID'].astype(int)

        todays_date = int(datetime.now().strftime('%Y%m%d'))
        price_table['dateID'].fillna(todays_date, inplace=True) #Fill with today's date
        price_table['dateID'] = price_table['dateID'].astype(int)

        # Filter /reorder only needed columns
        price_table = price_table[['factID', 'measureID', 'dateID', 'locationID', 'value', 'commodity_name']]

        return price_table

    def export_table(self, filename):
        '''

        :return: The price table in a parquet format with the date added in the name.
        '''
        prices_df = self.add_missing_locIDs()
        self.flats.export_parquet_w_date(prices_df, filename)
        #self.flats.export_csv_w_date(prices_df, filename) #only for testing purposes

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print(INPUT_PATH)
    print(OUTPUT_PATH)

    print("------- Extracting prices table ---------")

    #PricesTable().normalise_units()
    #prices = PricesTable().filter_prices()
    #prices = PricesTable().location_id_to_markets()
    #PricesTable().export_table('price_table')
    PricesTable().export_table('price_fact/price_table')
