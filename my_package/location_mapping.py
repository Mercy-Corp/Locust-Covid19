# -*- coding: utf-8 -*-
"""
The aim of this module is to map the markets to the districts.

Created on Thu Jul 21 17:14:40 2020

@author: ioanna.papachristou@accenture.com
"""

#Imports
import geopandas as gpd
import pandas as pd
from geopy.geocoders import Nominatim
from functools import partial
from geopy.extra.rate_limiter import RateLimiter
import pickle

#S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

geolocator = Nominatim(user_agent="custom-application", timeout=10)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

COUNTRY_LIST = ['Uganda', 'Kenya', 'Somalia', 'Ethiopia']

class ExtractCoordinates:
    '''
    This class creates the population table for the expected year.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

        #prices
        self.prices = pd.read_csv(self.path_in + 'wfpvam_foodprices.csv', sep=',')

        # #Settlement locations per country
        # self.ethiopia_gdf = gpd.read_file(self.path_in + "location/eth_pplp_multiplesources_20160205.shp")
        # self.kenya_gdf = gpd.read_file(self.path_in + "location/KEN_Populated places_2002_DEPHA.shp")
        # self.somalia_gdf_1 = gpd.read_file(self.path_in + "location/somalia_settlements_p_coded_v3.shp")
        # self.uganda_gdf = gpd.read_file(self.path_in + "location/uga_villages_jan_2009_pcodedb.shp")
        # self.somalia_gdf_2 = gpd.read_file(
        #     self.path_in + "location/hotosm_som_populated_places_points.shp")
        # self.somalia_gdf_3 = gpd.read_file(
        #     self.path_in + "location/hotosm_som_populated_places_polygons.shp")

    def prices_to_pickle(self):
        prices = self.prices
        prices.to_pickle(self.path_in + "prices.pickle")

    def filter_prices(self):
        prices = pickle.load(open(self.path_in + 'prices.pickle', 'rb'))
        print(prices.columns)
        #Filter countries
        prices = prices[prices['adm0_name'].isin(COUNTRY_LIST)]
        #Filter columns
        prices = prices.drop(['adm0_id', 'adm1_id', 'mkt_id', 'cm_id', 'cur_id', 'cur_name', 'pt_id', 'um_id', 'um_name',
                      'mp_commoditysource'], axis=1)
        #Filter commodities
        cm_name = ['Maize (white) - Retail', 'Maize - Retail', 'Rice - Retail',
                   'Milk - Retail', 'Milk (cow, fresh) - Retail',
                   'Beans - Retail', 'Beans (dry) - Retail', 'Beans (fava, dry) - Retail']
        prices = prices[prices['cm_name'].isin(cm_name)]

        return prices

    def market_country_geolocations(self, country):
        prices = self.filter_prices()

        df = prices[prices['adm0_name'].isin([country])]
        #df_kenya_test = df_kenya[:20]
        #df_kenya_test['geo_location'] = df_kenya_test['mkt_name'].apply(geocode)

        mkt_replacements = {}
        adm1_replacements = {}
        if country == 'Kenya':
            adm1_replacements = {'Coast': 'Coastal Kenya', 'Eastern': 'East Kenya', 'North Eastern': 'Northeast Kenya',
                             'Rift Valley': ' '}
        elif country == 'Uganda':
            mkt_replacements = {'Makaratin': ''}
        elif country == 'Ethiopia':
            mkt_replacements = {'Abaala': 'Abala', 'Robit': 'Shoa Robit', 'Asayta': 'Asaita', 'Wonago': 'Wenago',
                                'Karati': 'Konso',
                                'Haromaya': 'Alemaya', 'Nazareth': 'Adama', 'Beddenno': 'Bedeno', 'Wekro': 'Wukro',
                                'Punido': 'Gog',
                                'Ebinat': 'Ebbenat', 'Wolenchiti': 'Welenchete', 'Ajeber': 'Jedo', 'Bedessa': 'Wachu'}
            adm1_replacements = {'Beneshangul Gumu': 'Benishangul-Gumuz', 'Hareri': 'Harari'}
        elif country == 'Somalia':
            df.loc[df['mkt_name'] == 'Wadajir', 'adm1_name'] = 'Shabelle Hoose'
            mkt_replacements = {'Buloburto': 'Bulo Burto', 'Qorioley': 'Qoryooley'}
            adm1_replacements = {'Hiraan': 'Hiiraan'}

        df['adm1_name'].replace(adm1_replacements, inplace=True)
        df['mkt_name'].replace(mkt_replacements, inplace=True)

        df['location'] = df['mkt_name'] + ', ' + df['adm1_name'] + ', ' + df['adm0_name']

        df['geo_location'] = df['location'].apply(geocode)

        print(df.isna().sum())

        df['point'] = df['geo_location'].apply(lambda loc: tuple(loc.point) if loc else None)

        df.to_pickle(self.path_in + "location/" + country + "_geopy.pickle")

        return df

    def markets_geolocations(self):

        for country in COUNTRY_LIST:
            self.market_country_geolocations(country)
            print("Pickle exported for " + country)

        return "All pickles exported."



if __name__ == '__main__':

    print("------- Extracting coordinates for locations ---------")
    #PopulationTable(2000).export_population()
    ExtractCoordinates().prices_to_pickle()

    prices = ExtractCoordinates().filter_prices()
    print(prices.shape)
    print(prices.head())
    print(prices.isna().sum())

    # Kenya = ExtractCoordinates().market_country_geolocations('Kenya')
    # print("Kenya:")
    # print(Kenya.shape)
    # print(Kenya.columns)
    # print(Kenya.head())

    #ExtractCoordinates().markets_geolocations()
