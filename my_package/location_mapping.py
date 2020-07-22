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
#INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
#OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
INPUT_PATH = r'data/input/'
OUTPUT_PATH = r'data/output/'

geolocator = Nominatim(user_agent="custom-application", timeout=10)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

COUNTRY_LIST = ['Uganda', 'Kenya', 'Somalia', 'Ethiopia']

class ExtractMarkets:
    '''
    This class extracts the markets of the prices and demand dfs. # TODO extract markets from demand df. Currently only working on prices.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

        # prices
        self.prices = pd.read_csv(self.path_in + 'wfpvam_foodprices.csv', sep=',')
        # TODO load demand

    def filter_prices(self):
        #Load prices
        prices = self.prices
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

    # TODO filter_demand function

    def save_to_pickle(self): # TODO transform to generic for both demand and price
        prices = self.filter_prices()
        prices.to_pickle(self.path_in + "prices_filtered.pickle")


class ExtractCoordinates:
    '''
    This class extracts coordinates from locations using geopy.
    '''
    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

    def load_prices_pickle(self):
        prices = pickle.load(open(self.path_in + 'prices.pickle', 'rb'))
        return prices

    def market_country_geolocations(self, country):
        prices = self.load_prices_pickle()
        #Filter for country
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

        #Save df to pickle
        df.to_pickle(self.path_in + "location/" + country + "_geopy.pickle")

    def markets_geolocations(self):

        for country in COUNTRY_LIST:
            self.market_country_geolocations(country)
            print("Pickle exported for " + country)

        return "All pickles exported."


class ExtractLocations:
    '''
    This class extracts locations from shapefiles that include locations of cities, towns and villages per country.
    '''
    def __init__(self, path_in=INPUT_PATH, path_out=OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

        #Settlement locations per country
        self.ethiopia_gdf = gpd.read_file(self.path_in + "location/eth_pplp_multiplesources_20160205.shp")
        self.kenya_gdf = gpd.read_file(self.path_in + "location/KEN_Populated places_2002_DEPHA.shp")
        self.somalia_gdf_1 = gpd.read_file(self.path_in + "location/somalia_settlements_p_coded_v3.shp")
        self.uganda_gdf = gpd.read_file(self.path_in + "location/uga_villages_jan_2009_pcodedb.shp")
        # self.somalia_gdf_2 = gpd.read_file(
        #     self.path_in + "location/hotosm_som_populated_places_points.shp")
        # self.somalia_gdf_3 = gpd.read_file(
        #     self.path_in + "location/hotosm_som_populated_places_polygons.shp")

    def load_prices_pickle(self):
        prices = pickle.load(open(self.path_in + 'prices.pickle', 'rb'))
        return prices

    def is_in_column(self, word, location_gdf, column):
        return word in location_gdf[column].tolist() or word.upper() in location_gdf[column].tolist()

    def is_in_columns(self, word, location, columns):
        return any([self.is_in_column(word, location, column) for column in columns])

    def points(self, loc, location_gdf, column):
        df = location_gdf[location_gdf[column] == loc]
        geometry_list = df['geometry'].tolist()
        return geometry_list

    def add_points(self, element, location_gdf, column):
        element_list = list(element)
        l1 = self.points(element_list[0], location_gdf, column)
        l2 = self.points(element_list[0].upper(), location_gdf, column)
        element_list.append(l1 + l2)
        return tuple(element_list)

    def add_points2(self, element, location_gdf, columns):
        element_list = list(element)
        res = []
        for col in columns:
            res += self.points(element_list[0], location_gdf, col)
        element_list.append(res)
        return tuple(element_list)

    def get_tupples_w_points(self, country):
        # Load data
        data = self.load_prices_pickle()
        data = data[data['adm0_name'] == country]
        # Create list of unique markets
        markets = sorted(set(data['mkt_name']))

        tupples_country = []

        if country == 'Uganda':
            # Load country gdf with locations
            locations = self.uganda_gdf
            locations = locations[locations['ADM0_EN'] == 'Uganda']

            tupples_country = [
                (x, self.is_in_column(x, locations, 'FULL_NAME'), self.is_in_column(x, locations, 'ADM3_EN'),
                 self.is_in_column(x, locations, 'ADM2_EN'), self.is_in_column(x, locations, 'ADM1_EN')) for x in
                markets]

            tupples_country = [self.add_points(x, locations, 'FULL_NAME') for x in tupples_country]

        elif country == 'Kenya':
            locations = self.kenya_gdf
            tupples_country = [
                (x, self.is_in_column(x, locations, 'FULL_NAME_'), self.is_in_column(x, locations, 'SUB_LOCATI'),
                 self.is_in_column(x, locations, 'LOCATION'), self.is_in_column(x, locations, 'DISTRICT')) for x in
                markets]
            tupples_country = [self.add_points(x, locations, 'FULL_NAME_') for x in tupples_country]

        elif country == 'Somalia':
            locations = self.somalia_gdf_1
            locations = locations[locations['admin0Name'] == country]

            tupples_country = [(x, self.is_in_columns(x, locations, ['NAME', 'ALT_NAME1', 'ALT_NAME2']),
                               self.is_in_columns(x, locations, ['admin2Name', 'admin2RefN', 'admin2AltN', 'admin2Al_1']),
                               self.is_in_column(x, locations, 'admin1Name')) for x in markets]
            print(tupples_country)
            tupples_country = [self.add_points2(x, locations, ['NAME', 'ALT_NAME1', 'ALT_NAME2']) for x in
                              tupples_country]
            print(tupples_country)

        elif country == 'Ethiopia':
            locations = self.ethiopia_gdf
            locations = locations[locations['admin0Name'] == country]

            tupples_country = [(x, self.is_in_columns(x, locations, ['featureNam', 'featureRef', 'featureAlt']),
                                self.is_in_column(x, locations, 'admin3Name'),
                                self.is_in_column(x, locations, 'admin2Name'),
                                self.is_in_column(x, locations, 'admin1Name')) for x in markets]

            tupples_country = [self.add_points2(x, locations, ['featureNam', 'featureRef', 'featureAlt']) for x in
                               tupples_country]

        else:
            print(country + " not in scope. Please choose from: Uganda, Ethiopia, Somalia or Kenya.")


        return tupples_country

    def get_country_df_w_points(self, country):
        tupples_country = self.get_tupples_w_points(country)
        df_country = None

        if country == 'Somalia':
            df_country = pd.DataFrame(tupples_country,
                                      columns=['mkt_name', 'is_in_NAMES', 'is_in_ADMIN2', 'is_in_ADMIN1',
                                               'possible_points'])
        else:
            df_country = pd.DataFrame(tupples_country,
                                  columns=['mkt_name', 'is_in_FULL_NAME', 'is_in_ADM3', 'is_in_ADM2', 'is_in_ADM1',
                                           'possible_points'])
        print(country)
        print(df_country)

        with open(self.path_in + 'location/df_' + country + '.pickle', 'wb') as file:
            pickle.dump(df_country, file)

        return df_country

    def markets_locations(self):

        for country in COUNTRY_LIST:
            self.get_country_df_w_points(country)
            print("Pickle exported for " + country)

        return "All pickles exported."



if __name__ == '__main__':

    print("------- Extracting coordinates for locations ---------")
    #PopulationTable(2000).export_population()
    #ExtractCoordinates().filter_prices()

    # prices = ExtractCoordinates().filter_prices()
    # print(prices.shape)
    # print(prices.head())
    # print(prices.isna().sum())

    #Somalia = ExtractCoordinates().market_country_geolocations('Somalia')
    #print("Somalia:")
    #print(Somalia.shape)
    #print(Somalia.columns)
    #print(Somalia.head())

    #ExtractCoordinates().markets_geolocations()
    #Uganda = ExtractLocations().get_country_df_w_points("Uganda")
    #print(Uganda.shape)
    #print(Uganda.columns)
    #print(Uganda.head())

    ExtractLocations().markets_locations()