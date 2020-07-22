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
#from functools import partial
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
            markets = [x.split()[0] for x in markets]
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

class CrossResults:
    '''
    This class crosses the results between the 2 methods, based on geolocations and location shapefiles.
    '''
    def __init__(self, path_in=INPUT_PATH, path_out=OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out

        # load districts
        self.shp2_Kenya = gpd.read_file(self.path_in + "Spatial/gadm36_KEN_2.shp")[['GID_2', 'geometry']]
        self.shp2_Somalia = gpd.read_file(self.path_in + "Spatial/gadm36_SOM_2.shp")[['GID_2', 'geometry']]
        self.shp2_Ethiopia = gpd.read_file(self.path_in + "Spatial/gadm36_ETH_2.shp")[['GID_2', 'geometry']]
        self.shp2_Uganda = gpd.read_file(self.path_in + "Spatial/gadm36_UGA_2.shp")[['GID_2', 'geometry']]

    def load_df_country_pickle(self, country):
        df = pickle.load(open(self.path_in + 'location/df_' + country + '.pickle', 'rb'))
        return df

    def coor_to_points(self, coordinates):
        l = list(zip(*coordinates))
        if len(l) == 0:
            return []
        if len(l) == 3:
            l = l[:2]
        lats = list(l[0])
        lons = list(l[1])
        points = gpd.points_from_xy(lats, lons)
        return points

    def revert_points(self, point_list):
        lons = list(map(lambda point: point.x, point_list))
        lats = list(map(lambda point: point.y, point_list))
        points = gpd.points_from_xy(lats, lons)
        return points

    def cross_results(self, df_actual, gdf_referencia, nuevos, cambios, manual):
        n = df_actual.shape[0]
        points = []
        for i in range(n):
            mercado = df_actual.loc[i, 'mkt_name']
            if mercado in nuevos:
                punto = self.revert_points(gdf_referencia[gdf_referencia['mkt_name'] == mercado]['geometry'].tolist())[0]
            elif mercado in cambios.keys():
                punto = \
                self.revert_points(gdf_referencia[gdf_referencia['mkt_name'] == cambios[mercado]]['geometry'].tolist())[0]
            elif mercado in manual.keys():
                punto = self.revert_points(self.coor_to_points([manual[mercado]]))[0]
            else:
                puntos = df_actual.loc[i, 'possible_points']
                if len(puntos) == 1:
                    punto = puntos[0]
                else:
                    lista = gdf_referencia[gdf_referencia['mkt_name'] == mercado]
                    if len(lista) > 0:
                        punto = self.revert_points(lista['geometry'].tolist())[0]
                    else:
                        punto = None
            points.append(punto)
        gdf = gpd.GeoDataFrame(df_actual, geometry=points)
        return gdf

    def get_Kenya_final_points(self):

        df_kenya = pickle.load(open(self.path_in + 'location/df_kenya.pickle', 'rb'))
        points = list(df_kenya['possible_points'])
        points = [x[0] for x in points]
        gdf_kenya = gpd.GeoDataFrame(df_kenya, geometry=points)
        del gdf_kenya['possible_points']

        #save to pickle
        with open(self.path_in + 'location/gdf_kenya.pickle', 'wb') as file:
            pickle.dump(gdf_kenya, file)
            print("Kenya gdf saved to pickle")

        return gdf_kenya

    def get_Somalia_final_points(self):

        df_somalia = pickle.load(open(self.path_in + 'location/df_somalia.pickle', 'rb'))
        df_somalia2 = pickle.load(open(self.path_in + 'location/somalia_geopy.pickle', 'rb'))
        df_somalia2 = df_somalia2.loc[:, ['adm1_name', 'mkt_name', 'point']]
        df_somalia2 = df_somalia2.drop_duplicates()
        df_somalia2 = df_somalia2.sort_values('mkt_name')
        gdf_somalia2 = gpd.GeoDataFrame(df_somalia2, geometry=self.coor_to_points(df_somalia2['point']))

        new_somalia = []
        changes_somalia = {'Qorioley': 'Qoryooley', 'Buloburto': 'Bulo Burto'}
        manual_somalia = {'Qardho': (9.50694, 49.08608), 'Dinsoor': (2.24144, 42.58688)}

        gdf_somalia = self.cross_results(df_somalia, gdf_somalia2, new_somalia, changes_somalia, manual_somalia)
        del gdf_somalia['possible_points']

        with open(self.path_in + 'location/gdf_somalia.pickle', 'wb') as file:
            pickle.dump(gdf_somalia, file)

        return gdf_somalia

    def get_Uganda_final_points(self):
        df_uganda = pickle.load(open(self.path_in + 'location/df_uganda.pickle', 'rb'))
        df_uganda2 = pickle.load(open(self.path_in + 'location/uganda_geopy.pickle', 'rb'))
        df_uganda2 = df_uganda2.loc[:, ['adm1_name', 'mkt_name', 'point']]
        df_uganda2 = df_uganda2.drop_duplicates()
        df_uganda2 = df_uganda2.sort_values('mkt_name')

        gdf_uganda2 = gpd.GeoDataFrame(df_uganda2, geometry=self.coor_to_points(df_uganda2['point']))

        new_uganda = []
        changes_uganda = dict()
        # failing_uganda = ['Busia', 'Kabale', 'Kapchorwa', 'Makaratin', 'Mbale']
        manual_uganda = {'Busia': (0.464315, 34.100959), 'Kabale': (-1.259330, 29.990903),
                         'Kapchorwa': (1.398665, 34.445282), 'Mbale': (1.070686, 34.178467)}

        gdf_uganda = self.cross_results(df_uganda, gdf_uganda2, new_uganda, changes_uganda, manual_uganda)
        del gdf_uganda['possible_points']

        with open(self.path_in + 'location/gdf_uganda.pickle', 'wb') as file:
            pickle.dump(gdf_uganda, file)

        return gdf_uganda

    def get_Ethiopia_final_points(self):
        df_ethiopia = pickle.load(open(self.path_in + 'location/df_ethiopia.pickle', 'rb'))
        df_ethiopia2 = pickle.load(open(self.path_in + 'location/ethiopia_geopy.pickle', 'rb'))

        df_ethiopia2 = df_ethiopia2.loc[:, ['adm1_name', 'mkt_name', 'point']]
        df_ethiopia2 = df_ethiopia2.drop_duplicates()

        df_ethiopia2 = df_ethiopia2.sort_values('mkt_name')

        gdf_ethiopia2 = gpd.GeoDataFrame(df_ethiopia2, geometry=self.coor_to_points(df_ethiopia2['point']))

        new_ethiopia = ['Kobo', 'Meti']
        changes_ethiopia = {'Abaala': 'Abala', 'Ajeber': 'Jedo', 'Asayta': 'Asaita', 'Beddenno': 'Bedeno',
                            'Bedessa': 'Wachu', 'Ebinat': 'Ebbenat', 'Wekro': 'Wukro', 'Wolenchiti': 'Welenchete',
                            'Wonago': 'Wenago'}
        # failing_ethiopia = ['Abi Adi', 'Abomsa', 'Addis Ababa', 'Alaba', 'Ambo', 'Assela', 'Assosa', 'Baher Dar', 'Bitchena', 'Bure', 'Debre Birhan', 'Debre Markos', 'Delo', 'Derashe', 'Eteya', 'Gonder', 'Gordamole', 'Haromaya', 'Humera', 'Jijiga', 'Karati', 'Korem', 'Mekele', 'Merti', 'Woliso', 'Yabelo', 'Ziway']
        manual_ethiopia = {'Abomsa': (8.489248, 39.827752), 'Addis Ababa': (9.030492, 38.738864),
                           'Assosa': (10.057507, 34.542267), 'Baher Dar': (11.588808, 37.386951),
                           'Debre Birhan': (9.664792, 39.516591), 'Debre Markos': (10.336315, 37.742438),
                           'Gonder': (12.587218, 37.437590), 'Humera': (14.290656, 36.606580),
                           'Jijiga': (9.350723, 42.820830), 'Karati': (5.25, 37.4833333), 'Korem': (12.5, 39.5166667),
                           'Mekele': (13.497682, 39.463978), 'Woliso': (8.531432, 37.971069),
                           'Yabelo': (4.894945, 38.100220), 'Ziway': (7.935381, 38.714283)}

        gdf_ethiopia = self.cross_results(df_ethiopia, gdf_ethiopia2, new_ethiopia, changes_ethiopia, manual_ethiopia)
        del gdf_ethiopia['possible_points']

        with open(self.path_in + 'location/gdf_ethiopia.pickle', 'wb') as file:
            pickle.dump(gdf_ethiopia, file)

        return gdf_ethiopia

    def get_districts(self):
        '''

        :return: A geodataframe with all districts of the 4 countries concatenated.
        '''
        district_level = [self.shp2_Kenya, self.shp2_Ethiopia, self.shp2_Somalia, self.shp2_Uganda]
        gdf_districts = gpd.GeoDataFrame(pd.concat(district_level, ignore_index=True))
        gdf_districts.crs = {"init": "epsg:4326"}
        return gdf_districts


    def spatial_join_districts(self):
        Kenya = gpd.GeoDataFrame(self.get_Kenya_final_points()[['mkt_name', 'geometry']])
        print('Kenya:')
        print(Kenya)
        #Kenya['country'] = 'Kenya'
        Uganda = gpd.GeoDataFrame(self.get_Uganda_final_points()[['mkt_name', 'geometry']])
        #Uganda['country'] = 'Uganda'
        Somalia = gpd.GeoDataFrame(self.get_Somalia_final_points()[['mkt_name', 'geometry']])
        #Somalia['country'] = 'Somalia'
        Ethiopia = gpd.GeoDataFrame(self.get_Ethiopia_final_points()[['mkt_name', 'geometry']])
        #Ethiopia['country'] = 'Ethiopia'

        countries = [Kenya, Uganda, Somalia, Ethiopia]
        markets_gdf = gpd.GeoDataFrame(pd.concat(countries, ignore_index=True, sort=False))
        markets_gdf = markets_gdf.dropna()
        markets_gdf.crs = {"init": "epsg:4326"}

        districts = self.get_districts()

        sjoined_markets = gpd.sjoin(markets_gdf, districts, op="within")
        sjoined_markets['locationID'] = sjoined_markets['GID_2']
        sjoined_markets = sjoined_markets.drop(['geometry', 'index_right', 'GID_2'], axis=1)
        sjoined_markets.head()
        return sjoined_markets

    def export_markets_district_csv(self, file_name):
        '''
        Exports a dataframe to a parquet format.
        :param df: The dataframe to be exported
        :param file_name: the name of the file to be exported
        '''
        df = self.spatial_join_districts()
        # Export to csv
        df.to_csv(self.path_in + file_name + '.csv', sep='|', encoding='utf-8', index=False)
        print("Dataframe exported to csv format")


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

    #ExtractLocations().markets_locations()

    #Kenya = CrossResults().get_Kenya_final_points()
    #Uganda = CrossResults().get_Uganda_final_points()
    #print("Uganda:")
    #print(Uganda)
    #Somalia = CrossResults().get_Somalia_final_points()
    #Ethiopia = CrossResults().get_Ethiopia_final_points()

    #markets_district = CrossResults().spatial_join_districts()
    #print(markets_district.columns)
    #print(markets_district.head())

    CrossResults().export_markets_district_csv('markets_district')