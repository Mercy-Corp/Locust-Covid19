# -*- coding: utf-8 -*-
"""
The aim of this module is to extract the locust risk and RVF risk tables.
Locust risk data source: https://data.humdata.org/dataset/wfp-food-prices
RVF data source: https://www.numbeo.com/food-prices/

Created on Thu Sep 09 13:10:40 2020
Latest update Thu Sep 09 13:12:40 2020

@author: ioanna.papachristou@accenture.com
"""

import yaml
import os
import pandas as pd
from utils.flat_files import FlatFiles

COUNTRIES_IDS = ["KEN", "SOM", "ETH", "UGA", "SSD", "SDN"]

class RiskTables:
    def __init__(self, path_in, path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.flats = FlatFiles(self.path_in, self.path_out)
        self.locations = pd.read_parquet(self.path_out + '/location_dim/location_table.parquet', engine='pyarrow')[
            ['locationID', 'name', 'hierarchy', 'GID_0']]
        self.locust_risk = pd.read_csv(self.path_in + '/Locustincidencerisk.csv', sep = ';')
        self.RVF_risk = pd.read_csv(self.path_in + '/RVFrisk.csv', sep = ';')

    def location_id_to_risk(self, indicator, country_id):
        # Load files
        if indicator == 'locust':
            risk_country = self.locust_risk[self.locust_risk['CountryID'] == country_id]
        elif indicator == 'RVF':
            risk_country = self.RVF_risk[self.RVF_risk['CountryID'] == country_id]

        else:
            print("indicator should be either 'locust' or 'RVF'.")

        replacements = {'Gambela': 'Gambela Peoples', 'Jonglei': 'Jungoli', 'Western equatoria': 'West Equatoria',
                        'South Kordofan': 'South Kurdufan', 'Al-gadaref': 'Al Gadaref', 'Shaka': 'Sheka',
                        'Kaffa': 'Keffa', 'Welwel and warder': 'Warder', 'Galgaduud': 'Galguduud',
                        'Juba dhexe': 'Jubbada Dhexe', 'Juba hoose': 'Jubbada Hoose', 'Shabelle hoose': 'Shabeellaha Hoose',
                        'SNNPR': 'Southern Nations, Nationalities and Peoples', 'Nakapirpirit': 'Nakapiripirit'}
        risk_country['District'].replace(replacements, inplace=True)

        risk_country['District'] = risk_country['District'].str.lower()

        locations = self.locations[self.locations['GID_0'] == country_id]
        locations = locations[locations['hierarchy'].isin([1, 2, 3])][['locationID', 'name']]
        locations['name'] = locations['name'].str.lower()

        #Merge
        risk_country = risk_country.merge(locations, left_on='District', right_on='name', how='left')

        return risk_country

    def risk_table(self, indicator):
        risk_df = pd.DataFrame()
        for country_id in COUNTRIES_IDS:
            risk_country = self.location_id_to_risk(indicator, country_id)
            risk_df = risk_df.append(risk_country)

        risk_df = risk_df[['factID', 'measureID', 'dateID', 'locationID', 'value']]

        return risk_df

    def export_files(self, indicator):
        '''
        Exports to parquet format.
        '''
        risk_df = self.risk_table(indicator)
        #self.flats.export_csv_w_date(risk_df, indicator + '_risk_table')
        self.flats.export_to_parquet(risk_df, '/'+ indicator + 'fact/' + indicator + '_risk_table')

if __name__ == '__main__':

    filepath = os.path.join(os.path.dirname(__file__), 'config/application.yaml')
    with open(filepath, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    INPUT_PATH = cfg['data']['landing']
    OUTPUT_PATH = cfg['data']['reporting']
    print('INPUT_PATH: ' + INPUT_PATH)
    print('OUTPUT_PATH: ' + OUTPUT_PATH)

    print("------- Extracting risk tables ---------")

    risk = RiskTables(INPUT_PATH, OUTPUT_PATH)
    # Export
    risk.export_files('locust')
    risk.export_files('RVF')