"""
The aim of this module is to extract the measure table.

Created on Thu Jul 06 17:16:40 2020

@author: linnea.evanson@accenture.com
"""
import pandas as pd
from my_package.utils_flat_files import FlatFiles

# #S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/location_dim/'
#local paths
#INPUT_PATH = r'data/input/'
#OUTPUT_PATH = r'data/output/'

class MeasuresTable:
    '''
    This class creates the measures table
    '''

    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.measures_df = pd.read_csv(INPUT_PATH + 'Measures_csv.csv', sep=";")


if __name__ == '__main__':

    print("------- Extracting measures table ---------")

    measures_table = MeasuresTable()

    measures_df = measures_table.measures_df

    # Export
    FlatFiles().export_to_parquet(measures_df,"measures")
    FlatFiles().export_to_csv(measures_df,"measures")

