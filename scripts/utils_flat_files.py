# -*- coding: utf-8 -*-
"""
The aim of this module is to to list useful functions related to flat files.

Created on Sat Jun 04 09:36:40 2020

@author: ioanna.papachristou@accenture.com
"""
# Imports
import time
import pandas as pd
import boto3
client = boto3.client('s3')

#S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/Spatial/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

#local paths
#INPUT_PATH = r'data1/input/'
#OUTPUT_PATH = r'data1/output/'

class FlatFiles:
    '''
      Functions to treat flat files.
      '''

    def __init__(self, path_in = INPUT_PATH, path_out = OUTPUT_PATH):
        self.path_in = path_in
        self.path_out = path_out
        self.dates = pd.read_csv(self.path_out + 'Date_Dim/Date_Dim.csv', sep=",")
        self.dates['date'] = pd.to_datetime(self.dates['date'])

    def add_date_id(self, df, column):
        '''

        :param df: The dataframe that includes the column to be transform to datetime
        :param column: The column to be transform to datetime
        :return: The initial dataframe adding the dateID column
        '''
        # Merge with dates and add dateID
        df[column] = pd.to_datetime([f'{y}-01-01' for y in df[column]])
        df = df.merge(self.dates, left_on=column, right_on='date', how='left')
        return df

    def select_columns_fact_table(self, df):
        '''

        :param df: The dataframe to be filtered
        :return: A filtered dataframe including the columns of fact tables
        '''
        df_filtered = df[['factID', 'measureID', 'dateID', 'locationID', 'value']]
        return df_filtered

    def _date_today(self):
        '''

        :return: Today's date
        '''
        # Add today's date
        todays_date = time.strftime("%Y-%m-%d")
        return todays_date

    def export_to_parquet(self, df, file_name):
        '''
        Exports a dataframe to a parquet format.
        :param df: The dataframe to be exported
        :param file_name: the name of the file to be exported
        '''
        df.to_parquet(self.path_out+file_name+".parquet", index=False)
        print("Dataframe exported to parquet format")

    def export_to_csv(self, df, file_name):
        '''
        Exports a dataframe to a parquet format.
        :param df: The dataframe to be exported
        :param file_name: the name of the file to be exported
        '''
        # Export to csv
        df.to_csv(self.path_out + file_name + '.csv', sep='|', encoding='utf-8', index=False)
        print("Dataframe exported to csv format")

    def export_output(self, df, file_name):
        '''

        :param df: The dataframe to be exported.
        :param file_name: The name of the file we want to export to.
        :return: Exports both to parquet and csv formats
        '''
        self.export_to_parquet(df, file_name)
        self.export_to_csv(df, file_name)

    def export_output_w_date(self, df, file_name):
        '''

        :param df: The dataframe to be exported.
        :param file_name: The name of the file we want to export to.
        :return: Exports both to parquet and csv formats including today's date in the name.
        '''
        date = self._date_today()
        file_name = file_name + '_' + date

        self.export_to_parquet(df, file_name)
        self.export_to_csv(df, file_name)

