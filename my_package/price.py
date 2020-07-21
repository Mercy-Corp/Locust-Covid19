# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 10:35:13 2020

@author: rashmi.upreti
"""

import os
import csv
import pickle
import pyarrow.parquet as pq
import pyarrow.csv as pv
#import pyarrow as pa
#import pandas as pd
import boto3
client = boto3.client('s3')

# S3 paths
INPUT_PATH = r's3://mercy-locust-covid19-in-dev/inbound/sourcedata/'
OUTPUT_PATH = r's3://mercy-locust-covid19-out-dev/'

Source_file= INPUT_PATH + 'wfpvam_foodprices.csv'
PICKLE_FILE = INPUT_PATH + 'pickle.pkl'
print(Source_file)
print(PICKLE_FILE)

output_rows=[]
if not os.path.isfile(PICKLE_FILE):
    with open(Source_file) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for Source_file_row in readCSV:
            with open(PICKLE_FILE, 'ab') as file:
                pickle.dump(Source_file_row, file, pickle.HIGHEST_PROTOCOL)
else:
    with open(PICKLE_FILE, 'rb') as file:
        try:
            while True:
                output_rows.append(pickle.load(file))
        except EOFError:
            pass
    country=['Uganda','Kenya','Somalia','Ethiopia']
    cm_list=['Rice (imported) - Retail','Rice - Retail','Milk - Retail','Milk (cow, fresh) - Retail','Beans - Retail','Beans (fava, dry) - Retail','Beans (dry) - Retail']
    location_filter_filename= OUTPUT_PATH + 'location_dim/location_table.csv'
    location_data=[]
    with open(location_filter_filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter='|')
        for condition_row in readCSV:
            location_data.append(condition_row)
    country_filter_output=[]
    for Source_file_row in output_rows[1:]:
        if Source_file_row[1] in country:
            country_filter_output.append(Source_file_row)
    clean_filter_output=[]
    for clean_filter_output_row in country_filter_output:
        clean_filter_output.append([clean_filter_output_row[1],clean_filter_output_row[3],clean_filter_output_row[5],clean_filter_output_row[7],clean_filter_output_row[14],clean_filter_output_row[15],clean_filter_output_row[16]])
    cm_filter_output=[]
    for clean_filter_output_row in clean_filter_output:
        if clean_filter_output_row[3] in cm_list:
             if clean_filter_output_row[3] in cm_list[:1]:
                 clean_filter_output_row.append(7)
             elif clean_filter_output_row[3] in cm_list[2:3]:
                 clean_filter_output_row.append(9)
             else:
                 clean_filter_output_row.append(11)
             cm_filter_output.append(clean_filter_output_row)
    for cm_filter_output_row in cm_filter_output:
        cm_filter_output_row.append('')
        for location_row in location_data[5:]:
            if (cm_filter_output_row[1]==location_row[1]) or (cm_filter_output_row[2]==location_row[1]):
                 cm_filter_output_row[8]=location_row[0]
    print_output_rows=[]
    print_output_rows.append(['FACT_ID','MEASURE_ID','DATE','LOCATION_ID','VALUE','COMMODITY_NAME'])
    count=0
    for cm_filter_output_row in cm_filter_output:
        count=count+1
        fact_id="PF_"+str(count+2000)
        date=cm_filter_output_row[5]+"%02d" % int(cm_filter_output_row[4])+'01'
        print_output_rows.append([fact_id,cm_filter_output_row[7],date,cm_filter_output_row[8],cm_filter_output_row[6],cm_filter_output_row[3]])
        
    with open(OUTPUT_PATH + 'price_fact/Price_fact.csv', 'w', newline='') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(print_output_rows)
    filename= OUTPUT_PATH + 'price_fact/Price_fact.csv'
    df=pv.read_csv(filename)
    pq.write_table(df, filename.replace('csv', 'parquet'))
    print("Price table extracted")
