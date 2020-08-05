import csv
import os
import pyarrow.parquet as pq
import pyarrow.csv as pv
Source_file='data1/input/wfpvam_foodprices.csv'
output_rows=[]


def price_function():
    with open(Source_file) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for Source_file_row in readCSV:
            output_rows.append(Source_file_row)
    country=['Uganda','Kenya','Somalia','Ethiopia']
    cm_list=['Rice (imported) - Retail','Rice - Retail','Milk - Retail','Milk (cow, fresh) - Retail','Beans - Retail','Beans (fava, dry) - Retail','Beans (dry) - Retail']
    location_filter_filename='data1/output/markets_district_updated.csv'
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
            if (cm_filter_output_row[1]==location_row[0]) or (cm_filter_output_row[2]==location_row[0]):
                 cm_filter_output_row[8]=location_row[1]
            '''else:
                 print(cm_filter_output_row[1])'''
    print_output_rows=[]
    print_output_rows.append(['FACT_ID','MEASURE_ID','DATE','LOCATION_ID','VALUE','COMMODITY_NAME'])
    count=0
    for cm_filter_output_row in cm_filter_output:
        count=count+1
        fact_id="PF_"+str(count+2000)
        date=cm_filter_output_row[5]+"%02d" % int(cm_filter_output_row[4])+'01'
        print_output_rows.append([fact_id,cm_filter_output_row[7],date,cm_filter_output_row[8],cm_filter_output_row[6],cm_filter_output_row[3]])
        
    with open('data1/output/Price_fact.csv', 'w', newline='') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(print_output_rows)
    print("Price table extracted")
    filename='data1/output/Price_fact.csv'
    df=pv.read_csv(filename)
    pq.write_table(df, filename.replace('csv', 'parquet'))
    os.remove('data1/output/Price_fact.csv')
    
price_function()