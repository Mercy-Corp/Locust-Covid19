#!/usr/bin/env python
# coding: utf-8
#author: alicja.grochocka@gmail.com
#Script for creating the calendar (date table)

# In[2]:


import datetime
import pandas as pd
from datetime import timedelta, date


# In[9]:


class DataGenerator:
    
    def __init__ (self, range_start, range_end):
        self.range_start = range_start
        self.range_end = range_end
        self.generate()
              
    def daterange(self):
        for n in range(int ((self.range_end - self.range_start).days)+1):
            yield self.range_start + timedelta(n)
            
    def generate(self):
        self.rows_list = []
        for dt in self.daterange():
            self.rows_list.append([dt.strftime("%Y%m%d"),dt.strftime("%Y-%m-%d")])
        self.df = pd.DataFrame(self.rows_list)
        
start_dt = date(2000, 1, 1) 
end_dt = date(2023,1, 1)
datagenerator = DataGenerator(start_dt, end_dt)

df2=datagenerator.df
df2.rename(columns={0: "dateID", 1: "date"}, inplace=True)
df2.to_parquet('data_parquet',
              index = False)


# In[ ]:




