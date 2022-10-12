#!/usr/bin/env python
# coding: utf-8

# In[ ]:


## First, We extracted data from a flat file and load the data directly to a Cloud SQL database.

## Second, We extracted data from an API and loaded the data to that Cloud SQL database.

# Third, We extracted data from a different flat file, clean and transform the data, then load the cleaned/transformed
# data into that Cloud SQL database.


# In[ ]:


#importing
import pymysql
import pandas as pd
import csv
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String

#Reading CSV file and creating pandas dataframe
data = pd.read_csv("DOB_Job_Application_Filings.csv", nrows=100)
df = pd.DataFrame(data)

#Take 9 Required columns from original
column_list = ["Job #", "Doc #", "Borough", "House #", "Street Name", 
               "Job Type", "Job Status", "Job Status Descrp", "Latest Action Date"]
df = df[column_list]

### Connecting to DB
sql_e = create_engine('mysql+pymysql://root:root@127.0.0.1/')
dconnect = sql_e.connect()
# Write to sql database
sqltypes = {'Job #': Integer(), 'Doc #': Integer(), 'Borough': String(10), 'House #': String(6)}
df.to_sql(name = 'q1', con = sql_e, if_exists = 'replace', dtype = sqltypes)

dconnect.close()


# In[ ]:


import requests
import pymysql
import pandas as pd 
import csv
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String

#sending the API request and getting data from url
url = 'https://data.cityofnewyork.us/resource/ic3t-wcy2.csv'
urlD = requests.get(url).content

#creating dataframe from url data
data = pd.read_csv(StringIO(urlD.decode('utf-8')), nrows = 100)
df = pd.DataFrame(data)

#Taking required columns
column_list = ["job__", "doc__", "borough", "house__", "street_name", 
               "job_type", "job_status", "job_status_descrp", "latest_action_date"]
df = df[column_list]

#connecting to DB
SQLengine = create_engine("mysql+pymysql://root:root@127.0.0.1/hw2")
dbConnection = SQLengine.connect()

#writing to DB
sqltypes = {'job__' :Integer(), 'doc__': Integer(), 'borough': String(10), 'house__': String(6), 'street_name': String(30)}
df.to_sql (name = 'Q2', con = SQLengine, if_exists = 'replace', dtype = sqltypes)
dbConnection.close()


# In[ ]:





# In[ ]:


## CLEANING AND TRANSFORMING THE DATA 
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String
import pandas as pd
import numpy as np
 
 
 
#read csv and create pandas dataframe
data = pd.read_csv('DOB_Job_Application_Filings.csv')
df = pd.DataFrame(data)
 
#take the 9 required columns from original
col_needed = ['Job #','Doc #','Borough','House #','Street Name','Initial Cost', 'Job Type','Job Status','Job Status Descrp','Landmarked', 'Adult Estab', 'Latest Action Date', 'Existing Occupancy', 'Proposed Occupancy', 'Owner Type', 'Job Description']
df = df[col_needed]
 
#transform the specified columns
df[['Landmarked'],['Adult Establishment'], ['Job Description']].replace('', np.nan, inplace=True)
df.dropna(subset=[['Landmarked'],['Adult Establishment'], ['Job Description']], inplace=True)
df['Initial Cost'] = df['Initial Cost'] / 10000
 
#connect to db
SQLengine = create_engine("mysql+pymysql://root:root@127.0.0.1/")
dbConnection = SQLengine.connect()
 
#write to sql database
SQLtypes = {'Job #': Integer(), 'Doc #': Integer(), 'Borough': String(10), 'House #': String(6)}
df.to_sql(name='q1', con=SQLengine, if_exists='replace', dtype=SQLtypes)
 
dbConnection.close()

