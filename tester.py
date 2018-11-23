'''
Created on 20.11.2018

@author: test
'''
import numpy as np
import pandas as pd
from gsod import GSOD
import datetime

data_sales='data_sales'
greader = GSOD()

#pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

destations = greader.stationSearch({'ctry': 'UK'})

print(destations.head(300))

dfweather = greader.getData(140150, 2018, 2018)

print(dfweather.shape)



print(dfweather.head(3))

dfsales = pd.read_csv(data_sales+r'\sales.csv')
dfcategories = pd.read_csv(data_sales+r'\categories.csv').drop_duplicates('LowCategory')
dfsales = pd.merge(dfsales,dfcategories,on='LowCategory')
dfsales.columns=dfsales.columns.str.lower()

print(dfsales.head(3))

data = pd.merge(dfweather, dfsales, on='dayofyear')

print(data.head(3))

j = data.plot(kind='scatter', x='prcp', y='countpurchases')
data['prcp'].corr(data['countpurchases'])
j = data.plot(kind='scatter', x='mintemp', y='countpurchases')
j = data.plot(kind='scatter', x='dayofweek', y='countpurchases')
