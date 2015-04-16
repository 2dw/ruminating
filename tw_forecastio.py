# -*- coding: utf-8 -*-
"""
Created on Wed Apr 08 14:18:19 2015

@author: twang
"""

import os, sys, json

#from sql import *
#from config import *


from pandas import date_range, concat, DataFrame, Timestamp, to_datetime, merge,notnull
from datetime import datetime

import requests
from numpy import floor

#import sclib.sclib.v031 as scty # SolarCity specific functionshttp://slc5for00:9999/notebooks/WalMart_Bills.ipynb#; contact jtheurer@solarcity.com for help.

#timezones = os.path.join(os.getcwd(),r"timezones.json")
timezones = os.path.join(r'C:\Users\twang\Dropbox\Optimization\python',r'timezones.json')

def interpolate(series, index):
    series = series.reindex(set(series.index).union(index)).sort_index()

    series = series.interpolate(limit=12).ix[index]
    return DataFrame(series)

def ts_to_dt(ts):
    dt = datetime(ts.year, ts.month,ts.day, ts.hour, ts.minute, ts.second)
    return dt


def convert_hours(td):
    sec = td.total_seconds()
    hours = sec/60.0/60.0
    return round(hours,2)

#def df_loop(df):
#    temp = DataFrame(columns=[],index=[])
#    for row in df.ix: ##iterate through dataframe
#        row = df.ix[row].tolist()
#        lat, lng, state = row

    

def pull_data(lat, lng, state, timestamp, freq, variables, forecastio_key = '80697c3428e4e6523286833268b5530c'):


    url  = "https://api.forecast.io/forecast/%s/%s,%s,%s?%s"%(forecastio_key,lat,lng,timestamp,'solar')
    print url
    r = requests.get(url)

    # set timezone... if error thrown, load default file
    print r.json()
    try:
        tz= r.json()['timezone']
    except KeyError:
        tz=json.loads(open(timezones).read())[state]

    now = Timestamp(datetime.now(),tz='UTC').tz_convert(tz).to_datetime()
    temp = DataFrame(index=[],columns=[])
    # get forecast ever hour
    print r.json()
    for key in r.json()['hourly']['data']:
        # print Timestamp(datetime.fromtimestamp(key['time']),tz='UTC').tz_convert(tz).to_datetime()
        try:
            key = dict(key['solar'].items() + key.items())
            del key['solar']
        except KeyError: pass
        temp= concat([temp,DataFrame(key, index=[0]).head()]) #is head needed?
        
    temp=temp.rename(columns={'time':'Timestamp'})
    temp['Timestamp'] = [Timestamp(datetime.fromtimestamp(i), tz='UTC').tz_convert(tz).to_datetime() for i in temp['Timestamp']]

    temp['State'] = state
    if timestamp =='': #change this to compare now to date
        #hour ahead forecasts
        temp['Offset'] = [floor(convert_hours(i-now))+1 for i in temp['Timestamp']]
    else:
        #actual data
        temp['Offset'] = 0
        
    temp['Downloaded'] = ts_to_dt(now)
    temp['Timestamp'] = [ts_to_dt(i) for i in temp['Timestamp']]
    temp = temp.sort('Timestamp')
    
    if timestamp =='': #change this to compare now to date
        temp = temp[temp['Offset']>0]

    index = date_range(start = min(temp['Timestamp']), end = max(temp['Timestamp']), freq=freq)
    temp = temp.set_index('Timestamp')

    #interpolation?
    keep = DataFrame(index=[],columns=[])
    for col in variables:
        series = interpolate(temp[col], index)
        keep = merge(keep, series, how='outer', left_index=True, right_index=True)
        #keep[col] = [float(i) for i in keep[col]]
        
    # forward fill the offset and non-numerical columns
    for col in ['summary','Offset','State','Downloaded','icon']:
        series = DataFrame(temp[col].reindex(set(temp.index).union(index)).sort_index().ffill().ix[index])
        keep = merge(keep, series, how='outer',left_index=True, right_index=True)
        
    keep = keep.reset_index().rename(columns={'index':'Timestamp'})

    keep = keep.where((notnull(keep)), None)

    return keep




def main(lat=-31.967819, lng=115.87718, state='CA', freq='15Min', timestamp=''):
    
    
    # installationID, latitude, longitude, state, date, zipcode = row[0:6]
    
#    path = scty.bookkeeping.make_folder('http://slc3dsh00:8888/') #creates folder in current directory named after ipython file
    path = os.path.join(r'C:\Users\twang\Dropbox\Optimization\python')
#    path = os.getcwd()
#    floc = join(path,'WEATHER.cache')
    
    start = datetime(2014,1,1,5,0,0)
    timestamp = start.strftime('%Y-%m-%dT%H:%M:%S')    
    
    if timestamp =='valid':
        timestamp = datetime.now()-timedelta(days=1)
        timestamp = timestamp.strftime(',%Y-%m-%dT12:00:00')

    #variables = ['precipIntensity','precipProbability','temperature','apparentTemperature','dewPoint','humidity','windSpeed','windBearing','visibility','cloudCover','pressure','ozone','azimuth','altitude','dni','ghi','dhi','etr','uvIndex']
    variables = ['temperature','apparentTemperature','dewPoint','humidity','windSpeed','windBearing','visibility','cloudCover','pressure','azimuth','altitude','dni','ghi','dhi','etr','uvIndex']
    
    cached = False


    forecast = pull_data(lat, lng, state, timestamp, freq, variables)
    print forecast.head()

#    forecast = forecastio.load_forecast(forecastio_key, lat, lng)

#    print forecast.hourly().data[0].temperature
#    print "===========Hourly Data========="
#    by_hour = forecast.hourly()
#    print "Hourly Summary: %s" % (by_hour.summary)
#
#    for hourly_data_point in by_hour.data:
#        print hourly_data_point
#
#    print "===========Daily Data========="
#    by_day = forecast.daily()
#    print "Daily Summary: %s" % (by_day.summary)
#
#    for daily_data_point in by_day.data:
#        print daily_data_point


if __name__ == "__main__":
    main()
