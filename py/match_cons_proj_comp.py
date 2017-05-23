sc

import pandas 
from math import radians, cos, sin, asin, sqrt
import time
import datetime
import pandas as pd
from operator import add
from shapely.geometry import Point
from shapely import geometry
import geopandas as gpd
import sys
import os
import pandas


cwd = os.getcwd()

path_cons_local = '/Users/macbook/Desktop/school/spring/bigdata/spark-2.1.0-bin-hadoop2.6/Analyzing_Noise_in_NYC/data/conswithloc.csv'
cons = sc.textFile(path_cons_local)

path_local = '/Users/macbook/Google Drive/BDM-project/311_dl_32817.csv'
path_ct = os.path.join(cwd,'Analyzing_Noise_in_NYC/data/conswithloc.csv')

all_comp = sc.textFile(path_local, 8)

ct_shp = pd.read_csv(path_ct)
ct_shp.geometry = map(lambda x: Point(float(x.split(' ')[1][1:]), float(x.split(' ')[2][:-1])), ct_shp.geometry)
ct_shp = gpd.GeoDataFrame(ct_shp, geometry = 'geometry')
ct_shp.crs = {u'datum': u'NAD83',
 u'lat_0': 40.16666666666666,
 u'lat_1': 40.66666666666666,
 u'lat_2': 41.03333333333333,
 u'lon_0': -74,
 u'no_defs': True,
 u'proj': u'lcc',
 u'units': u'us-ft',
 u'x_0': 300000,
 u'y_0': 0}
ct_shp.geometry =  ct_shp.geometry.buffer(264, resolution = 32)
ct_shp.to_crs({'init': 'epsg:4326'})
ct_shp.project_phase_actual_end_date = map(lambda end: datetime.datetime.strptime(end[:19], '%Y-%m-%d %H:%M:%S'),\
                                          ct_shp.project_phase_actual_end_date)
ct_shp.project_phase_actual_start_date = map(lambda end: datetime.datetime.strptime(end[:19], '%Y-%m-%d %H:%M:%S'),\
                                          ct_shp.project_phase_actual_start_date)

def ft_header(row):
    return row.split(',')[0].isdigit()

def ft_noise(row):
	'''
	this function filters complaint type with Noise
	'''
	return row[4][:5].lower() == 'noise'

def mp_col(row):
    '''
    mapping info that we need
    Unique, Created Date, Closed Date, Agency, Complaint Type, Latitude, Longitude, X Coordinate (State Plane),\
    Y Coordinate (State Plane), Incident Zip
    '''
    import datetime
    row = row.split(',')
    
    try:
    # convert to datetime
        row[1] = datetime.datetime.strptime(row[1], '%m/%d/%Y %I:%M:%S %p')
        row[2] = datetime.datetime.strptime(row[2], '%m/%d/%Y %I:%M:%S %p')
    except ValueError:
        row = row
    return (row[0], row[1], row[2], row[3], row[5], row[49], row[50], row[24], row[25], row[8])

def ft_havetime(row):
    return type(row[1]) == datetime.datetime

def mp_groupbykey(row):
    '''
    set key
    '''
    row, ct = row
    time = row[1]
    zipcode = row[9]
    year = time.year
    month = time.month
    day = time.day
    hour = time.hour
    minute = time.minute
    return ((time, ct), 1)
    #return ((year, ct), 1)

def ft_have_geo(row):
    if '.' in row[8]:
        return (row[8].split('.')[1].isdigit()) and (row[8].split('.')[0].isdigit())
    else:
        return (row[8].isdigit()) and (row[7].isdigit())
    

def mp_sjoin_ct(rows):
    #load in ct file
    for row in rows:
        X = float(row[7])
        Y = float(row[8])
        geo_point = Point((X,Y))
        try:
            ct_number =  ct_shp[ct_shp.geometry.contains(geo_point)].iloc[0,1]
            yield row, ct_number
        except IndexError:
            ct_number = 0
        #return row, ct_number

res = all_comp.filter(ft_header).map(mp_col)
ress = res.filter(ft_noise).filter(ft_havetime).filter(ft_have_geo).mapPartitions(mp_sjoin_ct).map(mp_groupbykey)

def mp_cons(rows):
    for row in rows:
        row = row.split(',')
        idnum = row[1]
        end = row[2][:19]
        start = row[3][:19]
        end = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        start = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        budget = row[4]
        yield idnum, start, end, budget
    
def ft_cons(row):
    row = row.split(',')
    return row[1][0] == 'D'

def ft_cons_time(row):

    idnum = row[1]
    timepoint = row[0]
    cons = ct_shp[ct_shp['dsf_number_s_'] == idnum]
    cons_s = cons.iloc[0,3]
    cons_e = cons.iloc[0,2]
    return cons_s <= timepoint <= cons_e

def mp_cons_key(rows):
    for row in rows:
        yield row[0][0], 1

const_count = ress.filter(ft_cons_time).mapPartitions(mp_cons_key).reduceByKey(add)
const_count.collect()