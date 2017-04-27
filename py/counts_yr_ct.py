import pandas 
from math import radians, cos, sin, asin, sqrt
import time
import datetime
import pyspark
from operator import add
from shapely.geometry import Point
from shapely import geometry
import geopandas as gpd
import sys
import os
                                                                
sys.stdout = open('../output/reduce_year_ct.txt', 'w')  
cwd = os.getcwd()
sc = pyspark.SparkContext()

path_noise ='hdfs:///user/xh895/BDM_Project/311_Noise_Complaints.csv'
path_comp_all = 'hdfs:///user/xh895/BDM_Project/311_dl_32817.csv'
path_local = '/Users/macbook/Google Drive/BDM-project/311_dl_32817.csv'
path_ct = os.path.join(os.path.dirname(cwd),'data/nyct2010_16d/nyct2010.shp')

path_ct_hdp = 'hdfs:///user/xh895/BDM_Project/nyct2010_16d/nyct2010.shp'
ct_shp = gpd.read_file(path_ct)
crs = {'init': 'epsg:2263'}
ct_shp.to_crs(crs)  

#noise  = sc.textFile(path_noise, 8)
#all_comp = sc.textFile(path_comp_all, 8)
all_comp = sc.textFile(path_comp_all)
# refer column names & loc in /output/complaints_column.csv
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
    #return ((year, month, day, hour, zipcode), 1)
    return ((year, month, day, hour, ct), 1)

def mp_reskey(row):
    row, count  = row
    year = row[0]
    month = row[1]
    day = row[2]
    hour = row[3]
    ct = row[4]
    return ((year, ct), 1)

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
            ct_number =  ct_shp[ct_shp.geometry.contains(geo_point)].iloc[0,0]
        except IndexError:
            ct_number = 0
        yield row, ct_number

res = all_comp.filter(ft_header).map(mp_col)
ress = res.filter(ft_noise).filter(ft_havetime).filter(ft_have_geo).mapPartitions(mp_sjoin_ct).map(mp_groupbykey)
print ress.map(mp_reskey).reduceByKey(add).collect()
