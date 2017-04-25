import pandas 
from math import radians, cos, sin, asin, sqrt
import time
import datetime
import pyspark
from operator import add

import sys                                                                
sys.stdout = open('../output/reduce.txt', 'w')  

sc = pyspark.SparkContext()

path_noise ='hdfs:///user/xh895/BDM_Project/311_Noise_Complaints.csv'
path_comp_all = 'hdfs:////user/xh895/BDM_Project/311_dl_32817.csv'
path_local = '/Users/macbook/Google Drive/BDM-project/311_dl_32817.csv'

#noise  = sc.textFile(path_noise, 8)
#all_comp = sc.textFile(path_comp_all, 8)
all_comp = sc.textFile(path_comp_all, 8)

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
    time = row[1]
    zipcode = row[9]
    year = time.year
    month = time.month
    day = time.day
    hour = time.hour
    minute = time.minute
    #return ((year, month, day, hour, zipcode), 1)
    return ((year, zipcode), 1)

res = all_comp.filter(ft_header).map(mp_col)
ress = res.filter(ft_noise).filter(ft_havetime).map(mp_groupbykey)

print ress.reduceByKey(add).collect()
