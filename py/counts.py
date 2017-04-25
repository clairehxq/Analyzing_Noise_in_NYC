from math import radians, cos, sin, asin, sqrt
import time
import datetime
import pyspark
from operator import add
sc = pyspark.SparkContext()
path_noise ='hdfs:///user/xh895/BDM_Project/311_Noise_Complaints.csv'
path_comp_all = 'hdfs:////user/xh895/BDM_Project/311_dl_32817.csv'

noise  = sc.textFile(path_noise, 8)
all_comp = sc.textFile(path_comp_all, 8)

def mapper(row):
    if row['Complaint Type'][:5] == 'Noise':
        yield (row['Unique Key'], row['Created Date'], row['Closed Date'], row['Agency'], row['Complaint Type'],\
    row['Latitude'], row['X Coordinate (State Plane)'], row['Y Coordinate (State Plane)'], row['Incident Zip'])

print all_comp.map(mapper).take(2)  
