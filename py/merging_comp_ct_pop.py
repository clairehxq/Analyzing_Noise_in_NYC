import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from shapely.geometry import Polygon

pwd

ct = gpd.read_file('../data/nyct2010_17a/nyct2010.shp')

ct.head(2)

noise = pd.read_csv('/Users/kristikorsberg/Downloads/311_Service_Requests_from_2010_to_Present.csv')

noise.head(2)

noise.columns

noise.head(2)

noise = noise[['Unique Key', 'Created Date', 'Closed Date', 'Agency', 'Complaint Type', 'Descriptor', 'Incident Zip', \
               'X Coordinate (State Plane)', 'Y Coordinate (State Plane)']]

noise = noise.dropna()

geometry = [Point(xy) for xy in zip(noise['X Coordinate (State Plane)'], noise['Y Coordinate (State Plane)'])]
noise = noise.drop(['X Coordinate (State Plane)', 'Y Coordinate (State Plane)'], axis=1)
crs = {'init': 'epsg:2263'}
geo_noise = gpd.GeoDataFrame(noise, crs=crs, geometry=geometry)

print geo_noise.crs
print ct.crs

ct = ct.to_crs({'init':'epsg:2263'})

print geo_noise.crs
print ct.crs

ct = ct[['geometry', 'CT2010', 'Shape_Area', 'Shape_Leng']]

ct.columns

noise_simple = geo_noise[['Unique Key', 'geometry']]

#http://geoffboeing.com/2016/10/r-tree-spatial-index-python/
precise_matches = {}
spatial_index = geo_noise.sindex
for i, CT2010 in enumerate(ct.CT2010):
    possible_matches_index = list(spatial_index.intersection(ct['geometry'][i].bounds))
    possible_matches = geo_noise.iloc[possible_matches_index]
    precise_matches[CT2010] = possible_matches[possible_matches.intersects(ct['geometry'][i])]
    #precise_matches[CT2010]['CT2010'] = CT2010 

noise_ct = pd.concat(precise_matches).reset_index()

noise_ct.head(2)

noise_ct = noise_ct.rename(columns={'level_0':'CT2010'})
noise_ct = noise_ct.drop('level_1',axis=1)

noise_ct.head(2)

noise_ct['CT2010'] = noise_ct['CT2010'].astype(str)

noise_ct['Census Tract'] = noise_ct['CT2010'].apply(lambda s: s.lstrip("0"))

noise_ct.head(2)

pop = pd.read_csv('../data/New_York_City_Population_By_Census_Tracts.csv')

pop.head(2)

pop = pop[pop['Year']==2010]

pop['Census Tract'] = pop['Census Tract'].astype(str)
noise_ct['Census Tract'] = noise_ct['Census Tract'].astype(str)

pop.head(2)

ct_noise_pop = pd.merge(noise_ct, pop, on='Census Tract', how='outer')

ct_noise_pop.head(2)

type(ct_noise_pop)

print len(ct_noise_pop['Unique Key'].unique())
print len(noise['Unique Key'].unique())
print len(noise_ct['Unique Key'].unique())

bbl = pd.read_csv('../data/ct_bbl_avg.csv')
bbl = bbl.drop('Unnamed: 0',axis=1)
bbl.head()

ct_noise_pop.head(2)

ct_noise_pop.CT2010 = ct_noise_pop.CT2010.astype(str)
dropped = []
for elem in ct_noise_pop.CT2010:
    dropped.append(elem.lstrip('00'))

dropped[:10]

ct_noise_pop.CT2010 = dropped

ct_noise_pop.head(2)

bbl.CT2010 = bbl.CT2010.astype(str)
merged = pd.merge(bbl, ct_noise_pop, on='CT2010')

print len(merged)
print len(bbl)
print len(ct_noise_pop)

merged.head()

merged.columns

merged = merged.drop(['Census Tract', 'Borough', 'Year', 'FIPS County Code', 'DCP Borough Code',\
                     'geometry'], axis=1)

merged.to_csv('../data/bbl_ct_noise.csv')

#ct_noise_pop.to_file('../data/ct_noise_pop/ct_noise_pop.shp',driver='ESRI Shapefile')