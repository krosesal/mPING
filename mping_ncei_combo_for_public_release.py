# -*- coding: utf-8 -*-
"""
GEOG586 Final Project

Author:
Kristina Preucil
krs5664@psu.edu

7/11/2023

This script accesses the mPING database, and returns reports in JSON format.
This script was written based on the examples provided by Oklahoma University
at this website: https://mping.ou.edu/api/api/examples.html.  It also reads in 
.CSV files from the NCEI Storm Events Database and provides methods for filering.

For more filter (data and spatial) requests, see here: https://mping.ou.edu/api/api/reports/filters.html

Eventually, this script will be implemented into an ArcPro Script Tool so that
data requests can be streamlined, and plotted into GIS.

My mPING API token was activated X/XX/XXXX by XXXXXXXX (XXXX.XXXXXX@noaa.gov).
Note, this API is active under a research license allowance, and cannot be used
to garner any type of income.
(FILL IN USING YOUR OWN DETAILS)
User: XXXXXXXX
Token key: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX


JSON variables: id, obtime, category, description, description_id, geom

mPing categories: Test, None, Rain/Snow, Hail, Wind Damage, Tornado, Flood, Mudslide, Reduced Visibility, Winter Weather Impacts

Rain/Snow descriptions:  Rain, Freezing Rain, Drizzle, Ice Pellets/Sleet, Snow and/or Graupel, Mixed Rain and Snow,
   Mixed Ice Pellets and Snow, Mixed Freezing Rain and Ice Pellets, Mixed Rain and Ice Pellets
   
Hail descriptions: Pea (0.25 in.), Half-inch (0.50 in.), Dime (0.75 in.), Quarter (1.00 in.), Half Dollar (1.25 in.),
   Ping Pong Ball (1.50 in.), Golf Ball (1.75 in.), Hen Egg (2.00 in.), Hen Egg+ (2.25 in.), Tennis Ball (2.50 in.),
   Baseball (2.75 in.), Tea Cup (3.00 in.), Baseball+ (3.25 in.), Baseball++ (3.50 in.), Grapefruit- (3.75 in.),
   Grapefruit (4.00 in.), Grapefruit+ (4.25 in.), Softball (4.50 in.), Softball+ (4.75 in.), Softball++ (>=5.00 in.)
   
Wind Damage descriptions: Lawn furniture or trash cans displaced; Small twigs broken, 1-inch tree limbs broken; Shingles blown off,
   3-inch tree limbs broken; Power poles broken, Trees uprooted or snapped; Roof blown off, Homes/Buildings completely destroyed
   
Tornado descriptions: Tornado (on ground), Water Spout

Flood descriptions: River/Creek overflowing; Cropland/Yard/Basement Flooding, Street/road flooding; Street/road closed; Vehicles stranded,
   Homes or buildings filled with water, Homes, buildings or vehicles swept away
   
Reduced Visibility descriptions: Dense Fog, Blowing Dust/Sand, Blowing Snow, Snow Squall, Smoke

Winter Weather Impacts descriptions: Downed tree limbs or power lines from snow or ice, Frozen or burst water pipes,
   Roof or structural collapse from snow or ice, School or business delay or early dismissal, School or business closure,
   Power or internet outage or disruption, Road closure, Icy sidewalks, driveways, and/or parking lots,
   Snow accumulating only on grass, Snow accumulating on roads and sidewalks
"""

# Import needed modules
import requests
import json
import pandas as pd
import arcpy
import re
import geopandas as gpd

# Set up variables
output_folder = r"X:\MGIS\GEOG586\final_project\Data_downloads\shapefiles"
output_file = 'giant_2012.shp'
full_file = output_folder + r'//' + output_file
sr = arcpy.SpatialReference(4326)
#poly = r"POLYGON((-111.0518368 45.0011094,-111.0414271 40.9974986,-109.0471059 41.0008005,-109.0407851 36.9980941,-102.0247635 36.9974697,-102.0405794 40.0025010,-95.3108911 39.9945930,-95.9198053 41.2677773,-96.4180079 42.5172376,-98.5057138 42.9996242,-104.037343 42.9996242,-104.0452516 44.9924344,-111.0518368 45.0011094))"
#poly = {"type":"Polygon", "coordinates":[[[-111.0518368, 45.0011094], [-111.0414271, 40.9974986], [-109.0471059, 41.0008005], [-109.0407851, 36.9980941], [-102.0247635, 36.9974697], [-102.0405794, 40.0025010], [-95.3108911, 39.9945930], [-95.9198053, 41.2677773], [-96.4180079, 42.5172376], [-98.5057138, 42.9996242], [-104.037343, 42.9996242], [-104.0452516, 44.9924344], [-111.0518368, 45.0011094]]]}
#poly = r"-111.0518368,45.0011094,-111.0414271,40.9974986,-109.0471059,41.0008005,-109.0407851,36.9980941,-102.0247635,36.9974697,-102.0405794,40.0025010,-95.3108911,39.9945930,-95.9198053,41.2677773,-96.4180079,42.5172376,-98.5057138,42.9996242,-104.037343,42.9996242,-104.0452516,44.9924344,-111.0518368,45.0011094"

# Set up our request headers indicating we want json returned and include
# our API Key for authorization.
# Make sure to include the word 'Token'. ie 'Token yourreallylongapikeyhere'
reqheaders = {
    'content-type':'application/json',
    'Authorization': 'Token XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' #FILL IN YOUR TOKEN HERE
    }

# Define the parameters here which you want to filter the data by
months = ['03','04','05','06','07','08']
mping_dfs = []
for m in months:
    reqparams = {
        'category':'Hail',
        #'in_bbox':poly,
        #'geom_within':poly,
        'year':'2022',
        'month':m
    }
    
    url = 'http://mping.ou.edu/mping/api/v2/reports'
    response = requests.get(url, params=reqparams, headers=reqheaders)
    
    print(response.url)
    
    # Let the user know if request worked or not, then print the returned JSON dictionaries
    if response.status_code != 200:
        print('Request Failed with status code ' + str(response.status_code))
    else:
        print('Request Successful')
        data = response.json()
        # Pretty print the data
        #print(json.dumps(data,indent=4))
        
    # Access only the point coordinates in returned dictionaries and put them into
    # their own list    
    results_df = pd.DataFrame(data['results'])
    mping_dfs.append(results_df)
    
    
results_df = pd.concat(mping_dfs)

#Read in state shapefiles cuz polygon filtering is BROKEN
co = gpd.read_file(r'X:\MGIS\GEOG586\final_project\Data_downloads\colorado\tl_2016_08_cousub.shp')
ne = gpd.read_file(r'X:\MGIS\GEOG586\final_project\Data_downloads\nebraska\tl_2016_31_cousub.shp')
wy = gpd.read_file(r'X:\MGIS\GEOG586\final_project\Data_downloads\wyoming\tl_2016_56_cousub.shp')
all_states = gpd.GeoDataFrame(pd.concat([co, ne, wy]))
all_states = all_states.to_crs(4326)

results_df = results_df.reset_index(drop=True)
geom_df = pd.DataFrame(results_df['geom'])
coords = []

for index,i in geom_df.iterrows():
    coords.append(i['geom']['coordinates'])
    
# Append coordinates into the results data frame
results_df['Coordinates'] = coords

lons = []
lats = []
# Need specific lat and lon to filter gdf based on shps
for index,i in results_df.iterrows():
    lon,lat = i['Coordinates'][0],i['Coordinates'][1] 
    lons.append(lon)
    lats.append(lat)

results_df['lon'] = lons
results_df['lat'] = lats

# Make hail size into float column
results_df['size'] = results_df['description'].str.extract(r'(\d\.\d\d)')
results_df['size'] = results_df['size'].astype(float)

# Drop unneeded columns from mPING
results_df = results_df.drop(['description_id', 'geom', 'description'], axis=1)

# Clip mping results to study area
results_gdf = gpd.GeoDataFrame(results_df,geometry=gpd.points_from_xy(results_df.lon, results_df.lat, crs="EPSG:4326"))
mping_filtered = gpd.sjoin(results_gdf, all_states)

# Reorder mPING columns
results_df = mping_filtered[['id', 'obtime', 'category', 'size', 'Coordinates']]

# Bring in NCEI shapefiles
df1 = pd.read_csv(r'X:\MGIS\GEOG586\final_project\Data_downloads\NCEI\2012\co_2012.csv')
df2 = pd.read_csv(r'X:\MGIS\GEOG586\final_project\Data_downloads\NCEI\2012\ne_2012_1.csv')
df3 = pd.read_csv(r'X:\MGIS\GEOG586\final_project\Data_downloads\NCEI\2012\ne_2012_2.csv')
df4 = pd.read_csv(r'X:\MGIS\GEOG586\final_project\Data_downloads\NCEI\2012\wy_2012.csv')
#df5 = pd.read_csv(r'X:\MGIS\GEOG586\final_project\Data_downloads\NCEI\2010\wy_2010.csv')

# Concatenate into one dataframe
df = pd.concat([df1, df2, df3, df4])
df = df.reset_index(drop=True)

# Maintain only needed columns from NCEI
df = df[['EVENT_ID', 'BEGIN_DATE', 'EVENT_TYPE', 'MAGNITUDE', 'BEGIN_LAT', 'BEGIN_LON']]

# Get coords for points NCEI
ncei_coords = []
for index,i in df.iterrows():
    ncei_coords.append([i['BEGIN_LON'], i['BEGIN_LAT']])
df['Coordinates'] = ncei_coords
df = df.drop(['BEGIN_LAT', 'BEGIN_LON'], axis=1)

# Rename columns
df.columns = ['id', 'obtime', 'category', 'size', 'Coordinates']

#Merge both mPING and NCEI into one dataframe
df = pd.concat([results_df, df])
df = df.reset_index(drop=True)

# Filter based on size
#df = df[df['size']<=1]
#df = df[(df['size']>1)&(df['size']<=2)]
#df = df[(df['size']>2)&(df['size']<=3)]
df = df[df['size']>3]


# Create shapefile and fields 
arcpy.management.CreateFeatureclass(output_folder, output_file, geometry_type="POINT", spatial_reference=sr)
arcpy.management.AddField(full_file, 'ping_id', "LONG")
arcpy.management.AddField(full_file, 'date_time', "STRING", field_length=30)
arcpy.management.AddField(full_file, 'category', "STRING", field_length=30)
arcpy.management.AddField(full_file, 'size', "FLOAT")
arcpy.management.AddField(full_file, 'lon', "FLOAT")
arcpy.management.AddField(full_file, 'lat', "FLOAT")

fields = ['SHAPE@XY', 'ping_id', 'date_time', 'category', 'size', 'lon', 'lat']

with arcpy.da.InsertCursor(full_file, fields) as cursor:
    for index,x in df.iterrows():
        cursor.insertRow((x['Coordinates'], x['id'], x['obtime'], x['category'], x['size'], x['Coordinates'][0], x['Coordinates'][1]))
        
del cursor


# End program
                          
    
