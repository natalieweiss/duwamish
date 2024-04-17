import geopandas as gp
import pandas as pd
import os

""" This script joins the sample points to the nearest river mile along the Duwamish"""

# Initiate lookup folder path 
folder_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Results_NEW"
lookup_folder = os.path.join(folder_path, "Lookup")
river_mile_path = os.path.join(lookup_folder, "duwamish_river_miles.shp")
sample_pts_path = os.path.join(lookup_folder,"Sampling_Sites_Master.csv")

# Read in the shapefiles and create geodataframes
river_mile = gp.read_file(river_mile_path)
sample_pts = pd.read_csv(sample_pts_path)
col = sample_pts.columns.to_list()
sample_pts_gdf = gp.GeoDataFrame(sample_pts, geometry=gp.points_from_xy(sample_pts.Longitude, sample_pts.Latitude), crs="EPSG:4326")

# Spatial join the sample points to river mile
sample_pts_gdf = sample_pts_gdf.sjoin_nearest(river_mile)
col.append('MILE')

# Export processed data
sample_pts_gdf[col].to_csv(os.path.join(lookup_folder,"Sampling_Sites_Master_w_river_mile.csv"))