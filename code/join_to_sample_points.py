import pandas as pd
import geopandas as gp
import numpy as np
from datetime import date

def main(sample_outing_name, processed_path, sample_pts_path, qaqc_path):

    """ 
    This function takes in the following inputs:
    sample_outing_name, str: month and year of the raw data processed
    qaqc_path, str: the path of the QAQC folder
    sample_pts_path, str: path of the Master Sampling Sites spreadsheet to check for matching sample IDs
    processed_path, str: path for output file 
    
    The output is a single file with processed data join screening levels and sample point geometries
    to be mapped and entered in Tableau
    """

    # Create output file names
    input_results_path = f"{processed_path}/{sample_outing_name}_results_joined_SL.csv"
    output_results_path = f"{processed_path}/{sample_outing_name}_results_join_geometry.csv"

    # Read in sample points
    sample_pts = pd.read_excel(sample_pts_path, sheet_name="Sample_Locations")
    sample_pts_gdf = gp.GeoDataFrame(sample_pts, geometry=gp.points_from_xy(sample_pts.Longitude, sample_pts.Latitude), crs="EPSG:4326")

    # Read in processed results
    screening_results = pd.read_csv(input_results_path)
    cols = screening_results.columns.to_list()
    cols += ['Latitude', 'Longitude', 'Description']

    # Strip all sample ids of leading spaces
    screening_results['Sample ID'] = screening_results['Sample ID'].str.strip()

    try:
        screening_results['DATE'] = screening_results['DATE'].str[:10]
        screening_results["DATE"] = pd.to_datetime(screening_results["DATE"])
    except:
        print("date not included")
    
    # Join sample points geometry to the results based on sample ID
    sample_pts_gdf["Date"] = pd.to_datetime(sample_pts_gdf["Date"], format='mixed').astype("datetime64[ns]")
    sample_pts_join_results = pd.merge(screening_results, sample_pts_gdf, left_on = ['Sample ID', 'DATE'], right_on = ['Sampling ID', 'Date'], how = 'left')
    sample_pts_join_results['RAL Definition_x'] = sample_pts_join_results['RAL Definition_x'].replace(np.nan, 'None')
    sample_pts_join_results['RAL Definition_y'] = sample_pts_join_results['RAL Definition_y'].replace(np.nan, 'None')
    sample_pts_join_results['RAL Flag'] = sample_pts_join_results.apply(lambda x: int(x['RAL Definition_y'] ==x['RAL Definition_x']), axis=1)
    sample_pts_join_results = sample_pts_join_results[sample_pts_join_results['RAL Flag']==1]
    sample_pts_join_results.rename(columns = {'Medium_x':'Medium', 'RAL Definition_y': 'RAL Definition'}, inplace = True)

    sample_pts_join_results_gdf = gp.GeoDataFrame(sample_pts_join_results)
    sample_pts_join_results_gdf['Date'] = sample_pts_join_results_gdf['Date'].astype(str)

    # Export results
    sample_pts_join_results[cols].to_csv(output_results_path, index = False)

    # Export IDs with missing geometries to a table
    missing_ids = sample_pts_join_results[sample_pts_join_results['Latitude'].isna()]
    if len(missing_ids)>0:
        missing_ids[['DATE', 'Sample ID', 'Sampling ID', 'Medium', 'Latitude', 'Longitude','Description']].drop_duplicates().to_csv(f"{qaqc_path}/{sample_outing_name}_missing_pts.csv", index = False)

