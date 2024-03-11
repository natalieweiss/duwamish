import pandas as pd
import geopandas as gp
import numpy as np
from datetime import date

def main(sample_outing_name, input_results_path, sample_pts_path):
    # create output file names
    input_results_path = f"/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/{sample_outing_name}_results_joined_SL.csv"
    output_results_path = f"{sample_outing_name}_results_join_geometry"

    sample_pts = pd.read_csv(sample_pts_path)
    sample_pts_gdf = gp.GeoDataFrame(sample_pts, geometry=gp.points_from_xy(sample_pts.Longitude, sample_pts.Latitude), crs="EPSG:4326")

    # replace SI-W-2 to S1-W-2 to match results spreadsheet
    sample_pts_gdf['Sampling ID_match_fb'] = np.where(sample_pts_gdf['Sampling ID'] == 'SI-W-2', 'S1-W-2', sample_pts_gdf['Sampling ID'])

    # replace BI-W-1 to B1-W-1 to match results spreadsheet
    sample_pts_gdf['Sampling ID_match_fb'] = np.where(sample_pts_gdf['Sampling ID_match_fb'] == 'BI-W-1', 'B1-W-1', sample_pts_gdf['Sampling ID_match_fb'])

    # replace DP-S-1 to DPS-1 to match results spreadsheet
    sample_pts_gdf['Sampling ID_match_fb'] = np.where(sample_pts_gdf['Sampling ID_match_fb'] == 'DP-S-1', 'DPS-1', sample_pts_gdf['Sampling ID_match_fb'])

    # replace BI-55-W to BI-55-1-W to match results spreadsheet
    sample_pts_gdf['Sampling ID_match_fb'] = np.where(sample_pts_gdf['Sampling ID_match_fb'] == 'BI-55-W', 'BI-55-1-W', sample_pts_gdf['Sampling ID_match_fb'])

    # replace date for BIS-1-2 from 11/9/2023 to 11/8/2023
    sample_pts_gdf['Date_match_fb'] = np.where(sample_pts_gdf['Sampling ID_match_fb'] == 'BIS-1-2', date(2023, 11, 8).strftime('%m/%d/%Y'), sample_pts_gdf['Date'])
    sample_pts_gdf['Date_match_fb'] = pd.to_datetime(sample_pts_gdf['Date_match_fb'])


    screening_results = pd.read_csv(input_results_path)

    # strip all sample ids of leading spaces
    screening_results['Sample ID'] = screening_results['Sample ID'].str.strip()

    # replace sampling ID for SPB-0159-S-1 to SPB-O159-S-1
    screening_results['Sample ID'] = np.where(screening_results['Sample ID'] == 'SPB-0159-S-1', 'SPB-O159-S-1', screening_results['Sample ID'])

    # TODO: remove any screening results with U and C qualifiers. do not want to show these in the map

    try:
        screening_results['DATE'] = screening_results['DATE'].str[:10]
        screening_results["DATE"] = pd.to_datetime(screening_results["DATE"])
    except:
        print("date not included")


    # replace date for BIS-1-2 from 11/9/2023 to 11/8/2023
    screening_results['DATE'] = np.where(screening_results['Sample ID'] == 'BIS-1-2', np.datetime64(date(2023, 11, 8)), screening_results['DATE'])

    sample_pts_gdf["Date"] = pd.to_datetime(sample_pts_gdf["Date"], format='mixed')


    sample_pts_join_results = pd.merge(screening_results, sample_pts_gdf, left_on = ['Sample ID', 'DATE'], right_on = ['Sampling ID_match_fb', 'Date'], how = 'left')
    sample_pts_join_results.rename(columns = {'Medium_x':'Medium'}, inplace = True)


    sample_pts_join_results_gdf = gp.GeoDataFrame(sample_pts_join_results)
    sample_pts_join_results_gdf['Date'] = sample_pts_join_results_gdf['Date'].astype(str)

    sample_pts_join_results[['DATE', 'Sample ID', 'Sampling ID', 'Medium', 'Chemical Group', 'Chemical', 'Scenario',
        'Pathway', 'Screening Level Measurement', 'SL Unit',
        'Source', 'Parameter in Spreadsheet', 'Result Value',
        'Result Value Units', 'SL_exceeded', 'SL_diff','stringent_ind','Latitude', 'Longitude','Description']].to_csv(f"/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/{output_results_path}.csv", index = False)


    missing_ids = sample_pts_join_results[sample_pts_join_results['Latitude'].isna()]
    if len(missing_ids)>0:
        missing_ids[['DATE', 'Sample ID', 'Sampling ID', 'Medium', 'Latitude', 'Longitude','Description']].to_csv(f"/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/qaqc/{sample_outing_name}_missing_pts.csv", index = False)

