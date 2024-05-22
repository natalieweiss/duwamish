import pandas as pd
import geopandas as gp
import numpy as np
from datetime import date
import glob
import os

def main(processed_path):
    output_file_name = "agg_results"

    # bring in all results with just screening levels
    file_extension = '*.csv'

    # List all files with the specified extension in the folder
    files = os.listdir(processed_path)

    sl_results = []
    sl_geom_results = []
    sl_stringent_results = []
    # Iterate through each file and read its content
    for file_path in files:
        if "joined_SL" in file_path:
            df = pd.read_csv(processed_path + '/' + file_path)
            sl_results.append(df)
        if "join_geometry" in file_path:
            df = pd.read_csv(processed_path + '/' + file_path)
            sl_geom_results.append(df)

    sl_results = pd.concat(sl_results)
    sl_geom_results = pd.concat(sl_geom_results)

    # Remove all rows where result is blank
    sl_results = sl_results[sl_results['Result Value'].isna()==False]
    sl_geom_results = sl_geom_results[sl_geom_results['Result Value'].isna()==False]

    cols = sl_results.columns
    cols = cols.insert(loc = len(cols), item =['Latitude','Longitude','MILE'])

    sl_results.drop_duplicates().to_csv(processed_path + '/' + output_file_name + '.csv', index = False)
    sl_geom_results.drop_duplicates().to_csv(processed_path + '/' + output_file_name + '_geom.csv', index = False)

    return sl_results

    # keep RCRA8 metals separate
    # sl_geom_results['Chemical Group'] = np.where(sl_geom_results['Chemical Group'] == 'RCRA8', sl_geom_results['Chemical'], sl_geom_results['Chemical Group'])

    sl_geom_results[cols].to_csv(results_folder_path + '/'  + output_file_name + "_geom.csv", index = False)

    sample_pollutant_test = sl_geom_results[['DATE','Sample ID','Medium','Chemical Group','Latitude','Longitude']]
    sample_pollutant_test = sample_pollutant_test[sample_pollutant_test['Medium'].isna()==False]
    sample_pollutant_test = sample_pollutant_test[sample_pollutant_test['Chemical Group'].isna()==False]
    sample_pollutant_test.drop_duplicates(subset=['DATE','Sample ID','Medium','Chemical Group','Latitude','Longitude'], inplace = True)


    #sample_pollutant_test.to_csv(results_folder_path + '/'  + output_file_name + "_geom_pollutant_screens.csv", index = False)

