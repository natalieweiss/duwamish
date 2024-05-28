import pandas as pd
import geopandas as gp
import numpy as np
import os
import glob
from datetime import date


## This script takes a path to a folder containing results from F&B and the name of the sampling outing 

def drop_levels(df):
    """ This function flattens a multiindex dataframe once aggregated"""
    df.reset_index(inplace = True)
    df.columns = df.columns.droplevel(1)
    return df

def clean_pcb(val):
    """ This function removes the / from specific PCB congeners to allow for accurate joins"""
    if 'aroclor' in val:
        val = val
    elif 'PCB' in val:
        val = val.replace("/"," ").split(" ")[1]
    return val

def main(sample_outing_name, qaqc_path, sample_pts_path, fixed_id_path, raw_data_path, processed_path):

    """ 
    This function takes in the following inputs:
    sample_outing_name, str: month and year of the raw data processed
    qaqc_path, str: the path of the QAQC folder
    sample_pts_path, str: path of the Master Sampling Sites spreadsheet to check for matching sample IDs
    raw_data_path, str: path of the raw .xls file
    processed_path, str: path for output file 
    
    The output is a single file with processed raw data with cleaned IDs to match the master sampling sites
    sheet and chemical names to match the screening level spreadsheet
    """

    print(f"Processing:{raw_data_path}")

    # Initiates file paths for output qaqc and processed files
    processed_path = f"{processed_path}/{sample_outing_name}_results.csv"
    qaqc_file = f"{qaqc_path}/{sample_outing_name}_qaqc.csv"

    # Initiates file extension of the raw data files from F&B.
    # Currently provided in .xls file
    file_extension = '*.xls'

    # List all files with the specified extension in the raw data folder
    files = glob.glob(os.path.join(raw_data_path, file_extension))

    results_df = []
    file_path = []
    records = []
    sample_ids = []
    methods = []

    # Iterate through each file and read its content
    for file in files:
        print(f"Processing: {file}")
        df = pd.read_excel(file, sheet_name='Sheet1', engine = 'xlrd') # read first tab of Excel document into a dataframe
        df.columns = df.columns.str.replace("_"," ") # replace underscores in columns to spaces
        df['Sample ID'] = df['Sample ID'].str.strip() # strip the sample IDs of any stray spaces
        results_df.append(df) # append into the results data frame

        # write out metadata for qaqc
        try:
            # write out file paths, length of data frame, and sample IDs for qa / qc
            sample_ids.append(list(df['Sample ID'].unique()))
            file_path.append(file)
            records.append(len(df))
            methods.append(list(df['Result Method'].unique()))
        except:
            continue

    # Concatenate all of the results into one data frame
    results_df = pd.concat(results_df)
    results_df['Sample ID'] = results_df['Sample ID'].str.strip()

    # Export all sample IDs and the methods run for them
    method_by_sample_id = results_df.drop_duplicates(subset = ['Sample ID', 'Result Method'])
    method_by_sample_id.sort_values(by = 'Sample ID', inplace = True)
    method_by_sample_id[['Sample ID', 'Result Method']].to_csv(f"{qaqc_path}/{sample_outing_name}_samples_analysis_methods.csv", index = False)

    # Check against the Master Sampling Sites spreadsheet to see if the IDs match F&B
    # If there is a match that is incorrect, add the ID to the Fixed IDs sheet

    # Replace mismatched IDs from F&B to the correct sampling ID
    fixed_ids = pd.read_excel(fixed_id_path, sheet_name = "IDs")

    sample_pts_gdf = pd.read_excel(sample_pts_path, sheet_name='Sample_Locations')
    sample_pt_ids = sample_pts_gdf['Sampling ID'].unique()
    no_match = []
    for f_b_ids in sample_ids:
        for f_b_id in f_b_ids:
            if f_b_id in sample_pt_ids: # check to see if f and b id is found in our master sampling sites
                continue
            else:
                if ("Method Blank" not in f_b_id):
                    if f_b_id[0:2] != "0_":
                        if f_b_id not in fixed_ids['FB_ID'].unique():
                            no_match.append(f_b_id)
    
    if len(no_match)>0:
        print('No matches found for these IDs:', no_match)
        pd.DataFrame(no_match).drop_duplicates().to_csv(f"{qaqc_path}/{sample_outing_name}_missing_IDs.csv")

    if len(no_match)>0:
        print('Email F&B with typo to get new reports')

    # Join and replace all F_B IDs with Fixed IDs
    results_df = results_df.merge(fixed_ids, how = 'left', left_on = 'Sample ID', right_on = 'FB_ID', indicator = True)
    results_df['Sample ID'] = np.where(results_df['_merge']=='both', results_df['Fixed_ID'], results_df['Sample ID'])
    results_df.drop(columns = ['_merge', 'Fixed_ID'], inplace = True)

    # Join and replace all F_B dates with Fixed dates
    fixed_dates = pd.read_excel(fixed_id_path, sheet_name = "Dates")
    results_df = results_df.merge(fixed_dates, how = 'left', left_on = 'Field Collection Start Date', right_on = 'FB_Date', indicator = True)
    results_df['Field Collection Start Date'] = np.where(results_df['_merge']=='both', results_df['Fixed_Date'], results_df['Field Collection Start Date'])
    results_df.drop(columns = ['_merge', 'Fixed_Date'], inplace = True)

    # Remove any rows that were not field data
    results_df =  results_df[results_df['Field Collection Start Date'].isna() == False]

    # Remove any duplicate rows of chemicals and keep the values with the lowest MRL value
    results_df.sort_values(by = 'Result Detection Limit', ascending = True, inplace = True)
    results_df.drop_duplicates(subset = ['Sample ID', 'Field Collection Start Date', 'Sample Matrix','Result Parameter Name'], inplace = True)

    # Create metadata for QAQC output
    file_path.append(processed_path)
    records.append(len(results_df))
    sample_ids.append(results_df['Sample ID'].unique())
    methods.append(results_df['Result Method'].unique())
    qaqc = {'file_path': file_path, 'records': records, 'sample_ids': sample_ids, 'method': methods}
    qaqc_df = pd.DataFrame(data = qaqc)
    qaqc_df.to_csv(qaqc_file, index = False)

    # create new column of sample type based on sample matrix and sample source columns
    results_df['Sample Matrix_clean'] = np.where(results_df['Sample Matrix']=='Aqueous', 'Water', results_df['Sample Matrix'])
    results_df['Sample Matrix_clean'] = np.where(results_df['Sample Matrix'].str.contains('Solid'), 'Soil', results_df['Sample Matrix_clean'])
    results_df['Sample Matrix_clean'] = np.where(results_df['Sample Source']=='Groundwater', 'Water', results_df['Sample Matrix_clean'])

    # clean up pcb values in order to make the join correctly with screening levels
    results_df['Result Parameter Name_clean'] = results_df['Result Parameter Name'].apply(lambda x: clean_pcb(x))

    # replace Lube Oil to Motor Oil to match the screening level spreadsheet
    results_df['Result Parameter Name_clean'] = np.where(results_df['Result Parameter Name_clean'] == 'Lube Oil', 'Motor Oil', results_df['Result Parameter Name_clean'])

    # calculate total PCBs for method epa1668
    tot_pcbs = results_df[results_df['Result Method'] == 'EPA1668C']
    tot_pcbs = tot_pcbs.groupby(by =['Sample ID', 'Field Collection Start Date', 'Sample Matrix', 'Sample Matrix_clean','Result Value Units']).agg({'Result Value': ['sum']}).reset_index()
    drop_levels(tot_pcbs)
    tot_pcbs['Result Parameter Name_clean'] = 'Total PCBs'

    results_df = pd.concat([results_df, tot_pcbs])

    # remove unnecessary columns from raw data
    results_df = results_df[['Sample ID','Field Collection Start Date', 'Lab Analysis Date', 'Sample Matrix_clean','Sample Matrix','Sample Source',
                            'Result Parameter Name','Result Parameter Name_clean','Result Value', 'Result Value Units', 'Result Reporting Limit', 
                            'Result Reporting Limit Type', 'Result Detection Limit','Result Detection Limit Type', 'Result Data Qualifier', 'Result Method']]

    # Output results processed file
    results_df.to_csv(processed_path, index = False)