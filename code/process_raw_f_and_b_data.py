import pandas as pd
import geopandas as gp
import numpy as np
import os
import glob
from datetime import date


## This script takes a path to a folder containing results from F&B and the name of the sampling outing 
## Note: All results must be in .xlsx format in order to be read by the script
def drop_levels(df):
    df.reset_index(inplace = True)
    df.columns = df.columns.droplevel(1)
    return df

def clean_pcb(val):
    if 'aroclor' in val:
        val = val
    elif 'PCB' in val:
        val = val.replace("/"," ").split(" ")[1]
    return val

def main(sample_outing_name, qaqc_path, sample_pts_path, folder_path):

    # folder containing spreadsheets from F & B
    output_results_path = f"/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/{sample_outing_name}_results.csv"
    qaqc_path = f"/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/qaqc/{sample_outing_name}_qaqc.csv"

    # PROCESS RESULTS SPREADSHEETS
    file_extension = '*.xlsx'

    # List all files with the specified extension in the folder
    files = glob.glob(os.path.join(folder_path, file_extension))

    results_df = []
    file_path = []
    records = []
    sample_ids = []
    methods = []

    # Iterate through each file and read its content
    for file in files:
        df = pd.read_excel(file, sheet_name = 'Sheet1')
        df.columns = df.columns.str.replace("_"," ")
        results_df.append(df)

        try:
            # write out file paths, length of data frame, and sample IDs for qa / qc
            sample_ids.append(list(df['Sample ID'].unique()))
            file_path.append(file)
            records.append(len(df))
            methods.append(list(df['Result Method'].unique()))
        except:
            print(file)

    results_df = pd.concat(results_df)
    results_df['Sample ID'] = results_df['Sample ID'].str.strip()

    #### QAQC check to make sure that the sample ids match what is in our sampling sites spreadsheet
    sample_pts_gdf = gp.read_file(sample_pts_path)
    sample_pt_ids = sample_pts_gdf['Sampling ID'].unique()
    for f_b_ids in sample_ids:
        for f_b_id in f_b_ids:
            if f_b_id in sample_pt_ids: # check to see if f and b id is found in our master sampling sites
                print("Match found:", f_b_id)
            else:
                print("No match found:", f_b_id)
                #user_input = input("Continue: y/n?")
                #if user_input == 'y':
                #    pass
                #else:
                #    raise()

    #### If an ID from F&B does not match the sampling sites id, replace the sample id here using this syntax
    ### TODO: join and replace from fixed ID spreadsheet
    # change sample ID DPS1 -> DPS-1
    results_df['Sample ID'] = np.where(results_df['Sample ID'] == 'DPS1', 'DPS-1', results_df['Sample ID'])

    # change sample ID SPB-0159-S-1 -> SPB-O159-S-1
    results_df['Sample ID'] = np.where(results_df['Sample ID'] == 'SPB-0159-S-1', 'SPB-O159-S-1', results_df['Sample ID'])

    #### QAQC check to make sure the dates line up
    sample_dates = sample_pts_gdf['Date'].unique()
    f_b_dates = results_df['Field Collection Start Date'].unique()
    for date in sample_dates:
        print(date)

    #### QAQC: if any dates are mismatched, replace below using the same syntax
    # replace typo from 11/17 -> 11/07
    results_df['Field Collection Start Date'] = np.where(results_df['Field Collection Start Date']=='2023-11-17 00:00:00', np.datetime64('2023-11-07'), results_df['Field Collection Start Date'])

    # remove any rows that were not field data
    results_df =  results_df[results_df['Field Collection Start Date'].isna() == False]
    qaqc = {'file_path': file_path, 'records': records, 'sample_ids': sample_ids, 'method': methods}
    qaqc_df = pd.DataFrame(data = qaqc)
    qaqc_df.to_csv(qaqc_path, index = False)

    # create new column of sample type based on sample matrix and sample source columns
    results_df['Sample Matrix_clean'] = np.where(results_df['Sample Matrix']=='Aqueous', 'Water', results_df['Sample Matrix'])
    results_df['Sample Matrix_clean'] = np.where(results_df['Sample Matrix'].str.contains('Solid'), 'Soil', results_df['Sample Matrix_clean'])
    results_df['Sample Matrix_clean'] = np.where(results_df['Sample Source']=='Groundwater', 'Water', results_df['Sample Matrix_clean'])

    # clean up pcb values in order to make the join correctly with screening levels
    results_df['Result Parameter Name_clean'] = results_df['Result Parameter Name'].apply(lambda x: clean_pcb(x))

    # clean up d/f results to join correctly with screening levels
    # replace Lube Oil to Diesel Range Organics
    results_df['Result Parameter Name_clean'] = np.where(results_df['Result Parameter Name_clean'] == 'Lube Oil', 'Diesel Range Organics', results_df['Result Parameter Name_clean'])

    # calculate total PCBs for epa1668
    tot_pcbs = results_df[results_df['Result Method'] == 'EPA1668C']
    tot_pcbs = tot_pcbs.groupby(by =['Sample ID', 'Field Collection Start Date', 'Sample Matrix', 'Sample Matrix_clean','Result Value Units']).agg({'Result Value': ['sum']}).reset_index()

    drop_levels(tot_pcbs)
    tot_pcbs['Result Parameter Name_clean'] = 'Total PCBs'

    results_df = pd.concat([results_df, tot_pcbs])

    # remove unnecessary columns from raw data
    results_df = results_df[['Sample ID','Field Collection Start Date','Sample Matrix_clean','Sample Matrix','Sample Source',
                            'Result Parameter Name','Result Parameter Name_clean','Result Value', 'Result Value Units', 'Result Reporting Limit', 
                            'Result Reporting Limit Type', 'Result Detection Limit','Result Detection Limit Type', 'Result Data Qualifier', 'Result Method']]

    results_df.to_csv(output_results_path, index = False)