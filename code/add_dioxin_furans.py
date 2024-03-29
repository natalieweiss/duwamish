import pandas as pd
import numpy as np
import os

def main(processed_path, sample_outing_name):
    # bring in previous results to append to
    prev_results_path = f"{processed_path}/{sample_outing_name}_results.csv"
    results_df_path = f"{processed_path}/Dioxin Furans/{sample_outing_name}_DF.xlsx"
    output_file_name = f"{processed_path}/{sample_outing_name}_w_df_results.csv"

    # columns
    if os.path.exists(results_df_path):
        print(results_df_path)
        results_df = pd.read_excel(results_df_path, sheet_name = 'Dioxins and Furans TEQ')
        results_df.rename(columns = {'SAMP_ID':'Sample ID', 'DATE': 'Field Collection Start Date', 'SAMPLE TYPE':'Sample Matrix_clean', 'ANALYTE':'Result Parameter Name', 'Result (ug/L)': 'Result Value'}, inplace = True)
        results_df['Result Value Units'] = np.where(results_df['Sample Matrix_clean']=='Water', 'ug/L', 'mg/kg')
        results_df['Result Value'] = np.where(results_df['Result Parameter Name'] == 'CALCULATED TEQ:UB', results_df['TEF: UB'], results_df['Result Value'])
        results_df['Result Value'] = results_df['Result Value'].astype(float)
        results_df['Result Parameter Name_clean'] = np.where(results_df['Result Parameter Name'] == 'CALCULATED TEQ:UB', 'Total dioxin/furan TEQ', results_df['Result Parameter Name'])
        results_df['Result Parameter Name_clean'] = results_df['Result Parameter Name_clean'].str.replace(',','')

        # append to previous results
        prev_results = pd.read_csv(prev_results_path)

        prev_results = pd.concat([prev_results,results_df])

        prev_results.to_csv(output_file_name, index = False)
            
        #results_df.to_csv(output_file_name, index = False)
