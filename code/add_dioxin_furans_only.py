import pandas as pd
import numpy as np
import os
import regex as re

def main(processed_path, sample_outing_name, results_df_path):
    # bring in previous results to append to
    output_file_name = f"{processed_path}/{sample_outing_name}_results.csv"

    # columns
    if os.path.exists(results_df_path):
        print(results_df_path)
        results_df = pd.read_excel(results_df_path, sheet_name = 'Dioxins and Furans TEQ')
        for col in results_df.columns:
            if re.match(r"Result .*", col):
                print(col)
                results_df.rename(columns = {col: "Result Value"}, inplace = True)
        results_df.rename(columns = {'SAMP_ID':'Sample ID', 'DATE': 'Field Collection Start Date', 'SAMPLE TYPE':'Sample Matrix_clean', 'ANALYTE':'Result Parameter Name', 'Result (ug/L)': 'Result Value'}, inplace = True)            
        results_df['Result Value Units'] = np.where(results_df['Sample Matrix_clean']=='Water', 'ug/L', 'mg/kg')
        results_df['Result Value'] = np.where(results_df['Result Parameter Name'] == 'CALCULATED TEQ:UB', results_df['TEF: UB'], results_df['Result Value'])
        results_df['Result Value'] = np.where(results_df['Result Value'] == 'ND', np.nan, results_df['Result Value'])
        results_df['Result Value'] = results_df['Result Value'].astype(float)
        results_df['Result Parameter Name_clean'] = np.where(results_df['Result Parameter Name'] == 'CALCULATED TEQ:UB', 'Total dioxin/furan TEQ', results_df['Result Parameter Name'])
        results_df['Result Parameter Name_clean'] = results_df['Result Parameter Name_clean'].str.replace(',','')

        # append to previous results
        results_df.to_csv(output_file_name, index = False)
