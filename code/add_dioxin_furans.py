import pandas as pd
import geopandas as gp
import numpy as np
from datetime import date

def main(sample_outing_name, df_path):
    # bring in previous results to append to
    results_df = f"/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/{sample_outing_name}_results.csv"
    output_file_name =f"/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/{sample_outing_name}_w_df_results.csv"

    # columns
    soil_dxf_teq_cols = ['DATE','SAMP_ID', 'Analyte', 'Result (mg/Kg)', 'UB (mg/kg)']
    water_dxf_teq_cols = ['DATE','SAMP_ID', 'Analyte', 'Result (pg/L)', 'UB (ug/L)']

    # calculate TEQ for dioxins and furans
    soil_dxf_teq_df = pd.read_excel(df_path, sheet_name = 'Dioxins and Furans Soils TEQ')

    soil_dxf_teq_df = soil_dxf_teq_df[soil_dxf_teq_cols]
    #soil_dxf_teq_df['Analyte'] = soil_dxf_teq_df['Analyte'] + ' TEQ'
    soil_dxf_teq_df.rename(columns = {"Analyte": 'Result Parameter Name', "UB (mg/kg)":"Result Value Unit", "SAMP_ID": "Sample ID", "DATE":"Field Collection Start Date"}, inplace = True)

    soil_dxf_teq_df['Result Value Units'] = 'mg/kg'
    soil_dxf_teq_df['Sample Matrix_clean'] = 'Soil'
    soil_dxf_teq_df['Measured'] = np.where(soil_dxf_teq_df['Result (mg/Kg)'].isna(), 'Upper Bound','Measured')
    soil_dxf_teq_df['Measured'] = np.where(soil_dxf_teq_df['Result (mg/Kg)']=='ND', 'Upper Bound',soil_dxf_teq_df['Measured'])

    # water dioxins and furans TEQ results
    # calculate TEQ for dioxins and furans
    water_dxf_teq_df = pd.read_excel(df_path, sheet_name = 'Dioxins and Furans Waters TEQ')

    water_dxf_teq_df = water_dxf_teq_df[water_dxf_teq_cols]
    #water_dxf_teq_df['Analyte'] = water_dxf_teq_df['Analyte'] + ' TEQ'
    water_dxf_teq_df.rename(columns = {"Analyte": 'Result Parameter Name', "UB (ug/L)":"Result Value","SAMP_ID": "Sample ID", "DATE":"Field Collection Start Date"}, inplace = True)
    water_dxf_teq_df['Result Value Units'] = 'ug/L'
    water_dxf_teq_df['Sample Matrix_clean'] = 'Water'
    water_dxf_teq_df['Measured'] = np.where(water_dxf_teq_df['Result (pg/L)'].isna(), 'Upper Bound','Measured')
    water_dxf_teq_df['Measured'] = np.where(water_dxf_teq_df['Result (pg/L)']=='ND', 'Upper Bound',water_dxf_teq_df['Measured'])
    water_dxf_teq_df.drop(columns ='Result (pg/L)', inplace = True)

    # append to previous results
    prev_results = pd.read_csv(results_df)

    prev_results = pd.concat([prev_results,soil_dxf_teq_df,water_dxf_teq_df])

    prev_results.to_csv(output_file_name, index = False)


