import pandas as pd
import numpy as np
from datetime import date
import os


def drop_levels(df):
    df.reset_index(inplace = True)
    df.columns = df.columns.droplevel(1)
    return df

## This script joins the combined F&B results to the screening levels
## The only input is the scenario name as an input

# for qaqc:
# a list of all chemicals with screening levels
# a list of all chemicals with tests that have been run

def main(sample_outing_name, processed_path, qaqc_path, sl_path, pcb_arc_lookup_path):
    # create file paths
    if os.path.exists(f"{processed_path}/{sample_outing_name}_w_df_results.csv"):
        input_results_path = f"{processed_path}/{sample_outing_name}_w_df_results.csv"
    elif sample_outing_name == 'prev_results':
        input_results_path = f"{processed_path}/{sample_outing_name}.csv"
    else:
        input_results_path = f"{processed_path}/{sample_outing_name}_results.csv"

    output_results_path = f"{sample_outing_name}_results_joined_SL"

    results_df = pd.read_csv(input_results_path)

    #convert units from pg/L -> ug/L
    results_df['Result Value'] = np.where(results_df['Result Value Units']=='pg/L', results_df['Result Value']/1000000, results_df['Result Value'])
    results_df['Result Value Units'] = np.where(results_df['Result Value Units']=='pg/L', 'ug/L', results_df['Result Value Units'])

    #convert units from mg/L -> ug/L
    results_df['Result Value'] = np.where(results_df['Result Value Units']=='mg/L', results_df['Result Value']/1000, results_df['Result Value'])
    results_df['Result Value Units'] = np.where(results_df['Result Value Units']=='mg/L', 'ug/L', results_df['Result Value Units'])

    #convert units pg/g -> mg/kg
    results_df['Result Value'] = np.where(results_df['Result Value Units']=='pg/g', results_df['Result Value']/1000000, results_df['Result Value'])
    results_df['Result Value Units'] = np.where(results_df['Result Value Units']=='pg/g', 'mg/kg', results_df['Result Value Units'])


    # JOIN TABLES OF RESULTS TO MASTER SCREENING LEVELS FROM F&B

    # create data frame of the screening levels for soils and
    sl_soil_df = pd.read_excel(sl_path, sheet_name='Soil')
    sl_water_df = pd.read_excel(sl_path, sheet_name='Water')

    # concatenate to all screening levels
    sl = pd.concat([sl_soil_df, sl_water_df])

    # remove screening levels that do not have values
    sl = sl[(sl['Screening Level']!='na')]
    sl = sl[(sl['Screening Level']!='TBD')]
    sl = sl[(sl['Screening Level']!='PQL')]
    sl = sl[(sl['Screening Level'].isna()==False)]

    #sl['Screening Level'] = float(sl['Screening Level'])
    sl['Screening Level'] = np.where(sl['SL Unit'] =='mg/L', sl['Screening Level']/1000, sl['Screening Level'])
    sl['SL Unit'] = np.where(sl['SL Unit']=='mg/L', 'ug/L', sl['SL Unit'])

    sl['Screening Level'] = np.where(sl['SL Unit'] =='ppb', sl['Screening Level'], sl['Screening Level'])
    sl['SL Unit'] = np.where(sl['SL Unit']=='ppb', 'ug/L', sl['SL Unit'])

    sl['Screening Level'] = np.where(sl['SL Unit'] =='ppm', sl['Screening Level']/1000, sl['Screening Level'])
    sl['SL Unit'] = np.where(sl['SL Unit']=='ppm', 'ug/L', sl['SL Unit'])

    sl['Screening Level'] = np.where((sl['SL Unit'] =='ppb') & (sl['Medium'] =='Water'), sl['Screening Level']/1000, sl['Screening Level'])
    sl['SL Unit'] = np.where(sl['SL Unit'] =='ppb', 'mg/kg', sl['SL Unit'])


    print(sl['Screening Level'].dtype)
    
    sl_qaqc = sl[['Medium', 'Chemical Group','Chemical', 'Scenario','Scenario Detail','Pathway', 'Screening Level']]
    sl_qaqc.drop_duplicates(inplace = True)
    sl_qaqc.to_csv(f"{qaqc_path}/screening_levels_identified.csv", index = False)

    # strip dioxin furans screening levels of their commas to match the results spreadsheet
    #sl['Chemical'] = np.where(sl['Chemical Group']== 'Dioxin Furans', sl['Chemical'].str.replace(',',''), sl['Chemical'])

    # strip pcbs of their commas to match the results spreadsheet
    sl['Chemical_fmt'] = np.where(sl['Chemical Group']== 'PCB', sl['Chemical'].str.replace(',',''), sl['Chemical'])


    # JOIN SCREENING LEVELS TO RESULTS

    # create data frames of the raw data and the lookup
    pcb_arc_lookup = pd.read_csv(pcb_arc_lookup_path)
    pcb_arc_lookup['PCB Isomer'] = pcb_arc_lookup['PCB Isomer'].str.replace(",","")

    # replace pcb names with the aroclor names to match with F&B results
    sl_arc_join = pd.merge(sl, pcb_arc_lookup, how = 'outer', left_on = 'Chemical_fmt', right_on = 'PCB Isomer')
    sl_arc_join['Chemical_fmt'] = np.where(sl_arc_join['Aroclor Name'].str.contains('aroclor', na=False),sl_arc_join['Aroclor Name'], sl_arc_join['Chemical_fmt'])

    results_df['Result Parameter Name_clean'] = np.where(results_df['Result Parameter Name_clean'].isna(), results_df['Result Parameter Name'], results_df['Result Parameter Name_clean'])

    # join screening levels to the results
    sl_results_join = pd.merge(sl_arc_join, results_df, how = 'outer', left_on = ['Chemical_fmt','Medium'], right_on = ['Result Parameter Name_clean','Sample Matrix_clean'])

    # calculate whether the screening levels have been exceeded
    sl_results_join['SL_exceeded'] = np.where(sl_results_join['Screening Level'] < sl_results_join['Result Value'],'Y','N')

    sl_results_join['SL_diff'] = sl_results_join['Result Value'] - sl_results_join['Screening Level']

    # where the screening level is blank, replace exceedance with "no screening level identified"
    sl_results_join['Screening Level'].fillna('No Screening Level Identified', inplace = True)
    sl_results_join['SL_exceeded'] = np.where(sl_results_join['Screening Level']=='No Screening Level Identified','No Screening Level Identified', sl_results_join['SL_exceeded'])

    sl_results_join.dropna(subset=['Sample ID'], inplace=True)
    sl_results_join.rename(columns = {'Field Collection Start Date': 'DATE'}, inplace = True)


    sl_results_join['Medium'] = np.where(sl_results_join['Medium'].isna() == True, sl_results_join['Sample Matrix_clean'], sl_results_join['Medium'])
    sl_results_join['Chemical_fmt'] = np.where(sl_results_join['Chemical_fmt'].isna() == True, sl_results_join['Result Parameter Name'], sl_results_join['Chemical_fmt'])


    # CALCUALTE MOST STRINGENT AND COUNT OF STRINGENT EXCEEDED
    sl_arc_join = sl_arc_join[sl_arc_join['Screening Level']!='N/A']
    sl_arc_join = sl_arc_join[(sl_arc_join['Screening Level']!='na')]
    sl_arc_join = sl_arc_join[(sl_arc_join['Screening Level']!='TBD')]
    sl_arc_join = sl_arc_join[(sl_arc_join['Screening Level']!='PQL')]
    sl_arc_join = sl_arc_join[(sl_arc_join['Screening Level']!='No Screening Level Identified')]
    sl_arc_join['Screening Level'] = sl_arc_join['Screening Level'].astype(float)

    # find the most stringent screening level for each scenario
    sl_stringent = sl_arc_join.groupby(by =['Medium', 'Chemical Group', 'Chemical_fmt', 'Scenario']).agg({'Screening Level': ['min']}).reset_index()
    sl_stringent = drop_levels(sl_stringent)

    #For the results that signify most stringent, add column indicating stringent value for filtering
    sl_results_join = sl_results_join.merge(sl_stringent, how = 'left', indicator = True, on = ['Medium', 'Chemical Group', 'Chemical_fmt', 'Scenario'])
    sl_results_join['stringent_ind'] = np.where(sl_results_join['_merge']=='both', 'Stringent','')


    sl_results_join.drop(columns = 'Screening Level_y', inplace = True)
    sl_results_join.rename(columns = {'Screening Level_x':'Screening Level'}, inplace = True)

    columns = ['DATE','Sample ID','Medium', 'Chemical Group', 'Chemical', 'Land Use', 'Target Receptor', 'Transport Pathway', 'Exposure Pathway', 'Screening Level', 'SL Unit',
        'Source', 'Result Value','Result Value Units','SL_exceeded', 'SL_diff','stringent_ind']

    sl_results_join[columns].to_csv(f'{processed_path}/{output_results_path}.csv', index = False)

    qaqc_df = sl_results_join[sl_results_join['Screening Level'] != 'No Screening Level Identified']
    qaqc_df= qaqc_df[['DATE','Sample ID', 'Medium', 'Chemical Group','Chemical','Result Value']]
    qaqc_df.drop_duplicates(inplace = True)
    qaqc_df.to_csv(f'{qaqc_path}/{output_results_path}_qaqc.csv', index = False)


    ##TODO: add check to make sure anything with a screening level has been included