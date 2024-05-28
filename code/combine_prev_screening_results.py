import pandas as pd
import geopandas as gp
import numpy as np
import os
import regex as re

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

def main(processed_path, prev_wb_path):
    output_results_path = f"{processed_path}/prev_results.csv"

    # initiate columns to keep in final dataframe
    soil_RCRA_cols = ['DATE', 'SAMP_ID', 'Arsenic', 'Barium', 'Cadmium', 'Chromium', 'Lead','Selenium', 'Silver']
    water_RCRA_cols = ['DATE', 'SAMP_ID', 'Arsenic', 'Barium', 'Cadmium', 'Lead', 'Selenium', 'Silver']
    pah_cols = ['DATE','SAMP_ID','1-Methylnaphthalene', '2-Methylnaphthalene', 'Acenaphthene', 'Acenaphthylene', 'Anthracene', 'Benzo(a)anthracene', 'Benzo(a)pyrene', 'Benzo(b)fluoranthene', 'Benzo(g,h,i)perylene', 'Benzo(k)fluoranthene', 'Chrysene', 'Dibenz(a,h)anthracene', 'Fluoranthene', 'Fluorene', 'Indeno(1,2,3-cd)pyrene', 'Naphthalene', 'Phenanthrene', 'Pyrene', '2-Fluorobiphenyl (S) %', 'Terphenyl-d14 (S) %']
    soil_pcb_cols = ['DATE', 'SAMP_ID', 'PCB Isomer','Concentrations Detected (mg/Kg)']
    water_pcb_cols = ['DATE', 'SAMP_ID', 'PCB Isomer','Concentrations Detected (ug/L)']

    # create empty data frames to store collated results across all sampling dates
    # by medium and pollutant type
    soil_rcra = pd.DataFrame()
    water_rcra = pd.DataFrame()
    soil_pah = pd.DataFrame()
    water_pah = pd.DataFrame()
    soil_pcb = pd.DataFrame()
    water_pcb = pd.DataFrame()

    # Use a list comprehension to get all .xlsx files in the subfolder
    results_files = [f for f in os.listdir(prev_wb_path) if f.endswith(".xlsx")]

    # soil rcra processing

    for i in results_files:
        try:
            soil_rcra8_df = pd.read_excel(prev_wb_path+'/'+i, sheet_name = 'Soil RCRA8')
            print(i , "Soil RCRA sheet found")

            soil_rcra8_df = soil_rcra8_df[1:]
            for col in soil_rcra8_df.columns:
                if re.match(r"Mercury.*", col):
                    mercury_col_name = col
                    soil_RCRA_cols.append(mercury_col_name)
            soil_rcra8_df = soil_rcra8_df[soil_RCRA_cols]
            soil_rcra8_df = soil_rcra8_df.melt(id_vars=['DATE','SAMP_ID'])
            soil_rcra8_df['Result Value Units'] = 'mg/kg'
            soil_rcra8_df['Sample Matrix'] = 'Soil'
            soil_rcra= pd.concat([soil_rcra, soil_rcra8_df], ignore_index= True)
            soil_RCRA_cols.remove(mercury_col_name)
        except Exception as e:
            print(i, "no Soil RCRA sheet")
            print(e)

    # water rcra results
    for i in results_files:
        try:
            water_rcra8_df = pd.read_excel(prev_wb_path+'/'+i, sheet_name = 'Water RCRA8')
            print(i , "Water RCRA sheet found")

            water_rcra8_df = water_rcra8_df[1:]
            for col in water_rcra8_df.columns:
                if re.match(r"Mercury.*", col):
                    mercury_col_name = col
                    water_RCRA_cols.append(mercury_col_name)
            water_rcra8_df = water_rcra8_df[1:]
            water_rcra8_df = water_rcra8_df[water_RCRA_cols]
            water_rcra8_df = water_rcra8_df.melt(id_vars=['DATE','SAMP_ID'])
            water_rcra8_df['Result Value Units'] = 'ug/L'
            water_rcra8_df['Sample Matrix'] = 'Water'
            water_rcra= pd.concat([water_rcra, water_rcra8_df], ignore_index= True)
            water_RCRA_cols.remove(mercury_col_name)
        except Exception as e:
            print(i, "no Water RCRA sheet")
            print(e)

    # soil pah results
    for i in results_files:
        try:
            soil_pah_df = pd.read_excel(prev_wb_path+'/'+i, sheet_name = 'PAH Soils')
            print(i , "Soil PAH sheet found")
            soil_pah_df = soil_pah_df[1:]
            #soil_pah_df= soil_pah_df[pah_cols]
            soil_pah_df = soil_pah_df.melt(id_vars=['DATE','SAMP_ID'])
            soil_pah_df['Result Value Units'] = 'mg/kg'
            soil_pah_df['Sample Matrix'] = 'Soil'
            soil_pah = pd.concat([soil_pah, soil_pah_df], ignore_index= True)
        except Exception as e:
            print(i, "no Soil PAH sheet found")
            print(e)

    # water pah results
    for i in results_files:
        try:
            water_pah_df = pd.read_excel(prev_wb_path+'/'+i, sheet_name = 'PAH Soil to Groundwater')
            print(i , "Water PAH sheet found")
            water_pah_df = water_pah_df[1:]
            #water_pah_df= water_pah_df[pah_cols]
            water_pah_df = water_pah_df.melt(id_vars=['DATE','SAMP_ID'])
            water_pah_df['Result Value Units'] = 'ug/L'
            water_pah_df['Sample Matrix'] = 'Water'
            water_pah = pd.concat([water_pah, water_pah_df], ignore_index= True)
        except Exception as e:
            print(i, "no Water PAH sheet found")
            print(e)

    # soil pcb results
    for i in results_files:
        try:
            soil_pcb_df = pd.read_excel(prev_wb_path+'/'+i, sheet_name = 'PCBs Soils')
            print(i , "PCBs Soils sheet found")
            soil_pcb_df = soil_pcb_df[1:]
            soil_pcb_df= soil_pcb_df[soil_pcb_cols]
            soil_pcb_df = soil_pcb_df.melt(id_vars=['DATE','SAMP_ID','PCB Isomer'])
            soil_pcb_df.drop(columns = 'variable', inplace = True)
            soil_pcb_df.rename(columns = {"PCB Isomer": 'variable'}, inplace = True)
            soil_pcb_df['Result Value Units'] = 'mg/kg'
            soil_pcb_df['Sample Matrix'] = 'Soil'
            soil_pcb_df['PCB_flag'] = 1
            soil_pcb = pd.concat([soil_pcb, soil_pcb_df], ignore_index= True)
        except Exception as e:
            print(i, "PCBs Soils no sheet found")
            print(e)

    # water pcb results
    for i in results_files:

        try:
            water_pcb_df = pd.read_excel(prev_wb_path+'/'+i, sheet_name = 'PCB Waters')
            print(i, "PCB Waters sheet found")

            water_pcb_df = water_pcb_df[1:]

            water_pcb_df= water_pcb_df[water_pcb_cols]
            water_pcb_df = water_pcb_df.melt(id_vars=['DATE','SAMP_ID','PCB Isomer'])
            water_pcb_df.drop(columns = 'variable', inplace = True)
            water_pcb_df.rename(columns = {"PCB Isomer": 'variable'}, inplace = True)
            water_pcb_df['Result Value Units'] = 'ug/L'
            water_pcb_df['Sample Matrix'] = 'Water'
            water_pcb_df['PCB_flag'] = 1
            water_pcb = pd.concat([water_pcb, water_pcb_df], ignore_index= True)
        except Exception as e:
            print(i, "no PCB Waters sheet found")
            print(e)

    # TODO: water TPH results

    ## merge all results
    all_results = pd.concat([soil_rcra,soil_pah,soil_pcb,water_rcra, water_pah, water_pcb])
    all_results.rename(columns = {'variable': 'Result Parameter Name', 'value': 'Result Value', 'SAMP_ID': 'Sample ID', 'Sample Matrix':'Sample Matrix_clean'}, inplace = True)

    # create no detect columns and format result values
    all_results.dropna(subset=['DATE'], inplace = True)
    all_results['Result Value'] = np.where(all_results['Result Value']=='ND',np.NaN,all_results['Result Value'])
    all_results['Result Value'] = np.where(all_results['Result Value']=='--',np.NaN,all_results['Result Value'])

    # clean up pcb values in order to make the join correctly with screening levels
    #print(all_results['Result Parameter Name'].unique())
    #all_results['Result Parameter Name_clean'] = all_results['Result Parameter Name'].apply(lambda x: clean_pcb(x))

    # clean up d/f results to join correctly with screening levels
    # replace Lube Oil to Diesel Range Organics
    all_results['Result Parameter Name_clean'] = np.where(all_results['Result Parameter Name'] == 'Lube Oil', 'Diesel Range Organics', all_results['Result Parameter Name'])

    # calculate total PCBs for epa1668
    tot_pcbs = all_results[all_results['PCB_flag'] == 1]
    tot_pcbs = tot_pcbs.groupby(by =['Sample ID', 'DATE', 'Sample Matrix_clean','Result Value Units']).agg({'Result Value': ['sum']}).reset_index()

    drop_levels(tot_pcbs)
    tot_pcbs['Result Parameter Name_clean'] = 'Total PCBs'

    # export compiled results
    all_results = pd.concat([all_results, tot_pcbs])
    all_results = all_results[all_results['Result Value']!='Y']
    all_results = all_results[all_results['Result Value']!='N']
    all_results['Result Value'] = all_results['Result Value'].astype(float)
    all_results['DATE'] = pd.to_datetime(all_results['DATE'], format="%Y-%m-%d %H:%M:%S").astype("datetime64[ns]")
    all_results.to_csv(output_results_path,index = False)

