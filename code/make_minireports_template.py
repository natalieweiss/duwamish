import os
import pandas as pd
import numpy as np
from math import floor, log10

# Input your file path for the root directory here
# The file path should look like this: "C:\\duwamish_code"
# All file paths need to have double backslash (\\) as the separators
folder_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Results_NEW"


# Initiate report parameters
# You can either specify the samples by their date OR their name depending on your purpose
event_name = 'DRCC' # event name or community group name
event_ids = [] # sample IDs of interest. If you are specifying by date, write empty brackets []
event_dates = [] # sample dates of interest in YYYY-MM-DD with leading 0s for month and date. If you are specifying by ID, write empty brackets []

# Initiate lookup tables paths
processed_folder = os.path.join(folder_path, "Processed")

# create table of exceedances
def make_pivot(df, med, group, name):
    df = df[(df['Chemical Group']==group)]
    df = df[df['Medium']==med]
    df['Screening Level'] = df['Screening Level'].astype(float).apply(lambda x: round(x, 2 - int(floor(log10(abs(x))))) if x > 0 else 0)
    
    df.drop_duplicates(inplace = True)
    df.sort_values(by = 'Sample ID')
    df.set_index(['Sample ID', 'DATE'], append=True, inplace = True)

    pivot_df = df.pivot(columns=['Chemical Group', 'Chemical','Reference', 'Pathway_for_Report','Screening Level', 'SL Unit'], values='SL_exceeded')
    pivot_df = pivot_df.groupby(['Sample ID', 'DATE']).agg("max")
    pivot_df.replace({1:"Y", 0:"N"}, inplace = True)
    if len(pivot_df)>0:
        try:
            with pd.ExcelWriter(f"{processed_folder}/{event_name}_exceedances.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
                pivot_df.to_excel(writer, sheet_name = f'PIVOT_{group}_{med}')
        except Exception as e:
            print(group, med)
            print(e)

# create table of results
def make_result_pivot(df, med, group, name):
    df = df[(df['Chemical Group']==group)]
    df = df[df['Medium']==med]
    df['Screening Level'] = df['Screening Level'].astype(float).apply(lambda x: round(x, 2 - int(floor(log10(abs(x))))) if x > 0 else 0)
    
    df.drop_duplicates(inplace = True)
    df.sort_values(by = 'Sample ID')
    df.set_index(['Sample ID', 'DATE'], append=True, inplace = True)

    pivot_df = df.pivot(columns=['Chemical Group', 'Chemical'], values='Result Value')
    pivot_df = pivot_df.groupby(['Sample ID', 'DATE']).agg("max")
    if len(pivot_df)>0:
        try:
            with pd.ExcelWriter(f"{processed_folder}/{event_name}_RESULTS.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
                pivot_df.to_excel(writer, sheet_name = f'PIVOT_{group}_{med}')
        except Exception as e:
            print(group, med)
            print(e)


##### Create report by Sample ID or Sample Date #####
all_results = pd.read_csv(f"{processed_folder}/agg_results.csv")
all_results['DATE'] = all_results['DATE'].str[:10]
all_results['Chemical']= all_results['Chemical'].str.strip()
all_results.sort_values(by = ['DATE', 'Chemical'], inplace = True)
results_df = []

if len(event_ids) > 0:
    print(f'ids included in report: {event_ids}')
    for sample_id in event_ids:
        df = all_results[all_results['Sample ID'] == sample_id]
        results_df.append(df)
    
if len(event_dates) > 0:
    for sample_date in event_dates:
        print(f'dates included in report: {event_dates}')
        df = all_results[all_results['DATE'] == sample_date]
        results_df.append(df)

if len(event_dates) == 0:
    if len(event_ids) == 0:
        print('all dates and ids included in report')
        results_df.append(all_results)

results_df = pd.concat(results_df)
results_df.drop_duplicates(inplace = True)
results_df['SL_exceeded'] = np.where(results_df['SL_exceeded'] == 'Y', 1, 0)
chemical_groups = ['Dioxin Furans', 'PCB', 'RCRA8', 'TPH', 'PAH']
medium = ['Soil', 'Water']

results_df.to_excel(f"{processed_folder}/{event_name}_exceedances.xlsx", index = False, sheet_name = 'RAW_DATA')
with pd.ExcelWriter(f"{processed_folder}/{event_name}_exceedances.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
    results_df.to_excel(writer, sheet_name = 'RAW_DATA', index = False)

results_df.to_excel(f"{processed_folder}/{event_name}_RESULTS.xlsx", index = False, sheet_name = 'RAW_DATA')
with pd.ExcelWriter(f"{processed_folder}/{event_name}_RESULTS.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
    results_df.to_excel(writer, sheet_name = 'RAW_DATA', index = False)

for group in chemical_groups:
    for med in medium:
        print(f"Writing out to Excel Sheet: {group}, {med}")
        make_pivot(df=results_df, med=med, group=group, name=event_name)
        make_result_pivot(df=results_df, med=med, group=group, name=event_name)
