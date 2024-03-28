import process_raw_f_and_b_data, add_dioxin_furans, join_to_screening_levels, join_to_sample_points, merge_all_results
import os
import glob
import pandas as pd
import numpy as np
from math import floor, log10


folder_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Results_NEW"

# Initiate lookup tables paths
processed_folder = os.path.join(folder_path, "Processed")

# create mini reports
def make_pivot(df, med, group, name):
    df = df[(df['Chemical Group']==group)]
    df = df[df['Medium']==med]
    df['Screening Level'] = df['Screening Level'].astype(float).apply(lambda x: round(x, 2 - int(floor(log10(abs(x))))) if x > 0 else 0)

    df.drop_duplicates(inplace = True)
    df.sort_values(by = 'Sample ID')
    df.set_index(['Sample ID', 'DATE'], append=True, inplace = True)

    pivot_df = df.pivot(columns=['Chemical Group', 'Chemical','Source', 'Screening Level', 'SL Unit'], values='SL_exceeded')
    pivot_df = pivot_df.groupby(['Sample ID', 'DATE']).agg(max)
    pivot_df.replace({1:"Y", 0:"N"}, inplace = True)
    if len(pivot_df)>0:
        try:
            with pd.ExcelWriter(f"{processed_folder}/{name}_event.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
                pivot_df.to_excel(writer, sheet_name = f'PIVOT_{group}_{med}')
        except Exception as e:
            print(group, med)
            print(e)

######### Tree Planting Event ###################
tree_planting_ids = ['GUF-1-S-1', 'GUF-1-S-2', 'GUF-1-S-3', 'GUF-1-S-4']
all_results = merge_all_results.main(processed_path = processed_folder)

tree_planting = []
for sample_id in tree_planting_ids:
    df = all_results[all_results['Sample ID'] == sample_id]
    tree_planting.append(df)

tree_planting = pd.concat(tree_planting)
tree_planting['SL_exceeded'] = np.where(tree_planting['SL_exceeded'] == 'Y', 1, 0)
chemical_groups = ['Dioxin Furans', 'PCB', 'RCRA8', 'TPH', 'PAH']
medium = ['Soil', 'Water']
tree_planting.to_excel(f"{processed_folder}/tree_planting_event.xlsx", index = False, sheet_name = 'RAW_DATA')
with pd.ExcelWriter(f"{processed_folder}/tree_planting_event.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
    tree_planting.to_excel(writer, sheet_name = 'RAW_DATA')

for group in chemical_groups:
    for med in medium:
        make_pivot(df=tree_planting, med=med, group=group, name='tree_planting')


######### Bioswale Event ###################

bioswale_event_dates = ['2024-01-18', '2024-01-21','2024-02-16']
bioswale_event = []
for bioswale_date in bioswale_event_dates:
    df = all_results[all_results['DATE'] == bioswale_date]
    bioswale_event.append(df)

bioswale_event = pd.concat(bioswale_event)
bioswale_event['SL_exceeded'] = np.where(bioswale_event['SL_exceeded'] == 'Y', 1, 0)
chemical_groups = ['Dioxin Furans', 'PCB', 'RCRA8', 'TPH', 'PAH']
medium = ['Soil', 'Water']
bioswale_event.to_excel(f"{processed_folder}/bioswale_event.xlsx", sheet_name = 'RAW_DATA', index = False)
with pd.ExcelWriter(f"{processed_folder}/bioswale_event.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
    bioswale_event.to_excel(writer, sheet_name = 'RAW_DATA', index = False)

for group in chemical_groups:
    for med in medium:
        make_pivot(df=bioswale_event, med=med, group=group, name='bioswale')

