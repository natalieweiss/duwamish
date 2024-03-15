import process_raw_f_and_b_data, add_dioxin_furans, join_to_screening_levels, join_to_sample_points, merge_all_results
import os
import glob
import pandas as pd

folder_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Results_NEW"

# Initiate lookup tables paths
lookup_folder = os.path.join(folder_path, "Lookup")
processed_folder = os.path.join(folder_path, "Processed")
qaqc_folder = os.path.join(folder_path, "QAQC")
raw_folder = os.path.join(folder_path, "Raw")

#for file in folder, assign each lookup file to a variable that will feed into the functions
sample_pts_path = os.path.join(lookup_folder,"Sampling_Sites_Master.csv")
sl_path = os.path.join(lookup_folder,"Master_Screening_Levels.xlsx")
pcb_arc_lookup_path = os.path.join(lookup_folder,"PCB_aroclor_lookup.csv")

# initiate folder paths of monthly data
monthly_data = glob.glob(f"{raw_folder}/*", recursive = True)

metadata_dict = {}
for i in monthly_data:
    sample_outing_name = os.path.basename(i)
    metadata_dict[sample_outing_name] = i

# Run all scripts
#sample_outing_name = '2024.2'
#test_run = metadata_dict[sample_outing_name]
#process_raw_f_and_b_data.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sample_pts_path = sample_pts_path, raw_data_path=test_run)
#add_dioxin_furans.main()
#join_to_screening_levels.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sl_path = sl_path, pcb_arc_lookup_path = pcb_arc_lookup_path)
#join_to_sample_points.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, sample_pts_path = sample_pts_path)

# create mini reports
    
######### Tree Planting Event ###################
tree_planting_ids = ['GUF-1-S-1', 'GUF-1-S-2', 'GUF-1-S-3', 'GUF-1-S-4']
all_results = merge_all_results.main(processed_path= processed_folder)

tree_planting = []
for sample_id in tree_planting_ids:
    df = all_results[all_results['Sample ID'] == sample_id]
    tree_planting.append(df)

tree_planting = pd.concat(tree_planting)
chemical_groups = ['Dioxin Furans', 'PCB', 'RCRA8', 'TPH', 'PAH']
medium = ['Soil', 'Water']
tree_planting.to_excel(f"{processed_folder}/tree_planting_event.xlsx", sheet_name = 'RAW_DATA')
with pd.ExcelWriter(f"{processed_folder}/tree_planting_event.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
    tree_planting.to_excel(writer, sheet_name = 'RAW_DATA')

for group in chemical_groups:
    for med in medium:
        df = tree_planting[(tree_planting['Chemical Group']==group)]
        df = df[df['Medium']==med]
        df.set_index(['Sample ID', 'DATE'], append=True, inplace = True)
        pivot_df = df.pivot(columns=['Chemical Group', 'Chemical','Source', 'Screening Level', 'SL Unit'], values='SL_exceeded')
        if len(pivot_df)>0:
            try:
                with pd.ExcelWriter(f"{processed_folder}/tree_planting_event.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
                    pivot_df.to_excel(writer, sheet_name = f'PIVOT_{group}_{med}')
            except Exception as e:
                print(group, med)
                print(e)
        else:
            continue


######### Bioswale Event ###################
bioswale_event_dates = ['2024-01-18', '2024-01-21']
bioswale_event = []
for bioswale_date in bioswale_event_dates:
    df = all_results[all_results['DATE'] == bioswale_date]
    bioswale_event.append(df)

bioswale_event = pd.concat(bioswale_event)
#bioswale_event = bioswale_event[bioswale_event['SL_exceeded'] != 'No Screening Level Identified']
#bioswale_event.set_index(['Sample ID', 'DATE', 'Medium'], append=True, inplace = True)

chemical_groups = ['Dioxin Furans', 'PCB', 'RCRA8', 'TPH', 'PAH']
medium = ['Soil', 'Water']
tree_planting.to_excel(f"{processed_folder}/bioswale_event.xlsx", sheet_name = 'RAW_DATA')
with pd.ExcelWriter(f"{processed_folder}/bioswale_event.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
    bioswale_event.to_excel(writer, sheet_name = 'RAW_DATA')

for group in chemical_groups:
    for med in medium:
        df = bioswale_event[(bioswale_event['Chemical Group']==group)]
        df = df[df['Medium']==med]
        df.set_index(['Sample ID', 'DATE'], append=True, inplace = True)
        pivot_df = df.pivot(columns=['Chemical Group', 'Chemical','Source', 'Screening Level','SL Unit'], values='SL_exceeded')
        if len(pivot_df)>0:
            try:
                with pd.ExcelWriter(f"{processed_folder}/bioswale_event.xlsx", mode = 'a', if_sheet_exists='replace') as writer: 
                    pivot_df.to_excel(writer, sheet_name = f'PIVOT_{group}_{med}')
            except Exception as e:
                print(group, med)
                print(e)
        else:
            continue