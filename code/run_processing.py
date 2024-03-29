import process_raw_f_and_b_data, add_dioxin_furans, join_to_screening_levels, join_to_sample_points, merge_all_results, combine_prev_screening_results
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
sample_pts_path = os.path.join(lookup_folder,"Sampling_Sites_Master_w_river_mile.csv")
sl_path = os.path.join(lookup_folder,"Master_Screening_Levels.xlsx")
pcb_arc_lookup_path = os.path.join(lookup_folder,"PCB_aroclor_lookup.csv")

# initiate folder paths of monthly data
monthly_data = glob.glob(f"{raw_folder}/*", recursive = True)

metadata_dict = {}

for i in monthly_data:
    try:
        sample_outing_name = os.path.basename(i)
        metadata_dict[sample_outing_name] = i
        print('processing raw data')
        process_raw_f_and_b_data.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sample_pts_path = sample_pts_path, raw_data_path= i)
        print('adding dioxin furans')
        add_dioxin_furans.main(sample_outing_name = sample_outing_name, processed_path = processed_folder)
        print('joining to screening levels')
        join_to_screening_levels.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sl_path = sl_path, pcb_arc_lookup_path = pcb_arc_lookup_path)
        print('joining to sample points')
        join_to_sample_points.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, sample_pts_path = sample_pts_path)
    except Exception as e:
        print(e)

# add in dioxin furans
dioxin_furans_data = glob.glob(f"{processed_folder}/Dioxin Furans/*", recursive = True)
'''for i in dioxin_furans_data:
    try:
        print(i)
        sample_outing_name = os.path.basename(i)
        metadata_dict[sample_outing_name] = i
        add_dioxin_furans.main(sample_outing_name = sample_outing_name, results_df_path = i, processed_path = processed_folder)
        join_to_screening_levels.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sl_path = sl_path, pcb_arc_lookup_path = pcb_arc_lookup_path)
        print('joining to sample points')
        join_to_sample_points.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, sample_pts_path = sample_pts_path)
    except Exception as e:
        print(e)'''

# add in previous results         
#sample_outing_name = "prev_results"
#combine_prev_screening_results.main(processed_path = processed_folder, prev_wb_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/2023 Screening Results")
#prev_results = pd.read_csv(f"{processed_folder}/{sample_outing_name}.csv")
#join_to_screening_levels.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sl_path = sl_path, pcb_arc_lookup_path = pcb_arc_lookup_path)
#join_to_sample_points.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, sample_pts_path = sample_pts_path)

# Run all scripts
merge_all_results.main(processed_path = processed_folder)