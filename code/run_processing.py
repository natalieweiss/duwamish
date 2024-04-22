import process_raw_f_and_b_data, join_to_screening_levels, join_to_sample_points, merge_all_results, combine_prev_screening_results
import add_dioxin_furans_only
import os
import glob
import pandas as pd

# Input your file path for the root directory here
folder_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Results_NEW"

# Initiate lookup tables paths from the root folder
lookup_folder = os.path.join(folder_path, "Lookup")
processed_folder = os.path.join(folder_path, "Processed")
qaqc_folder = os.path.join(folder_path, "QAQC")
raw_folder = os.path.join(folder_path, "Lab_Reports")
prev_wb_path = os.path.join(folder_path, "2023 Screening Results")

# Assign lookup table paths
sample_pts_path = os.path.join(lookup_folder,"Sampling_Sites_MASTER.xlsx")
sl_path = os.path.join(lookup_folder,"Master_Screening_Levels.xlsx")
pcb_arc_lookup_path = os.path.join(lookup_folder,"PCB_aroclor_lookup.csv")
fixed_id_path = os.path.join(lookup_folder,"Sample_ID_Fixed.xlsx")

# Initiate folder paths of raw data
monthly_data = glob.glob(f"{raw_folder}/*", recursive = True)

# Initiate empty dictionary to store all of the metadata, including sampling name and file path
metadata_dict = {}

# Loop through each month of raw data collected and process to join screening levels
for i in monthly_data:
    try:
        sample_outing_name = os.path.basename(i)
        metadata_dict[sample_outing_name] = i
        print('processing raw data')
        process_raw_f_and_b_data.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sample_pts_path = sample_pts_path, raw_data_path= i, fixed_id_path = fixed_id_path)
        print('joining to screening levels')
        join_to_screening_levels.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sl_path = sl_path, pcb_arc_lookup_path = pcb_arc_lookup_path)
        print('joining to sample points')
        join_to_sample_points.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, sample_pts_path = sample_pts_path, qaqc_path=qaqc_folder)
    except Exception as e:
        print(e)

# Individually add in Dioxin Furans
dioxin_furans_data = glob.glob(f"{processed_folder}/Dioxin Furans/*", recursive = True)
for i in dioxin_furans_data:
    sample_outing_name = os.path.basename(i)[:-5]
    metadata_dict[sample_outing_name] = i
    print(sample_outing_name)
    add_dioxin_furans_only.main(processed_path = processed_folder, sample_outing_name = sample_outing_name, results_df_path = i)
    join_to_screening_levels.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sl_path = sl_path, pcb_arc_lookup_path = pcb_arc_lookup_path)
    join_to_sample_points.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, sample_pts_path = sample_pts_path, qaqc_path= qaqc_folder)

# Append results from Pace Labs into the final results table        
'''sample_outing_name = "prev_results"
combine_prev_screening_results.main(processed_path = processed_folder, prev_wb_path = prev_wb_path)
prev_results = pd.read_csv(f"{processed_folder}/{sample_outing_name}.csv")
join_to_screening_levels.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, qaqc_path = qaqc_folder, sl_path = sl_path, pcb_arc_lookup_path = pcb_arc_lookup_path)
join_to_sample_points.main(sample_outing_name = sample_outing_name, processed_path = processed_folder, sample_pts_path = sample_pts_path, qaqc_path = qaqc_folder)
'''

# Merge all of the joined results together
merge_all_results.main(processed_path = processed_folder)