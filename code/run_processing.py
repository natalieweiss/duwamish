import process_raw_f_and_b_data, add_dioxin_furans, join_to_screening_levels, join_to_sample_points, merge_all_results
import os

# Initiate dictionary of sample outing name and data paths
sample_outing_name = 'feb24_add'
data_folder_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/2023 Screening Results/2024.2 add"
df_path = ""

# Initiate lookup tables paths
lookup_folder = ""
processed_folder = ""
qa_qc_folder = ""

#for file in folder, assign each lookup file to a variable that will feed into the functions
sample_pts_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/Lookup Tables/Sampling_Sites_MASTER.csv"
sl_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/Lookup Tables/Master_Screening_Levels.xlsx"
pcb_arc_lookup_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling_Results/Lookup Tables/PCB_aroclor_lookup.csv"
qaqc_path = "/home/nweiss/gdrive/Year 2/Summer - Duwamish/Sampling Results/qaqc"

# Run all scripts
process_raw_f_and_b_data.main(sample_outing_name=sample_outing_name, data_folder_path = data_folder_path, df_path = df_path)
add_dioxin_furans.main()
join_to_screening_levels()
join_to_sample_points()
merge_all_results()