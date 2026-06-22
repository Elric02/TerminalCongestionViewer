import polars as pl
import koda_lib
import json
import os
from datetime import datetime


# PARAMETERS
#provider = "otraf"
#terminal_coordinates = [(15.621, 58.416), (15.626, 58.416), (15.621, 58.419), (15.626, 58.419)]
date = "2024-09-30"
time_ranges = [
    [[6, 0, 0], [7, 59, 59]]
]
# Note: if a terminal has several operators, add 1 line in the CSV per operator, with the same terminal name (only the coordinates for the first row will be used)
terminals_csv = "terminal_coords.csv"
import_method = "online" # "online" for download from KoDa or "local" if files are already in the tempdata folder
delete_tempdata = True # Whether to delete all GTFS data from the tempdata folder after the operation is completed.

# FUNCTIONS

def export_results(results):
    """Export a dictionary to a text file, creating the file and output directory if needed."""

    file_path = f"output/results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt"

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as out_file:
        json.dump(results, out_file, indent=2, ensure_ascii=False)


def process_terminal(terminal_coordinates_df, terminal_name):
    print("Now starting", terminal_name)
    # Get the coordinates of the terminal to process
    coords = []
    for col in range(1,7):
        if terminal_coordinates_df.filter(pl.col('terminal') == terminal_name).select(pl.col('lon'+str(col))).to_series().to_list()[0] is not None:
            coords.append((terminal_coordinates_df.filter(pl.col('terminal') == terminal_name).select(pl.col('lon'+str(col))).to_series().to_list()[0], terminal_coordinates_df.filter(pl.col('terminal') == terminal_name).select(pl.col('lat'+str(col))).to_series().to_list()[0]))
    print("Coordinates of the terminal:", coords)
    # Get a list of all the (unique) operators for that terminal
    providers = terminal_coordinates_df.filter(pl.col('terminal') == terminal_name).select(pl.col('provider')).to_series().to_list()
    # Build the name of the CSV file
    export_name = "vehiclepositions_terminal_"+terminal_name+"_"+("_".join(providers))+"_"+date+".csv"
    export_path = "output/"+export_name
    total_df_list = []
    # Append to total_df_list the data for each operator
    for provider in providers:
        total_df_list.append(koda_lib.import_timeframe(provider, date, time_ranges, import_method=import_method, modulo=1, terminal_coordinates=coords, export_type="none", export_name="", delete_tempdata=delete_tempdata))
    total_df = pl.concat(total_df_list, how="diagonal_relaxed")
    total_df.write_csv(export_path)

# MAIN
#terminal_coordinates_df = pl.read_csv(terminals_csv)
# Get a list of all the (unique) names of the terminals to process
#terminal_names = terminal_coordinates_df.select(pl.col('terminal')).unique().to_series().to_list()
#print("Terminals to process:", terminal_names)
#for terminal_name in terminal_names:
#    process_terminal(terminal_coordinates_df, terminal_name)
test_results = {"module1": {"test_results": 42}, "module2": {"test_results": 69}}
export_results(test_results)