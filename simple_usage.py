import polars as pl
import json
import os
from datetime import datetime
from pathlib import Path
import re
# Local libs
import modules.gtfs_import as gtfs_import
import modules.uncertainty as uncertainty
import modules.otherfactors_data as otherfactors_data
import modules.weather_data as weather_data


# PARAMETERS
#provider = "otraf"
#terminal_coordinates = [(15.6218912408347,58.4177694632727), (15.6224848634713,58.4180357142838), (15.6250694051049,58.416677334774), (15.6248715308927,58.4162404738934)] # Linköping Central coordinates
# Note: if a terminal has several operators, add 1 line in the CSV per operator, with the same terminal name (only the coordinates for the first row will be used)
terminals_csv = "terminal_coords.csv"
import_method = "online" # "online" for download from KoDa or "local" if files are already in the tempdata folder
delete_tempdata = True # Whether to delete all GTFS data from the tempdata folder after the operation is completed.
# Enable/disable the different modules here
mod_koda_import = False
mod_uncertainty = False
mod_weatherfactors = False
mod_otherfactors = False
mod_process_results = True # Independent module, takes the .txt results and processes them rather than adding something in the txt files
year = "2024"
months = ["01", "07"]
days = ["01", "06", "11", "16", "21", "26"]
hours = [7, 16]
comparison_group_by = "berths"

# Only used if mod_process_results is True
results_folder_path = "output/results/A2 linköping otraf"
results_csv_path = "output/merged_results_linköping_otraf_a2.csv"


# Whole day import
#gtfs_import.koda_import_timeframe("otraf", "2024-09-01", None, import_method="online", modulo=1, terminal_coordinates=terminal_coordinates, export_type="csv", delete_tempdata=True)


# FUNCTIONS

def export_results(results, date, time_ranges):
    """Export a dictionary to a text file, creating the file and output directory if needed."""

    file_path = f"output/results/results_{date}_{"-".join(str(item) for sublist in time_ranges for subsublist in sublist for item in subsublist)}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt"

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as out_file:
        json.dump(results, out_file, indent=2, ensure_ascii=False)


def process_terminal(terminal_coordinates_df, terminal_name, date, time_ranges):
    filtered_terminal_coordinates_df = terminal_coordinates_df.filter(pl.col('terminal') == terminal_name)
    print("Now starting", terminal_name, "for the following date/times:", date, time_ranges)
    # Get a list of all the (unique) operators for that terminal
    providers = filtered_terminal_coordinates_df.select(pl.col('provider')).to_series().to_list()
    uncertainty_results = {}
    # Build the name of the CSV file here instead of in the gtfs_import module
    export_name = "vehiclepositions_terminal_"+terminal_name+"_"+("_".join(providers))+"_"+date+"_"+"-".join(str(item) for sublist in time_ranges for subsublist in sublist for item in subsublist)+".csv"
    export_path = "output/vehiclepositions/"+export_name
    if mod_koda_import:
        # Get the coordinates of the terminal to process
        coords = []
        for col in range(1,7):
            if filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0] is not None:
                coords.append((filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0], filtered_terminal_coordinates_df.select(pl.col('lat'+str(col))).to_series().to_list()[0]))
        print("Coordinates of the terminal:", coords)
        total_df_list = []
        # Append to total_df_list the data for each operator
        for provider in providers:
            total_df_list.append(gtfs_import.koda_import_timeframe(provider, date, time_ranges, import_method=import_method, modulo=1, terminal_coordinates=coords, export_type="none", export_name="", delete_tempdata=delete_tempdata))
        total_df = pl.concat(total_df_list, how="diagonal_relaxed")
        total_df.write_csv(export_path)
    if mod_uncertainty:
        static_data = {}
        for provider in providers:
            static_data[provider] = gtfs_import.koda_import_static(provider=provider, date=date, import_method="local", delete_tempdata=False)
        imprecision_pooled_mean, imprecision_pooled_std, imprecision_trajs = uncertainty.get_imprecision(terminal_name, date, providers, time_range=[time_ranges[0][0][0], time_ranges[-1][1][0]+1], min_trips_for_clustering=3, 
                                                                                                         vehiclepositions_path=export_path, paths_gpkg=False, verbose=False, dbscan_global_min_samples=3, dbscan_global_eps_percentile=20,
                                                                                                         comparison_group_by=comparison_group_by, static_data=static_data
                                                                                                        )
        uncertainty_results = uncertainty_results | {"imprecision": {"imprecision_val": imprecision_pooled_mean, "imprecision_std": imprecision_pooled_std, "imprecision_trajs": imprecision_trajs}}
    return uncertainty_results

def get_weatherfactors(terminal_coordinates_df, terminal_name, date, time_ranges):
    filtered_terminal_coordinates_df = terminal_coordinates_df.filter(pl.col('terminal') == terminal_name)
    coords = []
    for col in range(1,7):
        if filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0] is not None:
            coords.append((filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0], filtered_terminal_coordinates_df.select(pl.col('lat'+str(col))).to_series().to_list()[0]))
    print("Getting weather data for coordinates:", coords[0][0], coords[0][1])
    desired_datetime = [int(date.split("-")[0]), int(date.split("-")[1]), int(date.split("-")[2]), time_ranges[0][0][0] + 1, time_ranges[0][0][1], time_ranges[0][0][2]]
    desired_timezone = "Europe/Stockholm"
    weather_params = ["1", "6", "7", "9"]
    weather_results = {"weather": weather_data.get_weather(coords[0][1], coords[0][0], desired_datetime, desired_timezone, weather_params)}
    return weather_results

def get_otherfactors(terminal_coordinates_df, terminal_name, date, time_ranges):
    filtered_terminal_coordinates_df = terminal_coordinates_df.filter(pl.col('terminal') == terminal_name)
    coords = []
    for col in range(1,7):
        if filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0] is not None:
            coords.append((filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0], filtered_terminal_coordinates_df.select(pl.col('lat'+str(col))).to_series().to_list()[0]))
    print("Getting HDOP and iono delay for coordinates:", coords[0][0], coords[0][1])
    #constellations = [["BeiDou", "f", "CN"], ["GLONASS", "g", "RN"], ["Galileo", "l", "EN"], ["GPS", "n", "GN"]]
    constellations = [["GPS", "n", "GN"]]
    # Set desired time as 1h later than the beginning of the time range (so that it is in the middle when the time range is of 2h)
    desired_datetime = [int(date.split("-")[0]), int(date.split("-")[1]), int(date.split("-")[2]), time_ranges[0][0][0] + 1, time_ranges[0][0][1], time_ranges[0][0][2]]
    desired_timezone = "Europe/Stockholm"
    elevation_api = "geotorget"
    otherfactors_results = {}
    for constellation in constellations:
        hdop = otherfactors_data.get_hdop(
            coords[0][1],
            coords[0][0],
            desired_datetime,
            desired_timezone,
            constellation,
            verbose=False,
            elevation_api=elevation_api
        )
        vtec = otherfactors_data.get_iono_delay(
            coords[0][1],
            coords[0][0],
            desired_datetime,
            desired_timezone,
            constellation,
            verbose=False
        )
        otherfactors_results[constellation[0]] = {"hdop": hdop, "vtec": vtec}
    return otherfactors_results

    
def process_results(folder_path, output_csv=False, csv_path="output/merged_results.csv"):
    """Read all .txt files in the results folder, each containing a single nested dict, flatten and merge them into one Polars DataFrame.
    A datetime column is added, parsed from each filename. Optionnally write merged results to a CSV file.
 
    :param folder_path: Path to the folder containing the .txt files.
    :type folder_path: str
    :param output_csv: Whether to output the merged DataFrame to a CSV file.
    :type output_csv: bool
    :param csv_path: Path for the output CSV file (saved inside folder_path).
    :type csv_path: str
    :return: The merged DataFrame.
    :rtype: pl.DataFrame
    """
    
    def _flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
        items = {}
        for key, value in d.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.update(_flatten_dict(value, new_key, sep=sep))
            else:
                items[new_key] = value
        return items

    folder = Path(folder_path)
    records = []
    for txt_file in sorted(folder.glob("*.txt")):
        print(f"Processing file: {txt_file.name}")
        # Read the file, convert its content to a dict, flatten it, and extract the datetime from the filename
        content = txt_file.read_text(encoding="utf-8").strip()
        data = json.loads(content)
        flat_data = _flatten_dict(data)
        match = re.compile(r"results_(\d{4}-\d{2}-\d{2})_(\d+)-(\d+)-(\d+)-").match(txt_file.name)
        date_str, hour, minute, second = match.groups()
        flat_data["datetime"] = datetime.strptime(f"{date_str} {int(hour):02d}:{int(minute):02d}:{int(second):02d}", "%Y-%m-%d %H:%M:%S")
        records.append(flat_data)
    if not records:
        raise ValueError(f"No .txt files found in {folder_path}")
    results_df = pl.DataFrame(records)
    if output_csv:
        results_df.write_csv(csv_path)
    return results_df


# MAIN
if mod_koda_import or mod_uncertainty or mod_weatherfactors or mod_otherfactors:
    terminal_coordinates_df = pl.read_csv(terminals_csv)
    # Get a list of all the (unique) names of the terminals to process
    terminal_names = terminal_coordinates_df.select(pl.col('terminal')).unique().to_series().to_list()
    print("Terminals to process:", terminal_names)
    for month in months:
        for day in days:
            date = f"{year}-{month}-{day}"
            for hour in hours:
                time_ranges = [
                    [[hour-1, 0, 0], [hour, 59, 59]]
                ]
                print("****************************************")
                print("****************************************")
                print("****************************************")
                print("****************************************")
                print("NOW STARTING PROCESSING FOR DATE:", date, "TIME RANGE:", time_ranges)
                print("****************************************")
                print("****************************************")
                print("****************************************")
                print("****************************************")
                for terminal_name in terminal_names:
                    uncertainty_results = process_terminal(terminal_coordinates_df, terminal_name, date, time_ranges)
                    otherfactors_results = {}
                    weatherfactors_results = {}
                    if mod_weatherfactors:
                        weatherfactors_results = get_weatherfactors(terminal_coordinates_df, terminal_name, date, time_ranges)
                    if mod_otherfactors:
                        otherfactors_results = get_otherfactors(terminal_coordinates_df, terminal_name, date, time_ranges)
                    results = uncertainty_results | otherfactors_results | weatherfactors_results
                    export_results(results, date, time_ranges)

if mod_process_results:
    results_df = process_results(results_folder_path, output_csv=True, csv_path=results_csv_path)
    print(results_df)


print("*** DONE! ***")