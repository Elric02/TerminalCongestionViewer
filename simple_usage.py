import polars as pl
import json
import os
from datetime import datetime
# Local libs
import modules.gtfs_import as gtfs_import
import modules.uncertainty as uncertainty
import modules.otherfactors_data as otherfactors_data
import modules.weather_data as weather_data


# PARAMETERS
#provider = "otraf"
#terminal_coordinates = [(15.621, 58.416), (15.626, 58.416), (15.621, 58.419), (15.626, 58.419)]
# Note: if a terminal has several operators, add 1 line in the CSV per operator, with the same terminal name (only the coordinates for the first row will be used)
terminals_csv = "terminal_coords.csv"
import_method = "online" # "online" for download from KoDa or "local" if files are already in the tempdata folder
delete_tempdata = True # Whether to delete all GTFS data from the tempdata folder after the operation is completed.
# Enable/disable the different modules here
mod_koda_import = False
mod_uncertainty = False
mod_weatherfactors = True
mod_otherfactors = False


# FUNCTIONS

def export_results(results, date, time_ranges):
    """Export a dictionary to a text file, creating the file and output directory if needed."""

    file_path = f"output/results/results_{date}_{"-".join(str(item) for sublist in time_ranges for subsublist in sublist for item in subsublist)}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt"

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as out_file:
        json.dump(results, out_file, indent=2, ensure_ascii=False)


def process_terminal(terminal_coordinates_df, terminal_name, date, time_ranges):
    filtered_terminal_coordinates_df = terminal_coordinates_df.filter(pl.col('terminal') == terminal_name)
    print("Now starting", terminal_name)
    # Get a list of all the (unique) operators for that terminal
    providers = filtered_terminal_coordinates_df.select(pl.col('provider')).to_series().to_list()
    uncertainty_results = {}
    if mod_koda_import:
        # Get the coordinates of the terminal to process
        coords = []
        for col in range(1,7):
            if filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0] is not None:
                coords.append((filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0], filtered_terminal_coordinates_df.select(pl.col('lat'+str(col))).to_series().to_list()[0]))
        print("Coordinates of the terminal:", coords)
        # Build the name of the CSV file here instead of in the gtfs_import module
        export_name = "vehiclepositions_terminal_"+terminal_name+"_"+("_".join(providers))+"_"+date+"_"+"-".join(str(item) for sublist in time_ranges for subsublist in sublist for item in subsublist)+".csv"
        export_path = "output/vehiclepositions/"+export_name
        total_df_list = []
        # Append to total_df_list the data for each operator
        for provider in providers:
            total_df_list.append(gtfs_import.koda_import_timeframe(provider, date, time_ranges, import_method=import_method, modulo=1, terminal_coordinates=coords, export_type="none", export_name="", delete_tempdata=delete_tempdata))
        total_df = pl.concat(total_df_list, how="diagonal_relaxed")
        total_df.write_csv(export_path)
    if mod_uncertainty:
        imprecision_pooled_mean, imprecision_pooled_std, imprecision_trajs = uncertainty.get_imprecision(terminal_name, date, providers, time_range=[time_ranges[0][0][0], time_ranges[-1][1][0]+1], vehiclepositions_path=export_path, paths_gpkg=False, verbose=False, dbscan_global_min_samples=3, dbscan_global_eps_percentile=20)
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
    #constellations = [["Beidou", "f", "CN"], ["GLONASS", "g", "RN"], ["Galileo", "l", "EN"], ["GPS", "n", "GN"]]
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
            verbose=True,
            elevation_api=elevation_api
        )
        vtec = otherfactors_data.get_iono_delay(
            coords[0][1],
            coords[0][0],
            desired_datetime,
            desired_timezone,
            constellation,
            verbose=True
        )
        otherfactors_results[constellation[0]] = {"hdop": hdop, "vtec": vtec}
    return otherfactors_results
    


# MAIN
months = ["09"]
days = ["11", "16", "21", "26"]
hours = [7, 12, 16, 21]
year = "2024"
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