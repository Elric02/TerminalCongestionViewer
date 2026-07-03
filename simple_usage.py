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
date = "2024-08-06"
time_ranges = [
    [[6, 0, 0], [7, 59, 59]]
]
# Note: if a terminal has several operators, add 1 line in the CSV per operator, with the same terminal name (only the coordinates for the first row will be used)
terminals_csv = "terminal_coords.csv"
import_method = "online" # "online" for download from KoDa or "local" if files are already in the tempdata folder
delete_tempdata = True # Whether to delete all GTFS data from the tempdata folder after the operation is completed.
# Enable/disable the different modules here
mod_koda_import = False
mod_uncertainty = False
mod_externalfactors = True


# FUNCTIONS

def export_results(results):
    """Export a dictionary to a text file, creating the file and output directory if needed."""

    file_path = f"output/results/results_{date}_{"-".join(str(item) for sublist in time_ranges for subsublist in sublist for item in subsublist)}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt"

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as out_file:
        json.dump(results, out_file, indent=2, ensure_ascii=False)


def process_terminal(terminal_coordinates_df, terminal_name):
    filtered_terminal_coordinates_df = terminal_coordinates_df.filter(pl.col('terminal') == terminal_name)
    print("Now starting", terminal_name)
    # Get a list of all the (unique) operators for that terminal
    providers = filtered_terminal_coordinates_df.select(pl.col('provider')).to_series().to_list()
    if mod_koda_import:
        # Get the coordinates of the terminal to process
        coords = []
        for col in range(1,7):
            if filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0] is not None:
                coords.append((filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0], filtered_terminal_coordinates_df.select(pl.col('lat'+str(col))).to_series().to_list()[0]))
        print("Coordinates of the terminal:", coords)
        # Build the name of the CSV file
        export_name = "vehiclepositions_terminal_"+terminal_name+"_"+("_".join(providers))+"_"+date+".csv"
        export_path = "output/"+export_name
        total_df_list = []
        # Append to total_df_list the data for each operator
        for provider in providers:
            total_df_list.append(gtfs_import.koda_import_timeframe(provider, date, time_ranges, import_method=import_method, modulo=1, terminal_coordinates=coords, export_type="none", export_name="", delete_tempdata=delete_tempdata))
        total_df = pl.concat(total_df_list, how="diagonal_relaxed")
        total_df.write_csv(export_path)
    if mod_uncertainty:
        imprecision_pooled_mean, imprecision_pooled_std, imprecision_trajs = uncertainty.get_imprecision(terminal_name, date, providers, time_range=[time_ranges[0][0][0], time_ranges[-1][1][0]+1], paths_gpkg=False, dbscan_global_min_samples=3, dbscan_global_eps_percentile=20)
        test_results = {"imprecision": {"imprecision_val": imprecision_pooled_mean, "imprecision_std": imprecision_pooled_std, "imprecision_trajs": imprecision_trajs}}
        export_results(test_results)

def get_factors(terminal_coordinates_df, terminal_name):
    filtered_terminal_coordinates_df = terminal_coordinates_df.filter(pl.col('terminal') == terminal_name)
    coords = []
    for col in range(1,7):
        if filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0] is not None:
            coords.append((filtered_terminal_coordinates_df.select(pl.col('lon'+str(col))).to_series().to_list()[0], filtered_terminal_coordinates_df.select(pl.col('lat'+str(col))).to_series().to_list()[0]))
    print(coords[0][0])
    # Rework locations, I want the terminal locations rather than the weather stations
    '''
    locations_csv_path="weather_stations.csv"
    locations_df = pl.read_csv(locations_csv_path)
    for location in locations_df.iter_rows(named=True):
        print("LOCATION:", location["Name"])
        constellations = [["GPS", "n", "GN"]]
        for constellation in constellations:
            navdata = get_cddis_data(constellation, "RINEX")
            get_hdop(location["Lat"], location["Lon"], navdata, constellation)
            #get_iono_delay(navdata)
            #get_iono_delay2(navdata)
    '''
    print("Getting HDOP and iono delay for coordinates:", coords[0][0], coords[0][1])
    #constellations = [["Beidou", "f", "CN"], ["GLONASS", "g", "RN"], ["Galileo", "l", "EN"], ["GPS", "n", "GN"]]
    constellations = [["GPS", "n", "GN"]]
    desired_datetime = [2024, 9, 13, 13, 0, 0]
    desired_timezone = "Europe/Stockholm"
    visibility_threshold_deg = 12
    elevation_api="geotorget"
    for constellation in constellations:
        hdop = otherfactors_data.get_hdop(
            coords[0][0],
            coords[0][1],
            desired_datetime,
            desired_timezone,
            constellation,
            verbose=True,
            visibility_threshold_deg=visibility_threshold_deg,
            elevation_api=elevation_api
        )
        vtec = otherfactors_data.get_iono_delay(
            coords[0][0],
            coords[0][1],
            desired_datetime,
            desired_timezone,
            constellation,
            verbose=True
        )
    


# MAIN
terminal_coordinates_df = pl.read_csv(terminals_csv)
# Get a list of all the (unique) names of the terminals to process
terminal_names = terminal_coordinates_df.select(pl.col('terminal')).unique().to_series().to_list()
print("Terminals to process:", terminal_names)
for terminal_name in terminal_names:
    process_terminal(terminal_coordinates_df, terminal_name)
    if mod_externalfactors:
        get_factors(terminal_coordinates_df, terminal_name)