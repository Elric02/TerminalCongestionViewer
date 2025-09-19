#TODO: import automatically data

import polars as pl
import numpy as np
import read_protobuf
import gtfs_realtime_pb2 # Local library, no need to install this
import math
import os
import time
import shutil
import zipfile


def import_timeframe(provider, date, time_ranges, import_method="online", vehiclepositions_path=None, staticdata_path=None, modulo=1, export_type="none"): 
    """Import VehiclePositions data from KoDa for the specified timeframe in a specific day

    :param provider: The desired provider code (e.g. otraf, sl, ul...), full list here:
        https://www.trafiklab.se/api/gtfs-datasets/gtfs-regional#operators-covered-by-this-dataset
    :type provider: str
    :param date: The desired date in format "YYYY-MM-DD"
    :type date: str
    :param time_ranges: List of 2-element-lists of 3-elements-list: start and beginning of desired time frames,
        where sub-sub-lists take the shape [h, min, sec] (e.g. `[[[7, 0, 0], [7, 59, 59]]]` -> only 1 timeframe, from 07:00:00 to 07:59:59 both included)
    :type time_ranges: list[list[list[int]]]
    :param import_method: Where to get the data (available: online, local), where "online" is requesting directly from KoDa and "local" is from local files
    :type import_method: str
    :param vehiclepositions_path: (Only if import_method=="local") The path to the content of the VehiclePositions folder (e.g. "../data/realtime/VehiclePositions")
    :type vehiclepositions_path: str
    :param staticdata_path: (Only if import_method=="local") The path to the folder containing static GTFS data, i.e. routes.txt, trips.txt... (e.g. "../data/static")
    :type staticdata_path: str
    :param modulo: Consider only 1 every x seconds, where x is this variable. Useful when using large timeframes
    :type modulo: int
    :param export_type: Which file type to export in (available: none, csv). The function always returns a DataFrame anyway
    :type export_type: str
    """

    def appendNewPBSecond(hour, minute, second, total_df, MessageType, trips, import_method):
        try:
            filename = provider+'-vehiclepositions-'+date+'T'+hour+'-'+minute+'-'+second+'Z.pb'
            temp_df = read_protobuf.read_protobuf(vehiclepositions_path+'/'+date[0:4]+'/'+date[5:7]+'/'+date[8:]+'/'+hour+'/'+filename, MessageType)
            temp_df = pl.DataFrame(temp_df['entity'].tolist())
            temp_df = temp_df.with_columns(pl.lit(filename).alias("source"))

            routes_list = []
            directions_list = []
            route_short_name_list = []
            route_type_list = []
            for row in temp_df.iter_rows():
                trip_id = row[temp_df.get_column_index('trip_id')]
                if trip_id is not None:
                    trip = trips.filter(pl.col("trip_id") == int(trip_id))
                    if not trip.is_empty():
                        route_id_val = trip.select("route_id").item()
                        direction_id_val = trip.select("direction_id").item()

                        routes_list.append(route_id_val)
                        directions_list.append(direction_id_val)

                        route = routes.filter(pl.col("route_id") == str(route_id_val))
                        route_short_name_val = route.select("route_short_name").item()
                        route_type_val = route.select("route_type").item()

                        route_short_name_list.append(str(route_short_name_val))
                        route_type_list.append(route_type_val)
                    else:
                        routes_list.append(-1)
                        directions_list.append(-1)
                        route_short_name_list.append(-1)
                        route_type_list.append(-1)
                else:
                    routes_list.append(-1)
                    directions_list.append(-1)
                    route_short_name_list.append(-1)
                    route_type_list.append(-1)
            temp_df = temp_df.with_columns([
                pl.Series("route_id", routes_list),
                pl.Series("direction_id", directions_list),
                pl.Series("route_short_name", route_short_name_list, strict=False),
                pl.Series("route_type", route_type_list),
            ])
            
            total_df = pl.concat([total_df, temp_df], how="diagonal")
        except FileNotFoundError:
            print("File not found:", filename)
        return total_df
    
    def prepare_file_on_server(url):
        time_waited = 0
        while True:
            print("Requesting data from", url)
            res_s = os.popen('curl -s -w %{http_code} -I "' + url + '"')
            res = res_s.read()
            if '200' in res: # Data ready to download
                print("Data is ready to download")
                return
            elif '202' in res: # Data will be prepared
                print("Data is being prepared on the server")
                print("Waiting. Time waited so far:", str(time_waited), "minutes")
                time.sleep(60)
                time_waited += 1
                continue
            else:
                raise Exception("Unknown response from server: " + res)

    def download_data(path, url):
        print("downloading file with command: curl -s -o " + path + ' "' + url + '"')
        os.system("curl -s -o" + path + ' "' + url + '"')
    
    if import_method == "online":
        try:
            api_key =  open("koda_api_key.txt", "r").read()
        except FileNotFoundError:
            print('KoDa API key not found. Please create a file named "koda_api_key.txt" in the same directory as this code, and paste your API key inside.')
            time.sleep(3)
            exit()
        static_url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-static/{provider}?date={date}&key={api_key}'
        realtime_url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/{provider}/VehiclePositions?date={date}&key={api_key}'
        prepare_file_on_server(static_url)
        download_data(f"tempdata/GTFS-{provider.upper()}-{date}.zip", static_url)
    elif import_method == "local":
        trips = pl.read_csv(staticdata_path+'/trips.txt')
        routes = pl.read_csv(staticdata_path+'/routes.txt', schema_overrides={'route_id': pl.Utf8})

    total_df = pl.DataFrame()
    MessageType = gtfs_realtime_pb2.FeedMessage()
    
    for time_range in time_ranges:
        timestamp = 3600*time_range[0][0] + 60*time_range[0][1] + time_range[0][2]
        max_timestamp = 3600*time_range[1][0] + 60*time_range[1][1] + time_range[1][2]
        while timestamp <= max_timestamp:
            if timestamp % modulo == 0:
                hour = math.floor(timestamp/3600)
                minute = math.floor((timestamp-hour*3600)/60)
                second = timestamp-hour*3600-minute*60
                if second == time_range[0][2]:
                    print("Now starting hour", hour, "minute", minute)
                # Add an extra 0 where necessary (for example 08:22)
                hour = str(hour).zfill(2)
                minute = str(minute).zfill(2)
                second = str(second).zfill(2)
                total_df = appendNewPBSecond(hour, minute, second, total_df, MessageType, trips)
            timestamp += 1

    if export_type == "csv":
        total_df.write_csv("output/entire_hour_"+provider+"_"+date+"_test.csv")
    return total_df

def prepare_file_on_server(url):
    time_waited = 0
    while True:
        print("Requesting data from", url)
        res_s = os.popen('curl -s -w %{http_code} -I "' + url + '"')
        res = res_s.read()
        if '200' in res: # Data ready to download
            print("Data is ready to download")
            return
        elif '202' in res: # Data will be prepared
            print("Data is being prepared on the server")
            print("Waiting. Time waited so far:", str(time_waited), "minutes")
            time.sleep(60)
            time_waited += 1
            continue
        else:
            raise Exception("Unknown response from server: " + res)

def download_data(path, url):
    print("downloading file with command: curl -s -o " + path + ' "' + url + '"')
    os.system("curl -s -o" + path + ' "' + url + '"')

provider = "sl" #REMOVE
date="2025-05-02" #REMOVE
key_text_file = "koda_api_key.txt"
try:
    api_key = open(key_text_file, "r").read()
except FileNotFoundError:
    print('KoDa API key not found. Please create a file named'+key_text_file+'in the same directory as this code, and paste your API key inside.')
    time.sleep(3)
    exit()
static_url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-static/{provider}?date={date}&key={api_key}'
realtime_url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/{provider}/VehiclePositions?date={date}&key={api_key}'
# First request to prepare the file. This will take time if the file is not ready yet
prepare_file_on_server(static_url)
# Then download the file
import_path = f"tempdata"
zip_name = f"GTFS-{provider.upper()}-{date}.zip"
download_data(os.path.join(import_path, zip_name), static_url)
# Unzip the static data
unzip_path = os.path.join(import_path, "static_unzipped")
os.makedirs(unzip_path, exist_ok=True)
with zipfile.ZipFile(os.path.join(import_path, zip_name), 'r') as zip_ref:
    zip_ref.extractall(unzip_path)

# Remove all data from tempdata folder
print("Operation completed. Data will now be removed from the tempdata folder.")
for item in os.listdir(import_path):
    item_path = os.path.join(import_path, item)
    # Skip .gitignore
    if item == ".gitignore": continue
    # Remove directories
    if os.path.isdir(item_path):
        shutil.rmtree(item_path)
    # Remove files
    else:
        os.remove(item_path)