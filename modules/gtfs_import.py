import polars as pl
import numpy as np
import read_protobuf
import libs.gtfs_realtime_pb2 as gtfs_realtime_pb2 # Local library, no need to install this
import math
import os
import time
import shutil
import zipfile
import py7zr
from shapely.geometry import Point, Polygon
import stat
from datetime import datetime



def koda_import_timeframe(provider, date, time_ranges, import_method="online", realtimedata_path="tempdata/realtime", staticdata_path="tempdata/static/static_unzipped", modulo=1, terminal_coordinates=None, export_type="none", export_name=None, delete_tempdata=True): 
    """Import VehiclePositions data from the KoDa database for the specified timeframe in a specific day

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
    :param realtimedata_path: (Only if import_method=="local") The path to the folder containing real-time GTFS data (e.g. "../data/realtime")
    :type realtimedata_path: str
    :param staticdata_path: (Only if import_method=="local") The path to the folder containing static GTFS data, i.e. routes.txt, trips.txt... (e.g. "../data/static")
    :type staticdata_path: str
    :param modulo: Consider only 1 every x seconds, where x is this variable. Useful when using large timeframes
    :type modulo: int
    :param terminal_coordinates: List of tuples, each tuple containing 2 floats: longitude and latitude. Each tuple represents a point delimiting the zone of the terminal to use. If None, then the data is not restricted to any zone
    :type terminal_coordinates: list[tuple(float)] or None
    :param export_type: Which file type to export in (available: none, csv). The function always returns a DataFrame anyway
    :type export_type: str
    :param export_name: Name of the exported file. If None, uses a default one defined in this function
    :type export_name: str
    :param delete_tempdata: Whether to delete the downloaded GTFS data after processing
    :type delete_tempdata: Boolean
    :return: DataFrame containing the data from the specified timeframe
    """

    def appendNewPBSecond(hour, minute, second, total_df, MessageType, trips):
        try:
            if import_method == "online":
                vehiclepositions_path = os.path.join('tempdata', 'realtime', provider, 'VehiclePositions')
            elif import_method == "local":
                vehiclepositions_path = os.path.join(realtimedata_path, provider, 'VehiclePositions')
            filename = provider+'-vehiclepositions-'+date+'T'+hour+'-'+minute+'-'+second+'Z.pb'
            temp_df = read_protobuf.read_protobuf(vehiclepositions_path+'/'+date[0:4]+'/'+date[5:7]+'/'+date[8:]+'/'+hour+'/'+filename, MessageType)
            temp_df = pl.DataFrame(temp_df['entity'].tolist())
            temp_df = temp_df.with_columns(pl.lit(filename).alias("source"))
            # If coordinates of a terminal are provided, filter the data to only include data points from that zone
            if terminal_coordinates is not None:
                if len(terminal_coordinates) < 3:
                    print("Error while checking for the zone of the terminal: a polygon must have at least 3 points!")
                    exit
                coords = temp_df.select(["longitude", "latitude"]).to_numpy()
                boolean_mask = np.array([Polygon(terminal_coordinates).contains(Point(x, y)) for x, y in coords])
                temp_df = temp_df.filter(boolean_mask)

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
                pl.Series("route_id", routes_list, dtype=pl.Utf8, strict=False),
                pl.Series("direction_id", directions_list, dtype=pl.Int64),
                pl.Series("route_short_name", route_short_name_list, dtype=pl.Utf8, strict=False),
                pl.Series("route_type", route_type_list, dtype=pl.Int64),
            ])
            
            total_df = pl.concat([total_df, temp_df], how="diagonal")
        except FileNotFoundError:
            print("File not found:", filename)
        return total_df
    
    def prepare_file_on_server(url):
        time_waited = 0
        while True:
            print(datetime.now().strftime("%H:%M:%S"), "Requesting data from", url)
            res_s = os.popen('curl -s -w %{http_code} -I "' + url + '"')
            res = res_s.read()
            print(res)
            if '200' in res: # Data ready to download
                print(datetime.now().strftime("%H:%M:%S"), "Data is ready to download")
                return
            elif '202' in res: # Data will be prepared
                print(datetime.now().strftime("%H:%M:%S"), "Data is being prepared on the server")
                print("Waiting. Time waited so far:", str(time_waited), "minutes")
                time.sleep(60)
                time_waited += 1
                if time_waited%5 == 0: print(datetime.now().strftime("%H:%M:%S"), "Continuing to wait...")
                continue
            else:
                raise Exception("Unknown response from server: " + res)

    def download_data(path, url):
        print(datetime.now().strftime("%H:%M:%S"), "downloading file with command: curl -s -o " + path + ' "' + url + '"')
        os.system("curl -s -o" + path + ' "' + url + '"')
    
    if import_method == "online":
        # Find the API key
        key_text_file = "koda_api_key.txt"
        try:
            api_key = open(key_text_file, "r").read()
        except FileNotFoundError:
            print('KoDa API key not found. Please create a file named'+key_text_file+'in the same directory as this code, and paste your API key inside.')
            time.sleep(3)
            exit()
        static_url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-static/{provider}?date={date}&key={api_key}'
        # First request to prepare the file. This will take time if the file is not ready yet
        prepare_file_on_server(static_url)
        # Then download the file
        import_path = os.path.join("tempdata", "static")
        zip_name = f"GTFS-{provider.upper()}-{date}.zip"
        download_data(os.path.join(import_path, zip_name), static_url)
        # Unzip the static data
        unzip_path = os.path.join(import_path, "static_unzipped")
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(os.path.join(import_path, zip_name), 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
        # Import the now-extracted TXT files as DataFrames
        trips = pl.read_csv(os.path.join(unzip_path, "trips.txt"), schema_overrides={'route_id': pl.Utf8})
        routes = pl.read_csv(os.path.join(unzip_path, "routes.txt"), schema_overrides={'route_id': pl.Utf8, 'route_short_name': pl.Utf8})
        # Determine which hours will be needed to download from KoDa (real-time data)
        hours_to_download = []
        for time_range in time_ranges:
            hours = list(range(time_range[0][0], time_range[1][0] + 1))
            for hour in hours:
                if hour not in hours_to_download:
                    hours_to_download.append(hour)
        print("The following hours will be downloaded for realtime data:", hours_to_download)
        for hour_to_download in hours_to_download:
            realtime_url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/{provider}/VehiclePositions?date={date}&key={api_key}&hour={hour_to_download}'
            # First request to prepare the file. This will take time if the file is not ready yet
            prepare_file_on_server(realtime_url)
            # Then download the file
            import_path = os.path.join("tempdata", "realtime")
            zip_name = f"{provider}-VehiclePositions-{date}-{str(hour_to_download).zfill(2)}.zip"
            download_data(os.path.join(import_path, zip_name), realtime_url)
            # Unzip the static data
            with py7zr.SevenZipFile(os.path.join(import_path, zip_name), mode='r') as archive:
                archive.extractall(path=import_path)
    elif import_method == "local":
        trips = pl.read_csv(os.path.join(staticdata_path, 'trips.txt'), schema_overrides={'route_id': pl.Utf8})
        routes = pl.read_csv(os.path.join(staticdata_path, 'routes.txt'), schema_overrides={'route_id': pl.Utf8, 'route_short_name': pl.Utf8})

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
                    print(datetime.now().strftime("%H:%M:%S"), "Now starting hour", hour, "minute", minute)
                # Add an extra 0 where necessary (for example 08:22)
                hour = str(hour).zfill(2)
                minute = str(minute).zfill(2)
                second = str(second).zfill(2)
                total_df = appendNewPBSecond(hour, minute, second, total_df, MessageType, trips)
            timestamp += 1

    # Export to file if asked
    if export_type == "csv":
        if export_name is not None:
            file_name = export_name
        else:
            file_name = "vehiclepositions_"+provider+"_"+date+".csv"
        directory = "output/vehiclepositions"
        os.makedirs(directory, exist_ok=True)
        total_df.write_csv(os.path.join(directory, file_name))

    # Remove all data from tempdata folder
    print(datetime.now().strftime("%H:%M:%S"), "Operation completed.")
    if delete_tempdata:
        print(datetime.now().strftime("%H:%M:%S"), "All GTFS data will now be removed from the tempdata folder.")
        time.sleep(2)
        def remove_readonly(func, path, _):
            os.chmod(path, stat.S_IWRITE)
            func(path)
        for component in ["realtime", "static"]:
            import_path = os.path.join("tempdata", component)
            for item in os.listdir(import_path):
                item_path = os.path.join(import_path, item)
                # Skip .gitignore
                if item == ".gitignore": continue
                # Remove directories
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path, onexc=remove_readonly)
                # Remove files
                else:
                    os.remove(item_path)

    return total_df

