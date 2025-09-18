#TODO: test
#TODO: remove parameter routeid_dtype
#TODO: import automatically data
#TODO: add "export type" parameter

import pandas as pd
import numpy as np
import read_protobuf
import gtfs_realtime_pb2
import math


# Import VehiclePositions data from KoDa for the specified timeframe in a specific day
def import_timeframe(
    provider, #string: the desired provider code (e.g. otraf, sl, ul...), full list here:
                # https://www.trafiklab.se/api/gtfs-datasets/gtfs-regional#operators-covered-by-this-dataset
    date, #string: the desired date in format "YYYY-MM-DD"
    time_ranges=[[[7, 0, 0], [7, 59, 59]]], #list of 2-element-lists of 3-elements-list: start and beginning of desired time frames,
                                            # where sub-sub-lists take the shape [h, min, sec]
                                            # (e.g. [[[7, 0, 0], [7, 59, 59]]] -> only 1 timeframe, from 07:00:00 to 07:59:59 included)
    vehiclepositions_path="", #string: the path to the content of the VehiclePositions folder (e.g. "../data/realtime/VehiclePositions")
    staticdata_path="", #string: the path to the folder containing static GTFS data, i.e. routes.txt, trips.txt... (e.g. "../data/static")
    modulo=1, #int: consider only 1 every x seconds, where x is this variable. Useful when using large timeframes.
    routeid_dtype="float" #this parameter will be removed soon
): 

    def appendNewPBMinute(hour, minute, second, total_df, MessageType, trips):
        try:
            filename = provider+'-vehiclepositions-'+date+'T'+hour+'-'+minute+'-'+second+'Z.pb'
            temp_df = read_protobuf.read_protobuf(vehiclepositions_path+'/'+date[0:4]+'/'+date[5:7]+'/'+date[8:]+'/'+hour+'/'+filename, MessageType)
            temp_df = pd.DataFrame(temp_df['entity'].tolist())
            temp_df['source'] = filename

            routes_list = []
            directions_list = []
            route_short_name_list = []
            route_type_list = []
            for _, row in temp_df.iterrows():
                if not math.isnan(float(row['trip_id'])):
                    trip = trips.loc[trips['trip_id'] == int(row['trip_id'])]
                    if not trip.empty:
                        routes_list.append(trip['route_id'].iloc[0])
                        directions_list.append(trip['direction_id'].iloc[0])
                        if routeid_dtype == "float":
                            route_short_name_list.append(routes.loc[routes['route_id'] == float(trip['route_id'].iloc[0])]['route_short_name'].iloc[0])
                            route_type_list.append(routes.loc[routes['route_id'] == float(trip['route_id'].iloc[0])]['route_type'].iloc[0])
                        elif routeid_dtype == "str":
                            route_short_name_list.append(routes.loc[routes['route_id'] == str(trip['route_id'].iloc[0])]['route_short_name'].iloc[0])
                            route_type_list.append(routes.loc[routes['route_id'] == str(trip['route_id'].iloc[0])]['route_type'].iloc[0])
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
            temp_df['route_id'] = np.asarray(routes_list)
            temp_df['direction_id'] = np.asarray(directions_list)
            temp_df['route_short_name'] = np.asarray(route_short_name_list)
            temp_df['route_type'] = np.asarray(route_type_list)

            total_df = pd.concat([total_df, temp_df], ignore_index=True)
        except FileNotFoundError:
            print("File not found:", filename)
        return total_df
    
    trips = pd.read_csv(staticdata_path+'/trips.txt')
    routes = pd.read_csv(staticdata_path+'/routes.txt')

    total_df = pd.DataFrame()
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
                total_df = appendNewPBMinute(hour, minute, second, total_df, MessageType, trips)
            timestamp += 1

    print(total_df)
    total_df.to_csv("output/entire_hour_"+provider+"_"+date+"_test.csv")
    return total_df