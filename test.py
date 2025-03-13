#import pykoda
import pandas as pd
import numpy as np
import read_protobuf
import gtfs_realtime_pb2
import math
from pyproj import Geod


# SINGLE START: only take data from 07:20:00
def single_start():
    
    MessageType = gtfs_realtime_pb2.FeedMessage()
    single_start_df = read_protobuf.read_protobuf('../data/vehiclepositions/07/'+filename, MessageType)    # use file instead of bytes
    single_start_df = pd.DataFrame(single_start_df['entity'].tolist())

    routes_list = []
    directions_list = []
    route_short_name_list = []
    route_type_list = []
    for _, row in single_start_df.iterrows():
        if not math.isnan(float(row['trip_id'])):
            trip = trips.loc[trips['trip_id'] == int(row['trip_id'])]
            if not trip.empty:
                route_id = trip['route_id']
                direction_id = trip['direction_id']
                route_short_name = routes.loc[routes['route_id'] == float(route_id.iloc[0])]['route_short_name']
                route_type = routes.loc[routes['route_id'] == float(route_id.iloc[0])]['route_type']
                routes_list.append(route_id.iloc[0])
                directions_list.append(direction_id.iloc[0])
                route_short_name_list.append(route_short_name.iloc[0])
                route_type_list.append(route_type.iloc[0])
            else:
                routes_list.append(np.nan)
                directions_list.append(np.nan)
                route_short_name_list.append(np.nan)
                route_type_list.append(np.nan)
        else:
            routes_list.append(np.nan)
            directions_list.append(np.nan)
            route_short_name_list.append(np.nan)
            route_type_list.append(np.nan)
    single_start_df['route_id'] = np.asarray(routes_list)
    single_start_df['direction_id'] = np.asarray(directions_list)
    single_start_df['route_short_name'] = np.asarray(route_short_name_list)
    single_start_df['route_type'] = np.asarray(route_type_list)

    print(single_start_df)
    single_start_df.to_csv("single_start.csv")
    return single_start_df


# ENTIRE HOUR: take all data from 07:20:00 to 08:19:59 included
def entire_hour(single_start_df):

    def appendNewPBMinute(hour, minute, total_df, MessageType):
        print("Now starting hour", hour, "minute", minute)
        for second in range(0, 60):
            second = str(second).zfill(2)
            try:
                filename = 'otraf-vehiclepositions-2022-03-22T'+hour+'-'+minute+'-'+second+'Z.pb'
                temp_df = read_protobuf.read_protobuf('../data/vehiclepositions/'+hour+'/'+filename, MessageType)    # use file instead of bytes
                temp_df = pd.DataFrame(temp_df['entity'].tolist())
                temp_df['source'] = filename

                # Exclude data outside the bus terminal
                temp_df = temp_df[((temp_df['latitude'] > 58.416) & (temp_df['latitude'] < 58.419) & (temp_df['longitude'] > 15.621) & (temp_df['longitude'] < 15.626))]
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
                            route_short_name_list.append(routes.loc[routes['route_id'] == float(trip['route_id'].iloc[0])]['route_short_name'].iloc[0])
                            route_type_list.append(routes.loc[routes['route_id'] == float(trip['route_id'].iloc[0])]['route_type'].iloc[0])
                        else:
                            routes_list.append(np.nan)
                            directions_list.append(np.nan)
                            route_short_name_list.append(np.nan)
                            route_type_list.append(np.nan)
                    else:
                        routes_list.append(np.nan)
                        directions_list.append(np.nan)
                        route_short_name_list.append(np.nan)
                        route_type_list.append(np.nan)
                temp_df['route_id'] = np.asarray(routes_list)
                temp_df['direction_id'] = np.asarray(directions_list)
                temp_df['route_short_name'] = np.asarray(route_short_name_list)
                temp_df['route_type'] = np.asarray(route_type_list)

                # Exclude non-bus data
                temp_df = temp_df[(temp_df['route_type'] == 700)]

                total_df = pd.concat([total_df, temp_df], ignore_index=True)
            except FileNotFoundError:
                print("File not found:", filename)
        return total_df

    total_df = single_start_df
    total_df['source'] = filename
    MessageType = gtfs_realtime_pb2.FeedMessage()

    hour = "07"
    for minute in range(20, 60):
        minute = str(minute).zfill(2)
        total_df = appendNewPBMinute(hour, minute, total_df, MessageType)
        
    hour = "08"
    for minute in range(0, 20):
        minute = str(minute).zfill(2)
        total_df = appendNewPBMinute(hour, minute, total_df, MessageType)

    print(total_df)
    total_df.to_csv("entire_hour.csv")
    return total_df

# Adding detected berths for all vehicles
def entire_hour_berths(entire_hour_df):
    berths_df = pd.read_csv("berths.csv")
    # Assign a berth number to each stopped bus in range of a berth
    entire_hour_stopped_df = entire_hour_df[(entire_hour_df['speed'] == 0)].copy()
    assigned_berths = []
    print(entire_hour_stopped_df)
    for i, row in entire_hour_stopped_df.iterrows():
        if i%500 == 0:
            print("Value", i, "/", len(entire_hour_df))
        guessed_berth = None
        for _, berth in berths_df.iterrows():
            geod = Geod(ellps="WGS84") # Define the geodetic model
            _, _, distance = geod.inv(row['longitude'], row['latitude'], berth['longitude'], berth['latitude']) # Compute geodesic distance
            if distance <= 6: # True if distance is equal or less than 6 meters
                guessed_berth = berth['berth']
        assigned_berths.append(guessed_berth)
    entire_hour_stopped_df['assigned_berth'] = assigned_berths
    entire_hour_stopped_df.to_csv("entire_hour_berths.csv")
    return entire_hour_stopped_df


# For now this is done manually 
#static_data = pykoda.datautils.load_static_data('otraf', '2022_03_22', remove_unused_stations=True)

filename = 'otraf-vehiclepositions-2022-03-22T07-20-00Z.pb'
trips = pd.read_csv('../data/static/trips.txt')
routes = pd.read_csv('../data/static/routes.txt')
stops = pd.read_csv('../data/static/stops.txt')
stop_times = pd.read_csv('../data/static/stop_times.txt')

#single_start_df = single_start()
#entire_hour_df = entire_hour(single_start_df.drop(single_start_df.index))
#entire_hour_stopped_df = entire_hour_berths(entire_hour_df)
single_start_df = pd.read_csv("single_start.csv")
entire_hour_df = pd.read_csv("entire_hour.csv")
entire_hour_stopped_df = pd.read_csv("entire_hour_berths.csv")


# Using static data to compare computed and detected berths

results = []
vehicles = entire_hour_stopped_df['vehicle.id'].unique()
for vehicle in vehicles:
    only_selected_vehicle = entire_hour_stopped_df.loc[entire_hour_stopped_df['vehicle.id'] == vehicle]
    assigned_berths = only_selected_vehicle['assigned_berth']
    berth_shifts = assigned_berths != assigned_berths.shift()
    counts = berth_shifts.cumsum().value_counts()
    # Get values of berths assigned at least 5x consecutively
    result = only_selected_vehicle[berth_shifts.cumsum().isin(counts[counts >= 5].index)]['assigned_berth'].unique()
    # Get associated trips to this vehicle for this time period
    vehicle_trips = only_selected_vehicle['trip_id'].unique()
    
    vehicle_routes = []
    directions = []
    computed_berths = []
    for vehicle_trip in vehicle_trips:
        associated_trip_gtfs = trips.loc[trips['trip_id'] == vehicle_trip]
        # Get associated routes short names
        route_id = associated_trip_gtfs['route_id'].iloc[0]
        route_short_name = routes.loc[routes['route_id'] == route_id]['route_short_name'].iloc[0]
        vehicle_routes.append(route_short_name)
        # Get associated directions 
        direction_id = associated_trip_gtfs['direction_id'].iloc[0]
        directions.append(direction_id)
        # Get associated berths at Linköpings Resecentrum
        stops_in_trip = stop_times.loc[stop_times['trip_id'] == vehicle_trip]['stop_id']
        lkpg_resecentrum = "90220050000500" # all stops at Linköping Centrum start with this sequence (and no other stop does)
        stops_in_trip_filtered = [stop for stop in stops_in_trip.tolist() if str(stop)[:len(lkpg_resecentrum)] == lkpg_resecentrum]
        # len(stops_in_trip_filtered) should always be 1, but maybe a trip could have two stops at the terminal
        for stop in stops_in_trip_filtered:
            computed_berth = stops.loc[stops['stop_id'] == stop]['platform_code'].iloc[0]
            computed_berths.append(computed_berth)
    computed_berths = pd.Series(computed_berths).unique().tolist()
    #print("trips id:", vehicle_trips, "routes id:", vehicle_routes, "directions:", directions, "computed berths:", computed_berths, "detected berths:", result)
    results.append({"vehicle": vehicle, "trips": vehicle_trips, "routes": vehicle_routes, "directions": directions, "computed": computed_berths, "detected": result})

results_df = pd.DataFrame(results)
print(results_df)
exact_same = np.where((results_df['computed'].apply(set) & results_df['detected'].apply(set)), 1, 0)
comparison = results_df.apply(lambda row: "same" if set(row['computed']) == set(row['detected']) else ("partial" if set(row['computed']) & set(row['detected']) else "different"), axis=1)
print(comparison.value_counts())


# Tests with the TripUpdates feed
MessageType = gtfs_realtime_pb2.FeedMessage()
test_df = read_protobuf.read_protobuf('../data/tripupdates/07/otraf-tripupdates-2022-03-22T07-00-05Z.pb', MessageType)
test_df = pd.DataFrame(test_df['entity'].tolist())
#print("-----------------------")
#print(test_df)
#print(pd.DataFrame(test_df['stop_time_update'].tolist()))
#pd.DataFrame(test_df['stop_time_update'].tolist()).to_csv("tripupdates_test.csv")