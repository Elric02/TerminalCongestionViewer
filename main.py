#import pykoda
import pandas as pd
import numpy as np
import read_protobuf
import gtfs_realtime_pb2
import math
from pyproj import Geod
from datetime import datetime
from shapely.geometry import Point, Polygon


# SINGLE START: only take data from 07:16:00
def single_start(filename):
    
    MessageType = gtfs_realtime_pb2.FeedMessage()
    single_start_df = read_protobuf.read_protobuf('../data/vehiclepositions/07/'+filename, MessageType)
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


# ENTIRE HOUR: take data for a certain period
def entire_hour(time_ranges, trips):

    def appendNewPBMinute(hour, minute, second, total_df, MessageType, trips):
        try:
            filename = 'otraf-vehiclepositions-2022-03-22T'+hour+'-'+minute+'-'+second+'Z.pb'
            temp_df = read_protobuf.read_protobuf('../data/feed/'+hour+'/'+filename, MessageType)
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

            # Exclude non-bus data
            temp_df = temp_df[((temp_df['vehicle.id'].str.contains('9031005920')) | (temp_df['vehicle.id'].str.contains('9031005917')) | (temp_df['vehicle.id'].str.contains('9031005918')))]

            total_df = pd.concat([total_df, temp_df], ignore_index=True)
        except FileNotFoundError:
            print("File not found:", filename)
        return total_df

    total_df = pd.DataFrame()
    MessageType = gtfs_realtime_pb2.FeedMessage()
    
    for time_range in time_ranges:
        timestamp = 3600*time_range[0][0] + 60*time_range[0][1] + time_range[0][2]
        max_timestamp = 3600*time_range[1][0] + 60*time_range[1][1] + time_range[1][2]
        while timestamp <= max_timestamp:
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
        if i%1000 == 0:
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

# Using static data to compare computed and detected berths
def entire_hour_results(entire_hour_stopped_df, trips, routes, stops, stop_times, nb_consecutive):
    results = []
    results_details = []
    vehicles = entire_hour_stopped_df['vehicle.id'].unique()
    # Set stops where no berth was detected as berth "-1"
    entire_hour_stopped_df['assigned_berth'] = entire_hour_stopped_df['assigned_berth'].fillna(-1)
    for vehicle in vehicles:
        only_selected_vehicle = entire_hour_stopped_df.loc[entire_hour_stopped_df['vehicle.id'] == vehicle]
        assigned_berths = only_selected_vehicle['assigned_berth']
        berth_shifts = assigned_berths != assigned_berths.shift()
        counts = berth_shifts.cumsum().value_counts()
        # Get values of berths assigned at least nb_consecutive times consecutively
        result = only_selected_vehicle[berth_shifts.cumsum().isin(counts[counts >= nb_consecutive].index)]['assigned_berth'].unique()
        result_with_times = only_selected_vehicle[berth_shifts.cumsum().isin(counts[counts >= nb_consecutive].index)][['assigned_berth', 'timestamp', 'longitude', 'latitude', 'bearing']]
        # Get associated trips to this vehicle for this time period
        vehicle_trips = only_selected_vehicle['trip_id'].unique()
        
        vehicle_routes = []
        directions = []
        computed_berths = []
        for vehicle_trip in vehicle_trips:
            # Do not count not-in-service trips
            try:
                if np.isnan(float(vehicle_trip)):
                    continue
            except TypeError:
                continue
            associated_trip_gtfs = trips.loc[trips['trip_id'].apply(str) == str(vehicle_trip)]
            # Get associated routes short names
            route_id = associated_trip_gtfs['route_id'].iloc[0]
            route_short_name = routes.loc[routes['route_id'] == route_id]['route_short_name'].iloc[0]
            vehicle_routes.append(route_short_name)
            # Get associated directions 
            direction_id = associated_trip_gtfs['direction_id'].iloc[0]
            directions.append(direction_id)
            # Get associated berths at Linköpings Resecentrum
            stops_in_trip = stop_times.loc[stop_times['trip_id'].apply(str) == str(vehicle_trip)]['stop_id']
            lkpg_resecentrum = "90220050000500" # all stops at Linköping Centrum start with this sequence (and no other stop does)
            stops_in_trip_filtered = [stop for stop in stops_in_trip.tolist() if str(stop)[:len(lkpg_resecentrum)] == lkpg_resecentrum]
            # len(stops_in_trip_filtered) should always be 1, but maybe a trip could have two stops at the terminal
            for stop in stops_in_trip_filtered:
                computed_berth = stops.loc[stops['stop_id'] == stop]['platform_code'].iloc[0]
                computed_berths.append(computed_berth)
        computed_berths = pd.Series(computed_berths).unique().tolist()
        #print("trips id:", vehicle_trips, "routes id:", vehicle_routes, "directions:", directions, "computed berths:", computed_berths, "detected berths:", result)
        results.append({"vehicle": vehicle, "trips": vehicle_trips, "routes": vehicle_routes, "directions": directions, "computed": computed_berths, "detected": result})
        results_details.append({"vehicle": vehicle, "trips": vehicle_trips, "routes": vehicle_routes, "directions": directions, "computed": computed_berths, "detected_details": result_with_times})

    results_df = pd.DataFrame(results)
    results_details_df = pd.DataFrame(results_details)
    print(results_details_df)
    results_details_df.to_csv('entire_hour_results.csv')
    comparison = results_df.apply(lambda row: "same" if set(row['computed']) == set(row['detected']) else ("partial" if set(row['computed']) & set(row['detected']) else "different"), axis=1)
    print(comparison.value_counts())
    return results_df, results_details_df


# For now this is done manually 
#static_data = pykoda.datautils.load_static_data('otraf', '2022_03_22', remove_unused_stations=True)

trips = pd.read_csv('../data/static/trips.txt')
routes = pd.read_csv('../data/static/routes.txt')
stops = pd.read_csv('../data/static/stops.txt')
stop_times = pd.read_csv('../data/static/stop_times.txt')

#single_start_df = single_start('otraf-vehiclepositions-2022-03-22T07-16-00Z.pb')
# Input here desired time ranges to take into account. For example, [[7, 16, 0], [7, 32, 35]] is "from 07:16:00 to 07:32:35" (both included)
time_ranges = [
    #[[7, 16, 0], [7, 32, 35]],
    #[[7, 37, 25], [7, 53, 25]],
    #[[8, 11, 15], [8, 30, 00]],
    #[[8, 36, 35], [8, 38, 50]]
    [[7, 0, 0], [8, 0, 0]]
]

entire_hour_df = entire_hour(time_ranges, trips)
entire_hour_stopped_df = entire_hour_berths(entire_hour_df)
single_start_df = pd.read_csv("single_start.csv")
#entire_hour_df = pd.read_csv("entire_hour_cleaned.csv", dtype={'vehicle.id': 'string', 'trip_id': 'string', 'route_id': 'string'})
#entire_hour_df.drop(entire_hour_df.columns[0], axis=1, inplace=True)
#entire_hour_stopped_df = pd.read_csv("entire_hour_berths.csv", dtype={'vehicle.id': 'string', 'trip_id': 'string', 'route_id': 'string'})
#entire_hour_stopped_df.drop(entire_hour_stopped_df.columns[0], axis=1, inplace=True)

# Input here how many times a row a bus must be at speed=0 to be considered stopped. For example: 5
nb_consecutive = 5
results_df, results_details_df = entire_hour_results(entire_hour_stopped_df, trips, routes, stops, stop_times, nb_consecutive)

# Input here the berths to skip (for example out-of-frame berths)
excluded_berths = ['E1', 'E2', 'A1', 'B1', 'C1']
# Input here the id and time of the bus stops to skip (for example out-of-frame buses)
excluded_bus_stops = [
    ['9031005920505739', '2022-03-22 07:37:23'],
    ['9031005920505756', '2022-03-22 07:37:27'],
    ['9031005920505755', '2022-03-22 07:41:23'],
    ['9031005918308724', '2022-03-22 07:42:13'],
    ['9031005920505749', '2022-03-22 07:50:50'],
    ['9031005920505742', '2022-03-22 07:52:11'],
    ['9031005920505759', '2022-03-22 08:11:14'],
    ['9031005920505752', '2022-03-22 08:11:14'],
    ['9031005920505733', '2022-03-22 08:23:48']
]
# Input here the special zones. The zone has to be defined by at least 3 geographical points (its ends), and if a bus stopping inside should be considered as a regular stop or not.
special_zones = [
    {'name': "traffic_lights",
     'regular': True,
     'coordinates': [(15.62236, 58.41773), (15.62214, 58.41761), (15.62192, 58.41773), (15.62214, 58.41785)]}
]

# Function to check if a point is within some special zones of the terminal
def check_special_zones(longitude, latitude):
    is_inside = []
    for special_zone in special_zones:
        if len(special_zone['coordinates']) < 3:
            print("Error while checking for special zones: a polygon must have at least 3 points!")
            continue
        polygon = Polygon(special_zone['coordinates'])
        point_obj = Point((longitude, latitude))
        if polygon.contains(point_obj):
            is_inside.append(special_zone['regular'])
    if len(is_inside) > 1:
        print("More than one special zone detected! Check that they don't overlap. The first one will be kept.")
    if len(is_inside) > 0:
        return is_inside[0]
    else:
        return False

# Function that will add remarks to the stop based on some criteria
def check_special_stopping_conditions(longitude, latitude, bearing, timestart, entire_hour_stopped_df, crossings_df):
    remarks = {}
    # If there's any vehicle stopped in front of the stopped vehicle, and what that vehicle is (thought of) stopped for
    geod = Geod(ellps="WGS84")
    # Tip of triangle, 2 meters in the given bearing direction
    tip_lon, tip_lat, _ = geod.fwd(longitude, latitude, bearing, 2)
    # Base of triangle, 22 meters away from origin at ±20° from bearing
    left_bearing, right_bearing = ((bearing - 20) % 360, (bearing + 20) % 360)
    base_left_lon, base_left_lat, _ = geod.fwd(longitude, latitude, left_bearing, 22)
    base_right_lon, base_right_lat, _ = geod.fwd(longitude, latitude, right_bearing, 22)
    time_filtered_df = entire_hour_stopped_df[(entire_hour_stopped_df['timestamp'] >= timestart-5) & (entire_hour_stopped_df['timestamp'] <= timestart+5)]
    is_inside = []
    for _, vehicle in time_filtered_df.iterrows():
        if Polygon([(tip_lon, tip_lat), (base_left_lon, base_left_lat), (base_right_lon, base_right_lat)]).contains(Point((vehicle['longitude'], vehicle['latitude']))):
            # Only append a vehicle's id once
            if not any(x['vehicle'] == vehicle['vehicle.id'] for x in is_inside):
                is_inside.append({'vehicle': vehicle['vehicle.id']})
    if len(is_inside) > 0:
        remarks['vehicle_infront'] = is_inside
    # If the vehicle is stopped at a pedestrian crossing
    is_at_crossing = False
    for _, crossing in crossings_df.iterrows():
        geod = Geod(ellps="WGS84") # Define the geodetic model
        _, _, distance = geod.inv(longitude, latitude, crossing['longitude'], crossing['latitude']) # Compute geodesic distance
        if distance <= 6: # True if distance is equal or less than 6 meters
            remarks['at_crossing'] = crossing['name']
    return remarks

# Prepare details list for video verification
timemarks = []
for i, vehicle in results_details_df.iterrows():
    detected_details = vehicle['detected_details']
    # Exclude excluded berths
    detected_details = detected_details[~detected_details['assigned_berth'].isin(excluded_berths)].reset_index()
    first_seen_date = entire_hour_stopped_df.loc[entire_hour_stopped_df['vehicle.id'] == vehicle['vehicle']]['timestamp'].iloc[0]
    is_to_exclude = False
    for excluded_bus_stop in excluded_bus_stops:
        if vehicle['vehicle'] == excluded_bus_stop[0] and datetime.fromtimestamp(int(str(first_seen_date))) == datetime.strptime(excluded_bus_stop[1], "%Y-%m-%d %H:%M:%S"):
            is_to_exclude = True
    if is_to_exclude:
        continue
    # Compute if the vehicle is stopped at a particular place
    regular_stop = check_special_zones(detected_details.iloc[0]['longitude'], detected_details.iloc[0]['latitude'])
    # Add its stops to a list
    status = ("no_berth" if detected_details.iloc[0]['assigned_berth'] == "" else ("confirmed" if detected_details.iloc[0]['assigned_berth'] in set(vehicle['computed']) else ("regular_stop" if regular_stop else "unclear")))
    timemarks.append({
        "vehicle": vehicle['vehicle'],
        "timestart": detected_details.iloc[0]['timestamp'],
        "detected_berth": detected_details.iloc[0]['assigned_berth'],
        "computed_berths_for_vehicle": vehicle['computed'],
        "route": vehicle['routes'],
        "status": status,
        "longitude": detected_details.iloc[0]['longitude'],
        "latitude": detected_details.iloc[0]['latitude'],
        "bearing": detected_details.iloc[0]['bearing']
    })
    current_time = detected_details.iloc[0]['timestamp']
    for j, timeframe in detected_details.iterrows():
        # If the timestamp of this row is >8 seconds later than the previous row, the vehicle probably moved significantly and it should be considered as a different stop
        if timeframe['timestamp'] > current_time + 8:
            timemarks[-1]['timestop'] = detected_details.iloc[j-1]['timestamp']
            regular_stop = check_special_zones(timeframe['longitude'], timeframe['latitude'])
            status = ("no_berth" if timeframe['assigned_berth'] == "" else ("confirmed" if timeframe['assigned_berth'] in set(vehicle['computed']) else ("regular_stop" if regular_stop else "unclear")))
            timemarks.append({
                "vehicle": vehicle['vehicle'],
                "timestart": timeframe['timestamp'],
                "detected_berth": timeframe['assigned_berth'],
                "computed_berths_for_vehicle": vehicle['computed'],
                "route": vehicle['routes'],
                "status": status,
                "longitude": timeframe['longitude'],
                "latitude": timeframe['latitude'],
                "bearing": timeframe['bearing']
            })
        current_time = timeframe['timestamp']
    timemarks[-1]['timestop'] = detected_details.iloc[-1]['timestamp']
# Fill in remarks column
crossings_df = pd.read_csv('pedestrian_crossings.csv')
for timemark in timemarks:
    timemark['remarks'] = check_special_stopping_conditions(timemark['longitude'], timemark['latitude'], timemark['bearing'], timemark['timestart'], entire_hour_stopped_df, crossings_df)

# Sort list of stopped vehicles by time of stop
timemarks_df = pd.DataFrame(timemarks).sort_values('timestart').reset_index()
# Convert dates/hours from timestamps to datetimes
timemarks_df['timestart'] = timemarks_df['timestart'].apply(lambda x: datetime.fromtimestamp(int(str(x))))
timemarks_df['timestop'] = timemarks_df['timestop'].apply(lambda x: datetime.fromtimestamp(int(str(x))) if x != "" else "")
# Reorder columns (and don't export latitude/longitude)
timemarks_df = timemarks_df.loc[:, ['index', 'vehicle', 'timestart', 'timestop', 'detected_berth', 'computed_berths_for_vehicle', 'route', 'status', 'remarks']]
print(timemarks_df)
timemarks_df.to_csv('for_video_verification.csv')






# Tests with the TripUpdates feed
#MessageType = gtfs_realtime_pb2.FeedMessage()
#test_df = read_protobuf.read_protobuf('../data/tripupdates/07/otraf-tripupdates-2022-03-22T07-00-05Z.pb', MessageType)
#test_df = pd.DataFrame(test_df['entity'].tolist())
#print("-----------------------")
#print(test_df)
#print(pd.DataFrame(test_df['stop_time_update'].tolist()))
#pd.DataFrame(test_df['stop_time_update'].tolist()).to_csv("tripupdates_test.csv")
