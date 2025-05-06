# For each row in for_verification:
# Take only both_confirmed 
# Save arrival and departure of the stop
# Find first point (before the stop arrival) in polygon-defined zone (which represents the terminal)
# Find first point (after the stop departure) outside of that zone
# Write trip time from entry to arrival, and trip time from departure to exit
# Discard both rows if there is a row with same entry or exit time
# Analysis: compute average amd std for each berth


import pandas as pd
from shapely.geometry import Point, Polygon
from datetime import datetime


stops_df = pd.read_csv('for_video_verification.csv', dtype={'vehicle': 'string'})
# Exclude non-confirmed stops
stops_df = stops_df[stops_df['status'] == "confirmed"].reset_index()

positions_df = pd.read_csv("entire_hour_cleaned.csv", dtype={'vehicle.id': 'string', 'trip_id': 'string', 'route_id': 'string'})
terminal_coordinates = [(15.62493, 58.41621), (15.62223, 58.41760), (15.62210, 58.41757), (15.62188, 58.41767), (15.62248, 58.41816), (15.62534, 58.41678)]
polygon = Polygon(terminal_coordinates)
# This will contain the last exit time registered for the vehicle. Thus, if a vehicle stopped twice before leaving the terminal, the second one will not be counted
start_at = {}
enter_timestamps = []
exit_timestamps = []
for _, stop in stops_df.iterrows():
    for_vehicle_df = positions_df[positions_df['vehicle.id'] == stop['vehicle']]
    enter_time = 0
    exit_time = 0
    discard_stop = False
    try:
        if start_at[stop['vehicle']]:
            first_timestamp = start_at[stop['vehicle']]
    except KeyError:
        first_timestamp = 0
    for_vehicle_df = for_vehicle_df[for_vehicle_df['timestamp'] > first_timestamp]
    for _, position in for_vehicle_df.iterrows():
        point_obj = Point((position['longitude'], position['latitude']))
        if polygon.contains(point_obj) and enter_time == 0:
            enter_time = position['timestamp']
            # If no suitable entry time was found, stop occurs probably within the same trip in the terminal. Then discard it.
            if enter_time > datetime.strptime(stop['timestart'], "%Y-%m-%d %H:%M:%S").timestamp():
                discard_stop = True
                break
        if not polygon.contains(point_obj) and enter_time != 0:
            exit_time = position['timestamp']
            start_at[position['vehicle.id']] = position['timestamp']
            break
    if discard_stop:
        enter_timestamps.append('')
        exit_timestamps.append('')
    else:
        enter_timestamps.append(enter_time)
        exit_timestamps.append(exit_time)
stops_df['enter_terminal'] = enter_timestamps
stops_df['exit_terminal'] = exit_timestamps

# Remove outdated index columns
stops_df.drop(stops_df.columns[0], axis=1, inplace=True)
stops_df.drop(stops_df.columns[0], axis=1, inplace=True)
# Discard vehicles that were already in the terminal at the beginning of the timeframe (with a 3 seconds margin) (if there aren't any, this will just discard the first vehicle)
minimum_timeframe = stops_df['enter_terminal'].min()
stops_df = stops_df[stops_df['enter_terminal'] > minimum_timeframe+3]
# Discard vehicles that were still in the terminal at the end of the timeframe
stops_df = stops_df[stops_df['exit_terminal'] > 0]

stops_df['time_to_berth'] = stops_df['timestart'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp()) - stops_df['enter_terminal']
stops_df['time_from_berth'] = stops_df['exit_terminal'] - stops_df['timestop'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp())

stops_df.to_csv('for_video_verification_enterexit.csv')