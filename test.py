#import pykoda
import pandas as pd
import numpy as np
import read_protobuf
import gtfs_realtime_pb2
import math

# For now this is done manually 
#static_data = pykoda.datautils.load_static_data('otraf', '2022_03_22', remove_unused_stations=True)


# SINGLE START: only take data from 07:20:00

filename = 'otraf-vehiclepositions-2022-03-22T07-20-00Z.pb'
MessageType = gtfs_realtime_pb2.FeedMessage()
single_start_df = read_protobuf.read_protobuf('../data/feed/07/'+filename, MessageType)    # use file instead of bytes
single_start_df = pd.DataFrame(single_start_df['entity'].tolist())

trips = pd.read_csv('../data/static/trips.txt')
routes = pd.read_csv('../data/static/routes.txt')

routes_list = []
directions_list = []
route_short_name_list = []
for index, row in single_start_df.iterrows():
    if not math.isnan(float(row['trip_id'])):
        trip = trips.loc[trips['trip_id'] == int(row['trip_id'])]
        if not trip.empty:
            route_id = trip['route_id']
            direction_id = trip['direction_id']
            route_short_name = routes.loc[routes['route_id'] == float(route_id.iloc[0])]['route_short_name']
            routes_list.append(route_id.iloc[0])
            directions_list.append(direction_id.iloc[0])
            route_short_name_list.append(route_short_name.iloc[0])
        else:
            routes_list.append(np.nan)
            directions_list.append(np.nan)
            route_short_name_list.append(np.nan)
    else:
        routes_list.append(np.nan)
        directions_list.append(np.nan)
        route_short_name_list.append(np.nan)

single_start_df['route_id'] = np.asarray(routes_list)
single_start_df['direction_id'] = np.asarray(directions_list)
single_start_df['route_short_name'] = np.asarray(route_short_name_list)

#print(single_start_df)
single_start_df.to_csv("single_start.csv")


# ENTIRE HOUR: take all data from 07:20:00 to 08:19:59 included

total_df = single_start_df
total_df['source'] = filename
MessageType = gtfs_realtime_pb2.FeedMessage()

def appendNewPBMinute(hour, minute, total_df):
    print("Now starting hour", hour, "minute", minute)
    for second in range(0, 60):
        second = str(second).zfill(2)
        try:
            filename = 'otraf-vehiclepositions-2022-03-22T'+hour+'-'+minute+'-'+second+'Z.pb'
            temp_df = read_protobuf.read_protobuf('../data/feed/'+hour+'/'+filename, MessageType)    # use file instead of bytes
            temp_df = pd.DataFrame(temp_df['entity'].tolist())
            temp_df['source'] = filename
            total_df = pd.concat([total_df, temp_df], ignore_index=True)
        except FileNotFoundError:
            print("File not found:", filename)
    return total_df

hour = "07"
for minute in range(20, 60):
    minute = str(minute).zfill(2)
    total_df = appendNewPBMinute(hour, minute, total_df)
    
hour = "08"
for minute in range(0, 20):
    minute = str(minute).zfill(2)
    total_df = appendNewPBMinute(hour, minute, total_df)

print(total_df)
total_df.to_csv("entire_hour.csv")