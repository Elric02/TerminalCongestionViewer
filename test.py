#import pykoda
import pandas as pd
import numpy as np
import read_protobuf
import gtfs_realtime_pb2
import math

# For now this is done manually 
#static_data = pykoda.datautils.load_static_data('otraf', '2022_03_22', remove_unused_stations=True)

MessageType = gtfs_realtime_pb2.FeedMessage()
df = read_protobuf.read_protobuf('../data/feed/07/otraf-vehiclepositions-2022-03-22T07-20-00Z.pb', MessageType)    # use file instead of bytes
df = pd.DataFrame(df['entity'].tolist())

trips = pd.read_csv('../data/static/trips.txt')
routes = pd.read_csv('../data/static/routes.txt')

routes_list = []
directions_list = []
route_short_name_list = []
for index, row in df.iterrows():
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

print(len(routes_list))
df['route_id'] = np.asarray(routes_list)
df['direction_id'] = np.asarray(directions_list)
df['route_short_name'] = np.asarray(route_short_name_list)

print(df)
df.to_csv("test.csv")