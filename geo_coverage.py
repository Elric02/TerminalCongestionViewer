import pandas as pd
import numpy as np
import read_protobuf
import gtfs_realtime_pb2
import main
import os

time_ranges = [
    [[7, 0, 0], [7, 59, 59]]
]

provider = "sl"
date = "2024-05-02"
modulo = 1
routeid_dtype = "str"

trips = pd.read_csv('../data/static/'+provider+"/"+date+'/trips.txt')
routes = pd.read_csv('../data/static/'+provider+"/"+date+'/routes.txt')

realtime_path = "../data/realtime/"+provider+"/VehiclePositions/"+date[0:4]+"/"+date[5:7]+"/"+date[8:]
total_df = main.entire_hour(provider, date, realtime_path, time_ranges, trips, routes, False, modulo, routeid_dtype)
print(total_df)