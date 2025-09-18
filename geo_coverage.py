import pandas as pd
import koda_lib


provider = "sl"
date = "2025-05-02"
time_ranges = [
    [[7, 0, 0], [7, 9, 59]]
]
modulo = 15
routeid_dtype = "str"

trips = pd.read_csv('../data/static/'+provider+"/"+date+'/trips.txt')
routes = pd.read_csv('../data/static/'+provider+"/"+date+'/routes.txt')

realtime_path = "../data/realtime/"+provider+"/VehiclePositions"
static_path = "../data/static/"+provider+"/"+date
total_df = koda_lib.import_timeframe(provider, date, time_ranges, realtime_path, static_path, modulo, routeid_dtype)
print(total_df)