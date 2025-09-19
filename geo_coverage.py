import pandas as pd
import koda_lib


provider = "ul"
date = "2025-05-02"
time_ranges = [
    [[7, 0, 0], [7, 9, 59]]
]

#realtime_path = "../data/realtime/"+provider+"/VehiclePositions"
#static_path = "../data/static/"+provider+"/"+date
total_df = koda_lib.import_timeframe(provider, date, time_ranges, import_method="online", modulo=15, export_type="csv")
print(total_df)
