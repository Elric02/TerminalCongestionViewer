import pandas as pd
import koda_lib


provider = "otraf"
date = "2025-05-02"
time_ranges = [
    [[7, 0, 0], [7, 9, 59]]
]
terminal_coordinates = [(15.621, 58.416), (15.626, 58.416), (15.621, 58.419), (15.626, 58.419)]

#realtime_path = "../data/realtime/"+provider+"/VehiclePositions"
#static_path = "../data/static/"+provider+"/"+date
total_df = koda_lib.import_timeframe(provider, date, time_ranges, import_method="online", modulo=15, terminal_coordinates=terminal_coordinates, export_type="csv")
print(total_df)
