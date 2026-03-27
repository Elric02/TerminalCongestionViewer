import polars as pl
import koda_lib


#provider = "otraf"
#terminal_coordinates = [(15.621, 58.416), (15.626, 58.416), (15.621, 58.419), (15.626, 58.419)]
date = "2025-09-20"
time_ranges = [
    [[7, 0, 0], [7, 59, 59]]
]
terminal_coordinates_df = pl.read_csv('terminal_coords.csv')
print(terminal_coordinates_df)
for terminal in terminal_coordinates_df.iter_rows():
    print("starting", terminal[terminal_coordinates_df.get_column_index('terminal')])
    coords = []
    for col in range(1,7):
        if terminal[terminal_coordinates_df.get_column_index('lon'+str(col))] is not None:
            coords.append((terminal[terminal_coordinates_df.get_column_index('lon'+str(col))], terminal[terminal_coordinates_df.get_column_index('lat'+str(col))]))
    print(coords)
    provider = terminal[terminal_coordinates_df.get_column_index('provider')]
    export_name = "vehiclepositions_terminal_"+terminal[terminal_coordinates_df.get_column_index('terminal')]+"_"+date+".csv"
    total_df = koda_lib.import_timeframe(provider, date, time_ranges, import_method="online", modulo=1, terminal_coordinates=coords, export_type="csv", export_name=export_name)

#realtime_path = "../data/realtime/"+provider+"/VehiclePositions"
#static_path = "../data/static/"+provider+"/"+date
#total_df = koda_lib.import_timeframe(provider, date, time_ranges, import_method="online", modulo=15, terminal_coordinates=terminal_coordinates, export_type="csv")
#print(total_df)
