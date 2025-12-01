import polars as pl



df = pl.read_csv("output/vehiclepositions_terminal_linköping_2025-09-16.csv", schema_overrides={'trip_id': pl.Utf8, 'vehicle.id': pl.Utf8, 'route_id': pl.Utf8})

# Select only data points for the route we want to examine
df = df.filter(pl.col("route_short_name") == 1)
df = df.filter(pl.col("direction_id") == 1)
# Remove incomplete paths / outliers
df = df.filter(pl.col("trip_id") != "55700000076548069")
print(df.shape)



#Current status: cannot execute "pip install ." in the traj-dist folder, get an error
#TODO: implement SSPD, EDR, and DFD for each of the 6 (remaining) paths