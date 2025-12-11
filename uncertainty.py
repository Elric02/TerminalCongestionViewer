import polars as pl
import traj_dist.distance as tdist
import numpy as np



df = pl.read_csv("output/vehiclepositions_terminal_linköping_2025-09-16.csv", schema_overrides={'trip_id': pl.Utf8, 'vehicle.id': pl.Utf8, 'route_id': pl.Utf8})

# Select only data points for the route we want to examine
df = df.filter(pl.col("route_short_name") == 1)
df = df.filter(pl.col("direction_id") == 1)
# Remove incomplete paths / outliers
df = df.filter(pl.col("trip_id") != "55700000076548069")

# Select all trip IDs (1 per trajectory) and put them in a list
trip_ids = df.select(pl.col('trip_id')).unique().to_series().to_list()
print("Trip IDs considered:", trip_ids)
# Format coordinates points to numpy arrays, 1 per traj, and put them in a list
trip_coords_list = []
for trip in trip_ids:
    temp_df = df.filter(pl.col("trip_id") == trip)
    trip_coords_list.append(temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy())

# Calculate distance measures for each pair of trajectory
#a=np.array([(116.750,40.632),(116.760,40.642),(116.770,40.652)])
#b=np.array([(116.751,40.632),(116.761,40.642),(116.771,40.652)])
sspd = tdist.pdist(trip_coords_list, metric="sspd")
print("-- SSPD --")
print(sspd)
edr = tdist.pdist(trip_coords_list, metric="edr", eps=0.0003)
print("-- EDR --")
print(edr)
dfd = tdist.pdist(trip_coords_list, metric="frechet")
print("-- DFD --")
print(dfd)
print([trip_ids[i] for i in range(len(trip_ids)) for _ in range(i+1, len(trip_ids))])
print([trip_ids[j] for i in range(len(trip_ids)) for j in range(i+1, len(trip_ids))])
output = pl.DataFrame(
    {
        "traj1": [trip_ids[i] for i in range(len(trip_ids)) for _ in range(i+1, len(trip_ids))],
        "traj2": [trip_ids[j] for i in range(len(trip_ids)) for j in range(i+1, len(trip_ids))],
        "sspd": sspd,
        "edr": edr,
        "dfd": dfd,
    }
)
output.write_csv('output/251202_pairwise_distances.csv')
