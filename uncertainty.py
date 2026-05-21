import polars as pl
import traj_dist.distance as tdist
import numpy as np
from datetime import datetime


terminal = "linköping"
date = "2026-03-12"
providers = ["otraf"]
# Only used in hour_comparison()
time_range = [5, 24] # second number not included


def get_data():
    temp_df_list = []
    for provider in providers:
        temp_df_list.append(pl.read_csv(f"output/vehiclepositions_terminal_{terminal}_{provider}_{date}.csv", schema_overrides={'trip_id': pl.Utf8, 'vehicle.id': pl.Utf8, 'route_id': pl.Utf8}))
    df = pl.concat(temp_df_list, how="diagonal_relaxed")

    '''
    # Select only data points for the route we want to examine
    df = df.filter(pl.col("route_short_name") == 1)
    df = df.filter(pl.col("direction_id") == 1)
    # Remove incomplete paths / outliers
    df = df.filter(pl.col("trip_id") != "55700000076548069")
    '''
    return df

def format_data(df):
    # Select all trip IDs (1 per trajectory) and put them in a list
    trip_ids = df.select(pl.col('trip_id')).unique().sort('trip_id').to_series().to_list()
    trip_ids = [x for x in trip_ids if x is not None]
    print("Trip IDs considered:", trip_ids)

    # Format coordinates points to numpy arrays, 1 per traj, and put them in a list
    trip_coords_list = []
    trip_coords_inv_list = []
    trip_coords_rand_list = []
    i = 0
    for trip in trip_ids:
        i += 1
        temp_df = df.filter(pl.col("trip_id") == trip)
        # Trajectory with points in their normal order
        trip_coords_list.append(temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy())
        # Trajectory with points in the reversed order (i.e. the first point becomes the last)
        trip_coords_inv_list.append(temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy()[::-1])
        # Trajectory with points in a random order
        trip_coords_rand = temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy()
        np.random.shuffle(trip_coords_rand)
        trip_coords_rand_list.append(trip_coords_rand)
    return trip_ids, trip_coords_list, trip_coords_inv_list, trip_coords_rand_list


def quick_analysis(data, measure="", name=""):
    return {"measure": measure, "name": name, "len": len(data), "mean": np.mean(data), "std": np.std(data), "min": np.min(data), "max": np.max(data), "median": np.median(data)}


# Just bits of code I used to try out the different measures and see the differences
def try_measures(trip_ids, trip_coords_list, trip_coords_inv_list, trip_coords_rand_list):
    #print(trip_coords_list[0])
    #print(trip_coords_inv_list[0])
    #print(trip_coords_rand_list[0])
    print("Length of the trajectories:", [len(traj) for traj in trip_coords_list])
    # Calculate distance measures for each pair of trajectory
    #a=np.array([(116.750,40.632),(116.760,40.642),(116.770,40.652)])
    #b=np.array([(116.751,40.632),(116.761,40.642),(116.771,40.652)])
    sspd = tdist.pdist(trip_coords_list, metric="sspd")
    sspd_inv = tdist.pdist(trip_coords_inv_list, metric="sspd")
    sspd_rand = tdist.pdist(trip_coords_rand_list, metric="sspd")
    print("-- SSPD --")
    #print(sspd)
    print(quick_analysis(sspd))
    print("-- SSPD (inverted) --")
    #print(sspd_inv)
    print(quick_analysis(sspd_inv))
    print("-- SSPD (randomized) --")
    #print(sspd_rand)
    print(quick_analysis(sspd_rand))
    edr = tdist.pdist(trip_coords_list, metric="edr", eps=0.0003)
    edr_inv = tdist.pdist(trip_coords_inv_list, metric="edr", eps=0.0003)
    edr_rand = tdist.pdist(trip_coords_rand_list, metric="edr", eps=0.0003)
    print("-- EDR --")
    #print(edr)
    print(quick_analysis(edr))
    print("-- EDR (inverted) --")
    #print(edr_inv)
    print(quick_analysis(edr_inv))
    print("-- EDR (randomized) --")
    #print(edr_rand)
    print(quick_analysis(edr_rand))
    dfd = tdist.pdist(trip_coords_list, metric="discret_frechet")
    dfd_inv = tdist.pdist(trip_coords_inv_list, metric="discret_frechet")
    dfd_rand = tdist.pdist(trip_coords_rand_list, metric="discret_frechet")
    print("-- DFD --")
    #print(dfd)
    print(quick_analysis(dfd))
    print("-- DFD (inverted) --")
    #print(dfd_inv)
    print(quick_analysis(dfd_inv))
    print("-- DFD (randomized) --")
    #print(dfd_rand)
    print(quick_analysis(dfd_rand))
    output = pl.DataFrame(
        {
            "traj1": [trip_ids[i] for i in range(len(trip_ids)) for _ in range(i+1, len(trip_ids))],
            "traj2": [trip_ids[j] for i in range(len(trip_ids)) for j in range(i+1, len(trip_ids))],
            "sspd": sspd,
            "edr": edr,
            "dfd": dfd,
        }
    )
    output.write_csv(f'output/pairwise_distances_{terminal}_{date}.csv')


def hour_comparison(df):
    results = []
    for hour in range(time_range[0], time_range[1]):
        start = datetime.timestamp(datetime.strptime(f"{date} {hour:02d}:00:00", "%Y-%m-%d %H:%M:%S"))
        end = datetime.timestamp(datetime.strptime(f"{date} {hour:02d}:59:59", "%Y-%m-%d %H:%M:%S"))
        df_hour = df.filter(
            pl.col("timestamp")
            .is_between(
                start,
                end,
                closed="both",
            )
        )
        print(f"{hour:02d}:00–{hour+1:02d}:00 => {len(df_hour)} rows")
        _, trip_coords_list, _, _ = format_data(df_hour)
        sspd = tdist.pdist(trip_coords_list, metric="sspd", verbose=True)
        dfd = tdist.pdist(trip_coords_list, metric="discret_frechet", verbose=True)
        results.append(quick_analysis(sspd, measure="sspd", name=f"{date} {hour:02d}h"))
        results.append(quick_analysis(dfd, measure="dfd", name=f"{date} {hour:02d}h"))
    results_df = pl.DataFrame(results)
    return results_df


data = get_data()

#trip_ids, data_list, inv_list, rand_list = format_data(data)
#try_measures(trip_ids, data_list, inv_list, rand_list)

results_df = hour_comparison(data)
results_df.write_csv(f'output/hourly_comparison_{terminal}_{date}.csv')