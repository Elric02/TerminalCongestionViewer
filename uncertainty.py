import polars as pl
import traj_dist.distance as tdist
import numpy as np
from datetime import datetime
from sklearn.cluster import DBSCAN


terminal = "linköping"
date = "2022-03-22"
providers = ["otraf"] # Attention: if there are several operators, they must be in the same order than in the filename!
# Only used in hour_comparison()
time_range = [5, 24] # second number not included (i.e. [6, 8] -> from 6:00:00 to 7:59:59)
min_points_in_traj = 10 # minimum number of points in a trajectory for it to be considered in the calculations


def get_data():
    df = pl.read_csv(f"output/vehiclepositions_terminal_{terminal}_{("_".join(providers))}_{date}.csv", schema_overrides={'trip_id': pl.Utf8, 'vehicle.id': pl.Utf8, 'route_id': pl.Utf8})
    return df

def format_data(df):
    # Select all trip IDs (1 per trajectory) and put them in a list
    trip_ids_all = df.select(pl.col('trip_id')).unique().sort('trip_id').to_series().to_list()
    trip_ids_all = [x for x in trip_ids_all if x is not None]
    print("Trip IDs considered:", trip_ids_all)

    trip_ids = []
    # Format coordinates points to numpy arrays, 1 per traj, and put them in a list
    trip_coords_list = []
    trip_coords_inv_list = []
    trip_coords_rand_list = []
    i = 0
    for trip in trip_ids_all:
        i += 1
        temp_df = df.filter(pl.col("trip_id") == trip)
        # Remove potential trajectories with less than X points, as they are not interesting for the distance measures and can cause errors
        if temp_df.shape[0] < min_points_in_traj:
            continue
        trip_ids.append(trip)
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


# Note: eps_percentile is the percentage in full numbers (e.g. 0.5 is 0.5%, NOT 50%)
def cluster_trips_dbscan(trip_coords_list, eps=None, eps_percentile=0.5, min_samples=2):
    if len(trip_coords_list) < 2:
        return np.array([], dtype=int), np.array([])

    sspd_dist = tdist.pdist(trip_coords_list, metric="sspd", verbose=True)
    n = len(trip_coords_list)
    dist_matrix = np.zeros((n, n), dtype=float)
    upper_idx = np.triu_indices(n, k=1)
    dist_matrix[upper_idx] = sspd_dist
    dist_matrix[(upper_idx[1], upper_idx[0])] = sspd_dist

    if eps is None:
        eps = max(np.percentile(sspd_dist, eps_percentile), 1e-7)

    labels = DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed").fit_predict(dist_matrix)
    return labels, sspd_dist, eps


def comparison(df, comparison_type="routes"):
    results = []

    if comparison_type == "hours":
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

    if comparison_type == "routes":
        # Remove values with no route
        df = df.filter(pl.col("route_short_name") != -1)
        route_values = df.select(pl.col("route_short_name")).unique().to_series().to_list()
        df_assign = pl.DataFrame(schema={"trip_id": pl.Utf8, "cluster": pl.Int64})
        for route in route_values:
            df_route = df.filter(pl.col("route_short_name") == route)
            direction_values = df_route.select(pl.col("direction_id")).unique().to_series().to_list()
            for direction in direction_values:
                df_direction = df_route.filter(pl.col("direction_id") == direction)
                print(f"route_short_name: {route}, direction_id: {direction}, nb of rows: {len(df_direction)}")
                trip_ids, trip_coords_list, _, _ = format_data(df_direction)
                if len(trip_ids) > 1:
                    labels, _, eps = cluster_trips_dbscan(trip_coords_list, eps_percentile=10, min_samples=4)
                    uniq, counts = np.unique(labels, return_counts=True)
                    print('p', 50, 'eps', eps, 'clusters', len(uniq[uniq!=-1]) if len(uniq)>0 else 0, 'noise', counts[uniq==-1][0] if -1 in uniq else 0, 'labelcounts', list(zip(uniq, counts)))
                    df_assign_temp = pl.DataFrame({"trip_id": trip_ids, "cluster": labels})
                    df_assign = pl.concat([df_assign, df_assign_temp])
                    
    #TODO
    # Filter out rows with len < 5 (5 being an arbitrarily chosen number of trajs) before the clustering takes place
    # Run the code and check out cluster_assignmentsxxx.csv
    # See if there's any trip+dir with more than 1 cluster. Think about how to handle this case in the automated analysis; probably discard the whole trip+dir in that case
    # Tweak parameters (mostly eps_percentile), look at every trip+dir to make sure that all outliers were identified as outliers, and not more
    # Automate/clean up the whole thing and prepare for the next steps

                    sspd = tdist.pdist(trip_coords_list, metric="sspd", verbose=True)
                    dfd = tdist.pdist(trip_coords_list, metric="discret_frechet", verbose=True)
                    results.append(quick_analysis(sspd, measure="sspd", name=f"{date} route_{route}_dir_{direction}"))
                    results.append(quick_analysis(dfd, measure="dfd", name=f"{date} route_{route}_dir_{direction}"))
                else:
                    print(f"Not enough trajectories for route {route} to calculate distance measures.")
                    results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "len": 0, "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
        df_assign.write_csv(f'output/cluster_assignments_{terminal}_{date}.csv')
        
    if comparison_type == "clustered":
        trip_ids, trip_coords_list, _, _ = format_data(df)
        if len(trip_ids) < 2:
            print(f"Not enough trajectories to cluster and calculate distance measures.")
            results.append({"measure": "None", "name": f"{date} clustered", "len": 0, "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
        else:
            labels, _, _ = cluster_trips_dbscan(trip_coords_list)
            # Export cluster assignments for each trip
            try:
                df_assign = pl.DataFrame({"trip_id": trip_ids, "cluster": labels})
                df_assign.write_csv(f'output/cluster_assignments_{terminal}_{date}.csv')
            except Exception as e:
                print(f"Failed to write cluster assignments CSV: {e}")
            unique_labels, counts = np.unique(labels, return_counts=True)
            for label, count in zip(unique_labels, counts):
                print(f"Cluster {label}: {count} trajectories")
                if label == -1:
                    print(f"Noise trajectories (label=-1): {count}")
                    continue
                if count < 2:
                    print(f"Cluster {label} has only {count} trajectory; skipping distance calculation.")
                    continue

                cluster_trajs = [trip_coords_list[i] for i in range(len(trip_coords_list)) if labels[i] == label]
                #print(cluster_trajs)
                sspd = tdist.pdist(cluster_trajs, metric="sspd", verbose=True)
                dfd = tdist.pdist(cluster_trajs, metric="discret_frechet", verbose=True)
                results.append(quick_analysis(sspd, measure="sspd", name=f"{date} cluster_{label}_size_{count}"))
                results.append(quick_analysis(dfd, measure="dfd", name=f"{date} cluster_{label}_size_{count}"))

    results_df = pl.DataFrame(results)
    return results_df


data = get_data()

results_df = comparison(data, comparison_type="routes") # comparison_type is "hours", "routes" or "clustered"
results_df.write_csv(f'output/uncertainty_comparison_{terminal}_{date}.csv')