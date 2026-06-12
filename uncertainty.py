#TODO: Idea: use multiple DBSCAN with different parameters, analyse the results?


import polars as pl
import traj_dist.distance as tdist
import numpy as np
from datetime import datetime
from sklearn.cluster import DBSCAN
import geopandas as gpd
from shapely.geometry import Point


terminal = "linköping"
date = "2022-03-22"
providers = ["otraf"] # Attention: if there are several operators, they must be in the same order than in the filename!
# Only used in hour_comparison()
time_range = [5, 24] # Second number not included (i.e. [6, 8] -> from 6:00:00 to 7:59:59)
min_points_in_traj = 10 # Minimum number of points in a trajectory for it to be considered in the calculations
min_trips_for_clustering = 5 # Minimum number of trips/trajectories in the route+dir set for the clustering and analysis to happen
discard_if_several_clusters = True # Whether the program should discard the route+dirs for which clustering has yield to more than 1 cluster (not counting outliers)
export_intermediate_to_csv = True # Whether to export the intermediate files (such as cluster_assignments and joined) to CSVs
# Parameters for the DBSCAN for the process "split by route+dir and cluster"
global_eps_percentile = 10 # Note: eps_percentile is the percentage in full numbers (e.g. 0.5 is 0.5%, NOT 50%)
global_min_samples = 4


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


# Merge VehiclePositions and Clusters dataframes and export to a GeoPackage (.gpkg) for QGIS
def export_to_gpkg(df_vehiclepositions, df_clusters):
    merged = df_vehiclepositions.join(df_clusters, how='left', on='trip_id')
    # Don't use datapoints which don't have any cluster
    merged = merged.filter(pl.col("cluster").is_not_null())

    # Build formatted column: "{route_short_name}_{direction_id}_{cluster}"
    merged = merged.with_columns([
        pl.col("route_short_name").fill_null("").cast(pl.Utf8),
        pl.col("direction_id").fill_null("").cast(pl.Utf8),
        pl.col("cluster").fill_null("").cast(pl.Utf8),
        pl.concat_str(
            [pl.col("route_short_name"), pl.col("direction_id"), pl.col("cluster")],
            separator="_"
        ).alias("route_dir_cluster"),
    ])
    if export_intermediate_to_csv:
        out_path = f"output/joined_{terminal}_{date}.csv"
        merged.write_csv(out_path)
        print(f"Wrote joined CSV: {out_path} (rows={len(merged)})")

    categories = merged['route_dir_cluster'].unique().sort().to_list()
    print(f"Found {len(categories)} cluster(s): {categories}")

    for i, cat_value in enumerate(categories):
        subset = merged.filter(pl.col("route_dir_cluster") == cat_value)
 
        # Build a GeoDataFrame
        pandas_df = subset.to_pandas()
        geometry = [
            Point(lon, lat)
            for lon, lat in zip(pandas_df["longitude"], pandas_df["latitude"])
        ]
        gdf = gpd.GeoDataFrame(pandas_df, geometry=geometry, crs="EPSG:4326")
 
        # Write mode: overwrite on first layer, append on the rest
        write_mode = "w" if i == 0 else "a"
        output_gpkg = f"output/geopackage_{terminal}_{date}.gpkg"
        gdf.to_file(output_gpkg, layer=cat_value, driver="GPKG", mode=write_mode)
        print(f"Layer '{cat_value}': written ({len(gdf)} rows)")
 
    print("In QGIS: Layer -> Add Layer -> Add Vector Layer, then select the .gpkg file.")


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
        df_clusters = pl.DataFrame(schema={"trip_id": pl.Utf8, "cluster": pl.Int64})
        for route in route_values:
            df_route = df.filter(pl.col("route_short_name") == route)
            direction_values = df_route.select(pl.col("direction_id")).unique().to_series().to_list()
            for direction in direction_values:
                df_direction = df_route.filter(pl.col("direction_id") == direction)
                print(f"route_short_name: {route}, direction_id: {direction}, nb of rows: {len(df_direction)}")
                trip_ids, trip_coords_list, _, _ = format_data(df_direction)
                if len(trip_ids) >= min_trips_for_clustering:
                    labels, _, eps = cluster_trips_dbscan(trip_coords_list, eps_percentile=global_eps_percentile, min_samples=global_min_samples)
                    uniq, counts = np.unique(labels, return_counts=True)
                    nb_clusters = len(uniq[uniq!=-1]) if len(uniq)>0 else 0
                    print('p', global_eps_percentile, 'eps', eps, 'clusters', nb_clusters, 'noise', counts[uniq==-1][0] if -1 in uniq else 0, 'labelcounts', list(zip(uniq, counts)))
                    if nb_clusters > 1 and discard_if_several_clusters:
                        print(f"Discarding route {route}, direction {direction} since it had {nb_clusters} clusters and parameter discard_if_several_clusters is set to True.")
                        results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "len": len(trip_ids), "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
                    else:
                        df_clusters_temp = pl.DataFrame({"trip_id": trip_ids, "cluster": labels})
                        df_clusters = pl.concat([df_clusters, df_clusters_temp])
                        sspd = tdist.pdist(trip_coords_list, metric="sspd", verbose=True)
                        dfd = tdist.pdist(trip_coords_list, metric="discret_frechet", verbose=True)
                        results.append(quick_analysis(sspd, measure="sspd", name=f"{date} route_{route}_dir_{direction}"))
                        results.append(quick_analysis(dfd, measure="dfd", name=f"{date} route_{route}_dir_{direction}"))
                else:
                    print(f"Not enough trajectories for route {route}, direction {direction} to calculate distance measures. Try changing the min_trips_for_clustering (current value: {min_trips_for_clustering}) parameter if you believe this is a mistake.")
                    results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "len": len(trip_ids), "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
        if export_intermediate_to_csv:
            out_path = f'output/cluster_assignments_{terminal}_{date}.csv'
            df_clusters.write_csv(out_path)
            print(f"Wrote clusters CSV: {out_path} (rows={len(df_clusters)})")
        export_to_gpkg(df, df_clusters)
        
        
    if comparison_type == "clustered":
        trip_ids, trip_coords_list, _, _ = format_data(df)
        if len(trip_ids) < 2:
            print(f"Not enough trajectories to cluster and calculate distance measures.")
            results.append({"measure": "None", "name": f"{date} clustered", "len": 0, "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
        else:
            labels, _, _ = cluster_trips_dbscan(trip_coords_list)
            # Export cluster assignments for each trip
            try:
                df_clusters = pl.DataFrame({"trip_id": trip_ids, "cluster": labels})
                df_clusters.write_csv(f'output/cluster_assignments_{terminal}_{date}.csv')
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