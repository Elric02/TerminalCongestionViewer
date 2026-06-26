import polars as pl
import traj_dist.distance as tdist
import numpy as np
from datetime import datetime
from sklearn.cluster import DBSCAN
import geopandas as gpd
from shapely.geometry import Point, LineString
import pandas as pd
import os


def get_data(terminal, providers, date):
    df = pl.read_csv(f"output/vehiclepositions/vehiclepositions_terminal_{terminal}_{("_".join(providers))}_{date}.csv", schema_overrides={'trip_id': pl.Utf8, 'vehicle.id': pl.Utf8, 'route_id': pl.Utf8})
    return df

def format_data(df, min_points_in_traj, verbose):
    # Select all trip IDs (1 per trajectory) and put them in a list
    trip_ids_all = df.select(pl.col('trip_id')).unique().sort('trip_id').to_series().to_list()
    trip_ids_all = [x for x in trip_ids_all if x is not None]
    if verbose:
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
    return {"measure": measure, "name": name, "nb_pairs": len(data), "mean": np.mean(data), "std": np.std(data), "min": np.min(data), "max": np.max(data), "median": np.median(data)}


# Note: eps_percentile is the percentage in full numbers (e.g. 0.5 is 0.5%, NOT 50%)
def cluster_trips_dbscan(trip_coords_list, eps=None, eps_percentile=0.5, min_samples=2, verbose=False):
    if len(trip_coords_list) < 2:
        return np.array([], dtype=int), np.array([])

    sspd_dist = tdist.pdist(trip_coords_list, metric="sspd", verbose=verbose)
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
def export_to_gpkg(df_vehiclepositions, df_clusters, terminal, date, export_intermediate_to_csv, verbose, paths_gpkg):
    print("Now exporting to .gpkg file(s)...")
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
    if verbose:
        print(f"Found {len(categories)} cluster(s): {categories}")

    all_gdfs = {}
    for i, cat_value in enumerate(categories):
        subset = merged.filter(pl.col("route_dir_cluster") == cat_value)
        pandas_df = subset.to_pandas()
 
        # POINTS GEOPACKAGE
        output_points_gpkg = f"output/geopackage_{terminal}_{date}.gpkg"
        geometry = [
            Point(lon, lat)
            for lon, lat in zip(pandas_df["longitude"], pandas_df["latitude"])
        ]
        gdf = gpd.GeoDataFrame(pandas_df, geometry=geometry, crs="EPSG:4326")
        # Write mode: overwrite on first layer, append on the rest
        write_mode = "w" if i == 0 else "a"
        gdf.to_file(output_points_gpkg, layer=cat_value, driver="GPKG", mode=write_mode)
        if verbose:
            print(f"Layer '{cat_value}': written ({len(gdf)} rows)")

        # PATHS GEOPACKAGE
        if paths_gpkg:
            output_lines_gpkg = f"output/geopackage_{terminal}_{date}_paths.gpkg"
            trips_list = subset["trip_id"].unique().sort().to_list()
            trajectory_gdfs = []
            for j, trip in enumerate(trips_list):
                subset2 = subset.filter(pl.col("trip_id") == trip)
                pandas_df = subset2.to_pandas()
                coords = list(zip(pandas_df["longitude"], pandas_df["latitude"]))
                if len(coords) >= 2:
                    line = LineString(coords)
                    trajectory_gdfs.append(gpd.GeoDataFrame(
                        {"route_dir_cluster": [cat_value], "n_points": [len(coords)]},
                        geometry=[line],
                        crs="EPSG:4326"
                    ))
            combined_gdf = gpd.GeoDataFrame(pd.concat(trajectory_gdfs, ignore_index=True), crs="EPSG:4326")
            if cat_value not in all_gdfs:
                all_gdfs[cat_value] = combined_gdf
            else:
                all_gdfs[cat_value] = gpd.GeoDataFrame(pd.concat([all_gdfs[cat_value], combined_gdf], ignore_index=True), crs="EPSG:4326")
    
    if paths_gpkg:
        for i, (layer_name, gdf) in enumerate(all_gdfs.items()):
            gdf.to_file(
                output_lines_gpkg,
                layer=layer_name,
                driver="GPKG",
                mode="w" if i == 0 else "a"
            )

    print("GPKG export done! In QGIS: Layer -> Add Layer -> Add Vector Layer, then select the .gpkg file.")


def comparison(df, comparison_type, terminal, date, time_range, min_points_in_traj, min_trips_for_clustering,
                    discard_if_several_clusters, export_intermediate_to_csv, verbose,
                    dbscan_global_eps_percentile, dbscan_global_min_samples, paths_gpkg):
    results = []
    routedirs_count = [0, 0, 0, 0]

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

            _, trip_coords_list, _, _ = format_data(df_hour, min_points_in_traj, verbose)
            sspd = tdist.pdist(trip_coords_list, metric="sspd", verbose=verbose)
            dfd = tdist.pdist(trip_coords_list, metric="discret_frechet", verbose=verbose)
            results.append(quick_analysis(sspd, measure="sspd", name=f"{date} {hour:02d}h"))
            results.append(quick_analysis(dfd, measure="dfd", name=f"{date} {hour:02d}h"))

    if comparison_type == "routes":
        # Remove values with no route
        df = df.filter(pl.col("route_short_name") != -1)
        route_values = df.select(pl.col("route_short_name")).unique().to_series().to_list()
        df_clusters = pl.DataFrame(schema={"trip_id": pl.Utf8, "cluster": pl.Int64})
        for iter_id, route in enumerate(route_values):
            df_route = df.filter(pl.col("route_short_name") == route)
            direction_values = df_route.select(pl.col("direction_id")).unique().to_series().to_list()
            for direction in direction_values:
                df_direction = df_route.filter(pl.col("direction_id") == direction)
                if verbose:
                    print(f"route_short_name: {route}, direction_id: {direction}, nb of rows: {len(df_direction)}")
                trip_ids, trip_coords_list, _, _ = format_data(df_direction, min_points_in_traj, verbose)
                if len(trip_ids) >= min_trips_for_clustering:
                    labels, _, eps = cluster_trips_dbscan(trip_coords_list, eps_percentile=dbscan_global_eps_percentile, min_samples=dbscan_global_min_samples, verbose=verbose)
                    uniq, counts = np.unique(labels, return_counts=True)
                    nb_clusters = len(uniq[uniq!=-1]) if len(uniq)>0 else 0
                    if verbose:
                        print('p', dbscan_global_eps_percentile, 'eps', eps, 'clusters', nb_clusters, 'noise', counts[uniq==-1][0] if -1 in uniq else 0, 'labelcounts', list(zip(uniq, counts)))
                    if nb_clusters > 1 and discard_if_several_clusters:
                        routedirs_count[1] += 1
                        print(f"Discarding route {route}, direction {direction} since it had {nb_clusters} clusters and parameter discard_if_several_clusters is set to True.")
                        results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "nb_pairs": len(trip_ids), "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
                    else:
                        # DF of trip IDs linked to cluster labels, for this route+dir
                        df_clusters_dir = pl.DataFrame({"trip_id": trip_ids, "cluster": labels})
                        # Append it to the main DF of trip IDs linked to cluster labels
                        df_clusters = pl.concat([df_clusters, df_clusters_dir])
                        # Now, we work with combination route+direction+cluster
                        for cluster in range(nb_clusters):
                            df_clusters_cluster = df_clusters.filter(pl.col("cluster") == cluster)
                            if df_clusters_cluster.shape[0] <= 1:
                                routedirs_count[2] += 1
                                print(f"Discarding route {route}, direction {direction}, cluster {cluster} since there are only {df_clusters_cluster.shape[0]} trips in the main cluster.")
                                results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "nb_pairs": len(trip_ids), "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
                                continue
                            _, trip_coords_list_cluster, _, _ = format_data(df_direction.join(df_clusters_cluster, on="trip_id", how="left").filter(pl.col("cluster") == cluster), min_points_in_traj, verbose)
                            routedirs_count[0] += 1
                            print(f"(route {iter_id+1}/{len(route_values)}) Now calculating distances for route {route}, direction {direction}, cluster {cluster}...")
                            sspd = tdist.pdist(trip_coords_list_cluster, metric="sspd", verbose=verbose)
                            dfd = tdist.pdist(trip_coords_list_cluster, metric="discret_frechet", verbose=verbose)
                            results.append(quick_analysis(sspd, measure="sspd", name=f"{date} route_{route}_dir_{direction}_cluster_{cluster}"))
                            results.append(quick_analysis(dfd, measure="dfd", name=f"{date} route_{route}_dir_{direction}_cluster_{cluster}"))
                else:
                    routedirs_count[3] += 1
                    print(f"Not enough trajectories for route {route}, direction {direction} to calculate distance measures. Try changing the min_trips_for_clustering (current value: {min_trips_for_clustering}) parameter if you believe this is a mistake.")
                    results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "nb_pairs": len(trip_ids), "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
        if export_intermediate_to_csv:
            out_path = f'output/cluster_assignments_{terminal}_{date}.csv'
            df_clusters.write_csv(out_path)
            print(f"Wrote clusters CSV: {out_path} (rows={len(df_clusters)})")
        export_to_gpkg(df, df_clusters, terminal, date, export_intermediate_to_csv, verbose, paths_gpkg)
        print("Route+dirs... kept:", routedirs_count[0], ", discarded because of multiple clusters:", routedirs_count[1], ", discarded because of too few trajs in main cluster:", routedirs_count[2], ", discarded because too few trajectories in general:", routedirs_count[3])
        
        
    if comparison_type == "clustered":
        trip_ids, trip_coords_list, _, _ = format_data(df, min_points_in_traj, verbose)
        if len(trip_ids) < 2:
            print(f"Not enough trajectories to cluster and calculate distance measures.")
            results.append({"measure": "None", "name": f"{date} clustered", "nb_pairs": 0, "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
        else:
            labels, _, _ = cluster_trips_dbscan(trip_coords_list, verbose=verbose)
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
                sspd = tdist.pdist(cluster_trajs, metric="sspd", verbose=verbose)
                dfd = tdist.pdist(cluster_trajs, metric="discret_frechet", verbose=verbose)
                results.append(quick_analysis(sspd, measure="sspd", name=f"{date} cluster_{label}_size_{count}"))
                results.append(quick_analysis(dfd, measure="dfd", name=f"{date} cluster_{label}_size_{count}"))

    results_df = pl.DataFrame(results)
    return results_df, routedirs_count


def get_imprecision(terminal, date, providers, time_range=[5, 24], min_points_in_traj=10, min_trips_for_clustering=5,
                    discard_if_several_clusters=False, export_intermediate_to_csv=False, export_final_to_csv=True, verbose=False,
                    dbscan_global_eps_percentile=12, dbscan_global_min_samples=3, paths_gpkg=True):
    """Get the imprecision value(s) for the desired location and timeframe

    :param terminal: The desired terminal name
    :type terminal: str
    :param date: The desired date in format "YYYY-MM-DD"
    :type date: str
    :param providers: The list of desired provider codes (e.g. ["otraf", "sl"]). Attention: if there are several operators, they must be in the same order than in the filename! All codes here:
        https://www.trafiklab.se/api/gtfs-datasets/gtfs-regional#operators-covered-by-this-dataset
    :type providers: list[str]
    :param time_range: 2-element-list: (Only used in time comparison) start and beginning of desired time frame (e.g. `[5, 24]` -> only 1 timeframe, from 05:00:00 to 23:59:59 both included)
    :type time_range: list[int]
    :param min_points_in_traj: Minimum number of points in a trajectory for it to be considered in the calculations
    :type min_points_in_traj: int
    :param min_trips_for_clustering: Minimum number of trips/trajectories in the route+dir set for the clustering and analysis to happen
    :type min_trips_for_clustering: int
    :param discard_if_several_clusters: Whether the program should discard the route+dirs for which clustering has yield to more than 1 cluster (not counting outliers)
    :type discard_if_several_clusters: Boolean
    :param export_intermediate_to_csv: Whether to export the intermediate files (such as cluster_assignments and joined) to CSVs
    :type export_intermediate_to_csv: Boolean
    :param export_final_to_csv: Whether to export the final imprecision matrix to a CSV
    :type export_final_to_csv: Boolean
    :param verbose: Print (some) information about completed operations on the console. Some important stuff will be printed anyway
    :type verbose: Boolean
    :param dbscan_global_eps_percentile: Parameter for the DBSCAN for the process "split by route+dir and cluster". Note: eps_percentile is the percentage in full numbers (e.g. 0.5 is 0.5%, NOT 50%)
    :type dbscan_global_eps_percentile: float
    :param dbscan_global_min_samples: Parameter for the DBSCAN for the process "split by route+dir and cluster"
    :type dbscan_global_min_samples: int
    :param paths_gpkg: Whether you also want a 2nd GPKG file with paths instead of points
    :type paths_gpkg: Boolean
    :return: Dict containing the imprecision results
    """
    data = get_data(terminal, providers, date)
    # comparison_type is "hours", "routes" or "clustered"
    results_df, routedirs_count = comparison(data, comparison_type="routes", terminal=terminal, date=date, time_range=time_range, min_points_in_traj=min_points_in_traj,
                            min_trips_for_clustering=min_trips_for_clustering, discard_if_several_clusters=discard_if_several_clusters,
                            export_intermediate_to_csv=export_intermediate_to_csv, verbose=verbose, dbscan_global_eps_percentile=dbscan_global_eps_percentile,
                            dbscan_global_min_samples=dbscan_global_min_samples, paths_gpkg=paths_gpkg)
    if export_final_to_csv:
        directory = "output/uncertainty"
        os.makedirs(directory, exist_ok=True)
        results_df.write_csv(os.path.join(directory, f'uncertainty_comparison_{terminal}_{date}.csv'))

    # Get pooled mean and pooled standard deviation across rows
    sspd_results_df = results_df.filter(pl.col("measure") == "sspd")
    means = sspd_results_df["mean"].to_numpy()
    stds = sspd_results_df["std"].to_numpy()
    ns = sspd_results_df["nb_pairs"].to_numpy()
    pooled_mean = (ns * means).sum() / ns.sum()
    # Within-group sum of squares: Σ (nᵢ - 1) * sᵢ²
    within_ss = ((ns - 1) * stds**2).sum()
    # Between-group sum of squares: Σ nᵢ * (x̄ᵢ - x̄_pooled)²
    between_ss = (ns * (means - pooled_mean)**2).sum()
    pooled_std = np.sqrt((within_ss + between_ss) / (ns.sum() - 1))

    # Intermediate results for calculation of imprecision
    imprecision_trajs = {
        "kept": routedirs_count[0],
        "multiple_clusters": routedirs_count[1],
        "too_few_trajs_main_cluster": routedirs_count[2],
        "too_few_trajs_general": routedirs_count[3]
    }

    print("-- Imprecision calculations done! --")
    return pooled_mean, pooled_std, imprecision_trajs
