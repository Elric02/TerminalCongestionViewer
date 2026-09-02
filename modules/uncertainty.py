import math

import polars as pl
from pyproj import Geod
import traj_dist.distance as tdist
import numpy as np
from datetime import datetime
from sklearn.cluster import DBSCAN
import geopandas as gpd
from shapely.geometry import Point, LineString
import pandas as pd
import os


def get_data(terminal, providers, date, vehiclepositions_path=None):
    if vehiclepositions_path is None:
        vehiclepositions_path = f"output/vehiclepositions/vehiclepositions_terminal_{terminal}_{("_".join(providers))}_{date}.csv"
    df = pl.read_csv(vehiclepositions_path, schema_overrides={'trip_id': pl.Utf8, 'vehicle.id': pl.Utf8, 'route_id': pl.Utf8, 'route_short_name': pl.Utf8})
    return df

def format_data(df, min_points_in_traj, verbose, group_column='trip_id'):
    # Select all groups (1 per trajectory) and put them in a list
    group_ids_all = df.select(pl.col(group_column)).unique().sort(group_column).to_series().to_list()
    group_ids_all = [group_id for group_id in group_ids_all if group_id is not None]
    if verbose:
        print("Groups considered:", group_ids_all)

    group_ids = []
    # Format coordinates points to numpy arrays, 1 per traj, and put them in a list
    trip_coords_list = []
    trip_coords_inv_list = []
    trip_coords_rand_list = []
    i = 0
    for group_id in group_ids_all:
        i += 1
        temp_df = df.filter(pl.col(group_column) == group_id)
        # Remove potential trajectories with less than X points, as they are not interesting for the distance measures and can cause errors
        if temp_df.shape[0] < min_points_in_traj:
            continue
        group_ids.append(group_id)
        # Trajectory with points in their normal order
        trip_coords_list.append(temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy())
        # Trajectory with points in the reversed order (i.e. the first point becomes the last)
        trip_coords_inv_list.append(temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy()[::-1])
        # Trajectory with points in a random order
        trip_coords_rand = temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy()
        np.random.shuffle(trip_coords_rand)
        trip_coords_rand_list.append(trip_coords_rand)
    return group_ids, trip_coords_list, trip_coords_inv_list, trip_coords_rand_list


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
    # If df_clusters is None, we assume that the clusters are already in df_vehiclepositions
    if df_clusters is not None:
        merged = df_vehiclepositions.join(df_clusters, how='left', on='trip_id')
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
        categories_var_name = "route_dir_cluster"
    else:
        merged = df_vehiclepositions
        categories_var_name = "cluster"
    # Don't use datapoints which don't have any cluster
    merged = merged.filter(pl.col("cluster").is_not_null())

    if export_intermediate_to_csv:
        out_path = f"output/joined_{terminal}_{date}.csv"
        merged.write_csv(out_path)
        print(f"Wrote joined CSV: {out_path} (rows={len(merged)})")

    categories = merged[categories_var_name].unique().sort().to_list()
    if verbose:
        print(f"Found {len(categories)} cluster(s): {categories}")

    all_gdfs = {}
    for i, cat_value in enumerate(categories):
        subset = merged.filter(pl.col(categories_var_name) == cat_value)
        pandas_df = subset.to_pandas()
 
        # POINTS GEOPACKAGE
        output_points_gpkg = f"output/geopackages/geopackage_{terminal}_{date}.gpkg"
        geometry = [
            Point(lon, lat)
            for lon, lat in zip(pandas_df["longitude"], pandas_df["latitude"])
        ]
        gdf = gpd.GeoDataFrame(pandas_df, geometry=geometry, crs="EPSG:4326")
        # Write mode: overwrite on first layer, append on the rest
        write_mode = "w" if i == 0 else "a"
        gdf.to_file(output_points_gpkg, layer=str(cat_value), driver="GPKG", mode=write_mode)
        if verbose:
            print(f"Layer '{cat_value}': written ({len(gdf)} rows)")

        # PATHS GEOPACKAGE
        if paths_gpkg:
            output_lines_gpkg = f"output/geopackages/geopackage_{terminal}_{date}_paths.gpkg"
            trips_list = subset["trip_id"].unique().sort().to_list()
            trajectory_gdfs = []
            for j, trip in enumerate(trips_list):
                subset2 = subset.filter(pl.col("trip_id") == trip)
                pandas_df = subset2.to_pandas()
                coords = list(zip(pandas_df["longitude"], pandas_df["latitude"]))
                if len(coords) >= 2:
                    line = LineString(coords)
                    trajectory_gdfs.append(gpd.GeoDataFrame(
                        {categories_var_name: [cat_value], "n_points": [len(coords)]},
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
                layer=str(layer_name),
                driver="GPKG",
                mode="w" if i == 0 else "a"
            )

    print("GPKG export done! In QGIS: Layer -> Add Layer -> Add Vector Layer, then select the .gpkg file.")


def get_planned_berth(row, stops, stop_times, stop_id_pattern):
    """Return planned berth for one bus position."""
    planned_berth = -1
    trip_id = row.get('trip_id', None)

    trip_stops = stop_times.filter(pl.col('trip_id').cast(pl.String) == str(trip_id)).get_column('stop_id').to_list()
    terminal_stops = [stop for stop in trip_stops if str(stop).startswith(stop_id_pattern)]
    if terminal_stops and stops is not None:
        matching_stops = stops.filter(pl.col('stop_id') == terminal_stops[0])
        if not matching_stops.is_empty():
            planned_berth = matching_stops.row(0, named=True)['platform_code']
    return planned_berth


def comparison(df, group_by, terminal, date, min_points_in_traj, min_trips_for_clustering,
                    discard_if_several_clusters, export_intermediate_to_csv, verbose,
                    dbscan_global_eps_percentile, dbscan_global_min_samples, paths_gpkg, static_data, 
                    berth_dist_threshold, expected_berth_dist_threshold, nb_consecutive_berths, 
                    traj_gap_seconds, max_speed_stopped, stop_id_pattern):
    results = []
    results_count = [0, 0, 0, 0]

    if group_by == "routes":
        # Remove values with no route or the special -2 sentinel
        df = df.filter((pl.col("route_short_name") != "-1") & (pl.col("route_short_name") != "-2"))
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
                        results_count[1] += 1
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
                                results_count[2] += 1
                                print(f"Discarding route {route}, direction {direction}, cluster {cluster} since there are only {df_clusters_cluster.shape[0]} trips in the main cluster.")
                                results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "nb_pairs": len(trip_ids), "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
                                continue
                            _, trip_coords_list_cluster, _, _ = format_data(df_direction.join(df_clusters_cluster, on="trip_id", how="left").filter(pl.col("cluster") == cluster), min_points_in_traj, verbose)
                            results_count[0] += 1
                            print(f"(route {iter_id+1}/{len(route_values)}) Now calculating distances for route {route}, direction {direction}, cluster {cluster}...")
                            sspd = tdist.pdist(trip_coords_list_cluster, metric="sspd", verbose=verbose)
                            dfd = tdist.pdist(trip_coords_list_cluster, metric="discret_frechet", verbose=verbose)
                            results.append(quick_analysis(sspd, measure="sspd", name=f"{date} route_{route}_dir_{direction}_cluster_{cluster}"))
                            results.append(quick_analysis(dfd, measure="dfd", name=f"{date} route_{route}_dir_{direction}_cluster_{cluster}"))
                else:
                    results_count[3] += 1
                    print(f"Not enough trajectories for route {route}, direction {direction} to calculate distance measures. Try changing the min_trips_for_clustering (current value: {min_trips_for_clustering}) parameter if you believe this is a mistake.")
                    results.append({"measure": "None", "name": f"{date} route_{route}_dir_{direction}", "nb_pairs": len(trip_ids), "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
        if export_intermediate_to_csv:
            out_path = f'output/cluster_assignments_{terminal}_{date}.csv'
            df_clusters.write_csv(out_path)
            print(f"Wrote clusters CSV: {out_path} (rows={len(df_clusters)})")
        export_to_gpkg(df, trajectory_clusters_df, terminal, date, export_intermediate_to_csv, verbose, paths_gpkg)
        print("Route+dirs... kept:", results_count[0], ", discarded because of multiple clusters:", results_count[1], ", discarded because of too few trajs in main cluster:", results_count[2], ", discarded because too few trajectories in general:", results_count[3])
        
    if group_by == "berths":
        berth_df = pl.read_csv('stop_detection/data/lkpg/berths.csv')
        provider = "otraf"

        # Split by trajectory, defined as a sequence of points with no time gaps larger than gap_seconds
        positions = (
            df.sort(['vehicle.id', 'timestamp'])
            .with_columns(
                trajectory_break=pl.col('timestamp').diff().over('vehicle.id').fill_null(0).ge(traj_gap_seconds)
            )
            .with_columns(
                trajectory_id=(
                    pl.col('vehicle.id').cast(pl.String) + pl.lit('-') +
                    pl.col('trajectory_break').cum_sum().over('vehicle.id').cast(pl.String)
                ),
                is_stopped=pl.col('speed').lt(max_speed_stopped),
            )
            .drop('trajectory_break')
        )
        geod = Geod(ellps='WGS84')
        output_columns = [
            'trajectory_id', 'vehicle', 'trajectory_start_timestamp', 'trajectory_end_timestamp',
            'stop_number', 'stop_start_timestamp', 'stop_end_timestamp', 'detected_berth',
            'start_route', 'start_direction', 'start_assigned_berth', 'end_route',
            'end_direction', 'end_assigned_berth', 'start_longitude', 'start_latitude'
        ]
        rows = []
        for trajectory in positions.partition_by('trajectory_id', as_dict=False, maintain_order=True):
            trajectory_id = trajectory.row(0, named=True)['trajectory_id']
            stop_groups = [
                group for group in trajectory.with_columns(
                    stopped_group=pl.col('is_stopped').ne(pl.col('is_stopped').shift()).fill_null(True).cum_sum()
                ).filter(pl.col('is_stopped'))
                .partition_by('stopped_group', as_dict=False, maintain_order=True)
            ]
            qualifying_stops = [group.drop('stopped_group') for group in stop_groups if group.height > nb_consecutive_berths]
            trajectory_start = trajectory.row(0, named=True)['timestamp']
            trajectory_end = trajectory.row(-1, named=True)['timestamp']

            # If there is no stop included in the trajectory
            if not qualifying_stops:
                rows.append({
                    'trajectory_id': trajectory_id, 'vehicle': trajectory.row(0, named=True)['vehicle.id'],
                    'trajectory_start_timestamp': trajectory_start, 'trajectory_end_timestamp': trajectory_end,
                    'stop_number': -1, **{column: -1 for column in output_columns[5:]}
                })
                continue

            for stop_number, stop in enumerate(qualifying_stops):
                start = stop.row(0, named=True)
                end = stop.row(-1, named=True)
                stops = static_data[provider]['stops']
                stop_times = static_data[provider]['stop_times']
                start_berth = get_planned_berth(start, stops, stop_times, stop_id_pattern)
                end_berth = get_planned_berth(end, stops, stop_times, stop_id_pattern)
                # First check if the expected (according to static data) end berth is within the distance threshold, then check the expected start berth, 
                # and finally check all berths if neither of those are within the threshold.
                # We start with the end berth because it is most likely the bus is stopped at the next berth it is supposed to depart from.
                detected_berth_found = False
                if end_berth != -1:
                    end_berth_row = berth_df.filter(pl.col('berth') == end_berth).row(0, named=True)
                    _, _, distance = geod.inv(start['longitude'], start['latitude'], end_berth_row['longitude'], end_berth_row['latitude'])
                    if distance <= expected_berth_dist_threshold:
                        detected_berth = end_berth_row['berth']
                        detected_berth_found = True
                if start_berth != -1 and not detected_berth_found:
                    start_berth_row = berth_df.filter(pl.col('berth') == start_berth).row(0, named=True)
                    _, _, distance = geod.inv(start['longitude'], start['latitude'], start_berth_row['longitude'], start_berth_row['latitude'])
                    if distance <= expected_berth_dist_threshold:
                        detected_berth = start_berth_row['berth']
                        detected_berth_found = True
                if not detected_berth_found:
                    for berth_row in berth_df.iter_rows(named=True):
                        _, _, distance = geod.inv(start['longitude'], start['latitude'], berth_row['longitude'], berth_row['latitude'])
                        if distance <= berth_dist_threshold:
                            detected_berth = berth_row['berth']
                            detected_berth_found = True
                            break
                if not detected_berth_found:
                    detected_berth = -1
                rows.append({
                    'trajectory_id': trajectory_id, 'vehicle': start['vehicle.id'],
                    'trajectory_start_timestamp': trajectory_start, 'trajectory_end_timestamp': trajectory_end,
                    'stop_number': stop_number, 'stop_start_timestamp': start['timestamp'],
                    'stop_end_timestamp': end['timestamp'], 'detected_berth': detected_berth,
                    'start_route': stop.row(0, named=True)['route_short_name'], 'start_direction': stop.row(0, named=True)['direction_id'],
                    'start_assigned_berth': start_berth, 'end_route': stop.row(-1, named=True)['route_short_name'],
                    'end_direction': stop.row(-1, named=True)['direction_id'], 'end_assigned_berth': end_berth,
                    'start_longitude': start.get('longitude', -1), 'start_latitude': start.get('latitude', -1)
                })
        # traj_stops_df is mostly to get the information of detected_berth, the rest of the columns is just there to give details on the process for the intermediate csv.
        traj_stops_df = pl.DataFrame(rows, schema=output_columns, orient='row')
        if export_intermediate_to_csv:
            out_path = "output/trajectory_stops_lkpg_220322_v3.csv"
            traj_stops_df.write_csv(out_path)
            print(f"Wrote trajectory stops CSV: {out_path} (rows={len(traj_stops_df)})")
        #Add a cluster number to every point based on its trajectory's berths.
        berth_sequences = {}
        trajectory_berths = {}
        cluster_numbers = {}
        for stop in traj_stops_df.sort(['trajectory_id', 'stop_number']).iter_rows(named=True):
            berth = str(stop['detected_berth'])
            if berth == '-1': continue
            trajectory_id = stop['trajectory_id']
            berth_sequences.setdefault(trajectory_id, []).append(berth)
            trajectory_berths.setdefault(trajectory_id, set()).add(berth)
        for trajectory_id, berth_sequence in berth_sequences.items():
            berth_key = tuple(sorted(berth_sequence))
            if berth_key not in cluster_numbers:
                cluster_numbers[berth_key] = len(cluster_numbers)
            berth_sequences[trajectory_id] = cluster_numbers[berth_key]
        trajectory_clusters_df = pl.DataFrame({
            'trajectory_id': list(berth_sequences.keys()),
            'cluster': list(berth_sequences.values()),
            'berths': [', '.join(sorted(trajectory_berths[trajectory_id])) for trajectory_id in berth_sequences],
        }, schema={'trajectory_id': pl.String, 'cluster': pl.Int64, 'berths': pl.String})
        clusters_df = (
            trajectory_clusters_df
            .group_by('cluster', maintain_order=True)
            .agg([
                pl.len().alias('nb_trajs'),
                pl.col('berths').first(),
            ])
        )
        df = positions.join(trajectory_clusters_df, on='trajectory_id', how='left')
        if export_intermediate_to_csv:
            out_path = f'output/positions_with_clusters_{terminal}_{date}.csv'
            #df.write_csv(out_path)
            print(f"Wrote positions CSV with clusters: {out_path} (rows={df.height})")
            out_path = f'output/cluster_assignments_{terminal}_{date}.csv'
            clusters_df.write_csv(out_path)
            print(f"Wrote clusters CSV: {out_path} (rows={clusters_df.height})")

        i = -1
        for cluster_row in clusters_df.iter_rows(named=True):
            i += 1
            temp_df = (
                positions.join(trajectory_clusters_df, on='trajectory_id', how='inner')
                .filter(pl.col('cluster') == cluster_row['cluster'])
            )
            if cluster_row['nb_trajs'] < min_trips_for_clustering or cluster_row['nb_trajs'] < 2:
                results_count[1] += 1
                print(f"Discarding cluster {cluster_row['cluster']} with berth(s) {cluster_row['berths']} since there are only {cluster_row['nb_trajs']} trajectories in the cluster. Try changing the min_trips_for_clustering (current value: {min_trips_for_clustering}) parameter if you believe this is a mistake.")
                results.append({"measure": "None", "name": f"{date} cluster_{cluster_row['cluster']}_berths_{cluster_row['berths'].replace(', ', '-')}", "nb_trajs": cluster_row['nb_trajs'], "mean": -1, "std": -1, "min": -1, "max": -1, "median": -1})
                continue
            _, trip_coords_list_cluster, _, _ = format_data(temp_df, min_points_in_traj, verbose, group_column='trajectory_id')
            results_count[0] += 1
            print(f"(cluster {i+1}/{clusters_df.shape[0]}) Now calculating distances for cluster {cluster_row['cluster']} with berths {cluster_row['berths']}...")
            sspd = tdist.pdist(trip_coords_list_cluster, metric="sspd", verbose=verbose)
            dfd = tdist.pdist(trip_coords_list_cluster, metric="discret_frechet", verbose=verbose)
            results.append(quick_analysis(sspd, measure="sspd", name=f"{date} cluster_{cluster_row['cluster']}_berths_{cluster_row['berths'].replace(', ', '-')}"))
            results.append(quick_analysis(dfd, measure="dfd", name=f"{date} cluster_{cluster_row['cluster']}_berths_{cluster_row['berths'].replace(', ', '-')}"))

        export_to_gpkg(df, None, terminal, date, export_intermediate_to_csv, verbose, paths_gpkg)
        print("Clusters... kept:", results_count[0], ", discarded because of too few trajectories in cluster:", results_count[1])

    results_df = pl.DataFrame(results)
    return results_df, results_count


def get_imprecision(terminal, date, providers, time_range=[5, 24], min_points_in_traj=10, min_trips_for_clustering=5, vehiclepositions_path=None,
                    discard_if_several_clusters=False, export_intermediate_to_csv=False, export_final_to_csv=True, verbose=False,
                    dbscan_global_eps_percentile=12, dbscan_global_min_samples=3, paths_gpkg=True, comparison_group_by="routes", static_data=None,
                    berth_dist_threshold=6, expected_berth_dist_threshold=9, nb_consecutive_berths=6, traj_gap_seconds=12, max_speed_stopped=1, stop_id_pattern="90220050000500"):
    """Get the imprecision value(s) for the desired location and timeframe

    :param terminal: The desired terminal name
    :type terminal: str
    :param date: The desired date in format "YYYY-MM-DD"
    :type date: str
    :param providers: The list of desired provider codes (e.g. ["otraf", "sl"]). Attention: if there are several operators, they must be in the same order than in the filename! All codes here:
        https://www.trafiklab.se/api/gtfs-datasets/gtfs-regional#operators-covered-by-this-dataset
    :type providers: list[str]
    :param time_range: 2-element-list: Start and beginning of desired time frame (e.g. `[5, 24]` -> only 1 timeframe, from 05:00:00 to 23:59:59 both included)
    :type time_range: list[int]
    :param min_points_in_traj: Minimum number of points in a trajectory for it to be considered in the calculations
    :type min_points_in_traj: int
    :param min_trips_for_clustering: Minimum number of trips/trajectories in the route+dir set for the clustering (or trajs in the berths cluster) and analysis to happen
    :type min_trips_for_clustering: int
    :param vehiclepositions_path: Path to the CSV file containing the VehiclePositions data for the desired terminal, date and providers. If None, it will be generated automatically
    :type vehiclepositions_path: str or None
    :param discard_if_several_clusters: (Routes comparison only) Whether the program should discard the route+dirs for which clustering has yield to more than 1 cluster (not counting outliers)
    :type discard_if_several_clusters: Boolean
    :param export_intermediate_to_csv: Whether to export the intermediate files (such as cluster_assignments and joined) to CSVs
    :type export_intermediate_to_csv: Boolean
    :param export_final_to_csv: Whether to export the final imprecision matrix to a CSV
    :type export_final_to_csv: Boolean
    :param verbose: Print (some) information about completed operations on the console. Some important stuff will be printed anyway
    :type verbose: Boolean
    :param dbscan_global_eps_percentile: (Routes comparison only) Parameter for the DBSCAN for the process "split by route+dir and cluster". Note: eps_percentile is the percentage in full numbers (e.g. 0.5 is 0.5%, NOT 50%)
    :type dbscan_global_eps_percentile: float
    :param dbscan_global_min_samples: (Routes comparison only) Parameter for the DBSCAN for the process "split by route+dir and cluster"
    :type dbscan_global_min_samples: int
    :param paths_gpkg: Whether you also want a 2nd GPKG file with paths instead of points
    :type paths_gpkg: Boolean
    :param comparison_group_by: For the distance computing, whethere to group by "routes" (including direction and cluster) or "berths"
    :type comparison_group_by: str
    :param static_data: (Berths comparison only) Static GTFS data for the terminal, used only if comparison_group_by is "berths". It should be a dictionary with keys "operators", each containing a polars DataFrame.
    :type static_data: dict[str, pl.DataFrame] or None
    :param berth_dist_threshold: (Berths comparison only) Radius in meters around a berth coordinate to consider a bus as stopped at that berth. For example: 6.
    :type berth_dist_threshold: int
    :param expected_berth_dist_threshold: (Berths comparison only) Radius in meters around the expected berth(s) to consider a bus stopped there, based on static GTFS data for the trip. For example: 9.
    :type expected_berth_dist_threshold: int
    :param nb_consecutive_berths: (Berths comparison only) Number of consecutive rows where the bus speed is below max_speed required for the stop to be considered valid. For example: 6.
    :type nb_consecutive_berths: int
    :param traj_gap_seconds: (Berths comparison only) Maximum allowed time gap in seconds between consecutive rows for them to belong to the same trajectory. For example: 12.
    :type traj_gap_seconds: int
    :param max_speed_stopped: (Berths comparison only) Maximum speed in km/h for a bus to be considered as stopped. For example: 1.
    :type max_speed_stopped: float
    :param stop_id_pattern: (Berths comparison only) Prefix that identifies terminal stop IDs, e.g. "90220050000500" for Linköping Centrum stops.
    :type stop_id_pattern: str
    :return: Tuple containing the pooled mean and standard deviation of imprecision, plus counts of kept and discarded trajectories.
    :rtype: tuple
    """
    data = get_data(terminal, providers, date, vehiclepositions_path=vehiclepositions_path)
    start = datetime.timestamp(datetime.strptime(f"{date} {time_range[0]:02d}:00:00", "%Y-%m-%d %H:%M:%S"))
    end = datetime.timestamp(datetime.strptime(f"{date} {(time_range[1]-1):02d}:59:59", "%Y-%m-%d %H:%M:%S"))
    data = data.filter(
        pl.col("timestamp")
        .is_between(
            start,
            end,
            closed="both",
        )
    )
    print(f"Uncertainty module: restricted time from {time_range[0]:02d}:00:00 (timestamp {start}) to {(time_range[1]-1):02d}:59:59  (timestamp {end})")
    # group_by is "routes" or "berths"
    results_df, results_count = comparison(data, group_by=comparison_group_by, terminal=terminal, date=date, min_points_in_traj=min_points_in_traj,
                            min_trips_for_clustering=min_trips_for_clustering, discard_if_several_clusters=discard_if_several_clusters,
                            export_intermediate_to_csv=export_intermediate_to_csv, verbose=verbose, dbscan_global_eps_percentile=dbscan_global_eps_percentile,
                            dbscan_global_min_samples=dbscan_global_min_samples, paths_gpkg=paths_gpkg, static_data=static_data,
                            berth_dist_threshold=berth_dist_threshold, expected_berth_dist_threshold=expected_berth_dist_threshold, nb_consecutive_berths=nb_consecutive_berths,
                            traj_gap_seconds=traj_gap_seconds, max_speed_stopped=max_speed_stopped, stop_id_pattern=stop_id_pattern)
    if export_final_to_csv:
        directory = "output/uncertainty"
        os.makedirs(directory, exist_ok=True)
        results_df.write_csv(os.path.join(directory, f'uncertainty_comparison_{terminal}_{("_".join(providers))}_{date}_{time_range[0]+1}.csv'))

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
    if len(results_count) > 2:
        imprecision_trajs = {
            "kept": results_count[0],
            "multiple_clusters": results_count[1],
            "too_few_trajs_main_cluster": results_count[2],
            "too_few_trajs_general": results_count[3]
        }
    else: # Was likely done by berths instead of route+dirs
        imprecision_trajs = {
            "kept": results_count[0],
            "too_few_trajs_general": results_count[1]
        }

    print("-- Imprecision calculations done! --")
    return pooled_mean, pooled_std, imprecision_trajs
