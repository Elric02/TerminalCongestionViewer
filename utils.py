from pathlib import Path
import sys
import pandas as pd


# Attach cluster numbers (provided in cluster_assignments CSV file) to a normal vehiclepositions CSV file.
def join_clusters():
    base = Path(__file__).resolve().parent
    vp_path = base / "output" / "vehiclepositions_terminal_linköping_otraf_2022-03-22.csv"
    ca_path = base / "output" / "cluster_assignments_linköping_2022-03-22.csv"
    out_path = base / "output" / "joined_linköping_2022-03-22.csv"
    if not vp_path.exists():
        print(f"Vehicle positions file not found: {vp_path}")
        sys.exit(1)
    if not ca_path.exists():
        print(f"Cluster assignments file not found: {ca_path}")
        sys.exit(1)
    print(f"Reading vehicle positions: {vp_path}")
    df_v = pd.read_csv(vp_path, dtype={'trip_id': str, 'vehicle.id': str, 'route_id': str})
    print(f"Reading cluster assignments: {ca_path}")
    df_c = pd.read_csv(ca_path, dtype={'trip_id': str, 'cluster': str})
    merged = df_v.merge(df_c, how='left', on='trip_id')

    # Build formatted column: "{route_short_name}_{direction_id}_{cluster}"
    rs = merged['route_short_name'].fillna('').astype(str)
    di = merged['direction_id'].fillna('').astype(str)
    cl = merged['cluster'].fillna('').astype(str)
    merged['route_dir_cluster'] = rs + '_' + di + '_' + cl
    merged.to_csv(out_path, index=False)
    print(f"Wrote joined CSV: {out_path} (rows={len(merged)})")


if __name__ == '__main__':
    join_clusters()
