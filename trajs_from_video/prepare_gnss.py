import pandas as pd
from matplotlib.path import Path

INPUT_CSV = "../output/vehiclepositions/vehiclepositions_terminal_linköping_klt_otraf_2022-03-22_071600to083850.csv"
OUTPUT_CSV = "output/trajs_comparison/vehiclepositions_terminal_linköping_klt_otraf_2022-03-22_071600to083850_treated_nostop.csv"

# Polygon vertices as (lat, lon), in order
ZONE_POLYGON = [
    (58.41707491, 15.62322917),
    (58.41722700, 15.62328556),
    (58.41732673, 15.62338183),
    (58.41747723, 15.62359063),
    (58.41677047, 15.62494802),
    (58.41664240, 15.62488409),
    (58.41665465, 15.62471782),
    (58.41658073, 15.62461258),
    (58.41675739, 15.62417232),
    (58.41660166, 15.62406504),
    (58.41707491, 15.62322917),
]

MAX_GAP_SECONDS = 10 # How many seconds of gap between points in a trajectory before splitting into a new trajectory
MIN_POINTS_PER_TRAJECTORY = 7 # Minimum number of points required for a trajectory to be included

VEHICLE_ID_COLUMN = "vehicle.id"
LAT_COLUMN = "latitude"
LON_COLUMN = "longitude"
TIMESTAMP_COLUMN = "timestamp" # POSIX seconds, or a datetime string
TIMESTAMP_IS_NUMERIC = True
TRAJECTORY_ID_COLUMN = "trajectory_id"

# Remove stops?
REMOVE_STOPS = False
SYNC_TO_FIRST = True # Instead of removing stops, sync stopped locations to the first stopped point of that stopping occurrence. Not recommended if REMOVE_STOPS is also True.
SPEED_COLUMN = "speed"
MAX_SPEED = 1 # Max speed in m/s, all points with speed below will be removed


def prepare_gnss(remove_stops=False, sync_to_first=False):
    df = pd.read_csv(INPUT_CSV, dtype={VEHICLE_ID_COLUMN: str})

    polygon = Path(ZONE_POLYGON)
    inside = polygon.contains_points(df[[LAT_COLUMN, LON_COLUMN]].to_numpy())
    df = df[inside].copy()

    if TIMESTAMP_IS_NUMERIC:
        seconds = pd.to_numeric(df[TIMESTAMP_COLUMN])
    else:
        seconds = pd.to_datetime(df[TIMESTAMP_COLUMN]).astype("int64") / 1e9

    df["_seconds"] = seconds
    df = df.sort_values([VEHICLE_ID_COLUMN, "_seconds"], kind="stable")

    gap = df.groupby(VEHICLE_ID_COLUMN)["_seconds"].diff()
    new_traj = gap.isna() | (gap > MAX_GAP_SECONDS)
    counter = new_traj.groupby(df[VEHICLE_ID_COLUMN]).cumsum()
    df[TRAJECTORY_ID_COLUMN] = df[VEHICLE_ID_COLUMN] + "_" + counter.astype(int).astype(str)

    traj_sizes = df.groupby(TRAJECTORY_ID_COLUMN)[TRAJECTORY_ID_COLUMN].transform("size")
    df = df[traj_sizes >= MIN_POINTS_PER_TRAJECTORY]

    if remove_stops:
        df = df[df[SPEED_COLUMN] >= MAX_SPEED]
    elif sync_to_first:
        df = df.sort_values([TRAJECTORY_ID_COLUMN, "_seconds"], kind="stable")
        # Each consecutive run of stopped points within a trajectory is one stop occurrence;
        # all its points take the lat/lon of the first point of that occurrence
        stopped_mask = df[SPEED_COLUMN] < MAX_SPEED
        run_start = stopped_mask & ~(
            stopped_mask.groupby(df[TRAJECTORY_ID_COLUMN]).shift(1, fill_value=False)
        )
        run_id = run_start.groupby(df[TRAJECTORY_ID_COLUMN]).cumsum()
        group_keys = [df[TRAJECTORY_ID_COLUMN], run_id]
        for col in (LAT_COLUMN, LON_COLUMN):
            first_vals = df[col].where(stopped_mask).groupby(group_keys).transform("first")
            df[col] = df[col].where(~stopped_mask, first_vals)

    df = df.drop(columns="_seconds")
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(df)} rows, {df[TRAJECTORY_ID_COLUMN].nunique()} trajectories to {OUTPUT_CSV}")


if __name__ == "__main__":
    prepare_gnss(REMOVE_STOPS, SYNC_TO_FIRST)
