import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import movingpandas as mpd
from datetime import datetime

# Load your data
df = pd.read_csv("entire_hour.csv", dtype={'vehicle.id': 'string', 'trip_id': 'string', 'route_id': 'string'})

# 2. Add a stable row ID (we'll use this to merge later)
df["row_id"] = df.index

# 3. Convert timestamp
df["timestamp"] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x))

# 4. Create geometry
df["geometry"] = df.apply(lambda row: Point(row["longitude"], row["latitude"]), axis=1)
gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326") # Use actual CRS if known

# 5. Create trajectories
traj_collection = mpd.TrajectoryCollection(gdf, traj_id_col="vehicle.id", t="timestamp")

# 6. Smooth each trajectory
cleaned_points = []
for traj in traj_collection.trajectories:
    smoothed_traj = mpd.KalmanSmootherCV(traj).smooth(process_noise_std=0.1, measurement_noise_std=2)
    # Get original trajectory DataFrame with row_id
    original_df = traj.df[["row_id"]]
    # Get cleaned point GeoDataFrame
    smoothed_gdf = smoothed_traj.to_point_gdf()
    # Add row_id back using index alignment
    smoothed_gdf["row_id"] = original_df["row_id"].values
    cleaned_points.append(smoothed_gdf)

# 7. Concatenate all cleaned points
cleaned_gdf = pd.concat(cleaned_points)

# 8. Update coordinates in the original DataFrame using row_id
df.set_index("row_id", inplace=True)
cleaned_gdf.set_index("row_id", inplace=True)

df.loc[cleaned_gdf.index, "x"] = cleaned_gdf.geometry.x
df.loc[cleaned_gdf.index, "y"] = cleaned_gdf.geometry.y
df.reset_index(drop=True, inplace=True)
df["timestamp"] = df['timestamp'].apply(lambda x: int(float(datetime.timestamp(x))))
print(df)
df.to_csv('entire_hour_cleaned.csv')