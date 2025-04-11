import pandas as pd
import numpy as np
from geopy.distance import geodesic

# Load your CSV file
df = pd.read_csv('entire_hour.csv')
df = df.loc[df['vehicle.id'] == 9031005920505507]

# Function to calculate the distance between two coordinates
def haversine(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters

# Step 1: Remove outliers based on a distance threshold
def remove_outliers(df, distance_threshold=100):  # Threshold can be adjusted
    cleaned_data = []
    cleaned_data.append(df.iloc[0])
    for i in range(1, len(df)):
        dist = haversine(df.iloc[i-1]['latitude'], df.iloc[i-1]['longitude'], df.iloc[i]['latitude'], df.iloc[i]['longitude'])
        if dist < distance_threshold:
            cleaned_data.append(df.iloc[i])
    return pd.DataFrame(cleaned_data)
print(len(df))
df_cleaned = remove_outliers(df)
print(len(df_cleaned))

# Step 2: Apply smoothing to the trajectory (using a simple moving average filter for the positions)
def smooth_trajectory(df, window_size=5):
    df['longitude_smooth'] = df['longitude'].rolling(window=window_size, min_periods=1).mean()
    df['latitude_smooth'] = df['latitude'].rolling(window=window_size, min_periods=1).mean()
    return df

df_cleaned = smooth_trajectory(df_cleaned)

# Step 3: Correct the trajectory based on bearing and speed
def correct_trajectory(df):
    corrected_coords = []
    for i in range(1, len(df)):
        # Calculate the new position using speed and bearing
        time_diff = int(df.iloc[i]['timestamp'] - df.iloc[i-1]['timestamp']) # Time difference in seconds
        distance_travelled = df.iloc[i-1]['speed'] * time_diff / 3.6  # Convert speed to m/s
        
        # Update position based on bearing and distance travelled
        bearing_rad = np.radians(df.iloc[i-1]['bearing'])
        delta_x = distance_travelled * np.sin(bearing_rad)
        delta_y = distance_travelled * np.cos(bearing_rad)
        
        new_x = df.iloc[i-1]['longitude'] + delta_x
        new_y = df.iloc[i-1]['latitude'] + delta_y
        
        corrected_coords.append((new_y, new_x))
    
    # Insert new coordinates (latitude, longitude) in the dataframe, except for the first row
    df.iloc[1:, 2:4] = corrected_coords
    return df

df_cleaned = correct_trajectory(df_cleaned)
df_cleaned.drop(df_cleaned.columns[0], axis=1, inplace=True)

# Step 4: Return the cleaned dataframe
df_cleaned.to_csv('entire_hour_cleaned.csv', index=False)

# Optionally, display the cleaned dataframe
print(df_cleaned.head())