#TODO:
# Go through each vehicle id
# Set coordinates of i to coordinates of i-1 if both i and i-1 have speed=0

import pandas as pd
import numpy as np


df = pd.read_csv('entire_hour.csv', dtype={'vehicle.id': 'string', 'trip_id': 'string', 'route_id': 'string'})

vehicles = df['vehicle.id'].unique()
for count, vehicle in enumerate(vehicles):
    if count%10==0: print("Vehicle", count, "of", len(vehicles))
    filtered_df = df.loc[df['vehicle.id'] == vehicle]
    previous_index = filtered_df.iloc[0, 0]
    for index, row in filtered_df.iloc[1:, :].iterrows():
        #print(index)
        if row['speed'] == 0 and filtered_df.loc[previous_index]['speed'] == 0:
            df.at[index, 'longitude'] = df.loc[previous_index]['longitude']
            df.at[index, 'latitude'] = df.loc[previous_index]['latitude']
        previous_index = index
df.to_csv('entire_hour_cleaned.csv')

print('Done!')