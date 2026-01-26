import pandas as pd
import numpy as np
import read_protobuf
import gtfs_realtime_pb2


MessageType = gtfs_realtime_pb2.FeedMessage()
single_start_df = read_protobuf.read_protobuf('../data/realtime/otraf/VehiclePositions/2025/08/26/12/otraf-vehiclepositions-2025-08-26T12-00-00Z.pb', MessageType)
single_start_df = pd.DataFrame(single_start_df["entity"].tolist())
print(single_start_df)