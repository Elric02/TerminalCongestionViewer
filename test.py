#import pykoda
import pandas as pd
import read_protobuf
import gtfs_realtime_pb2

# For now this is done manually 
#static_data = pykoda.datautils.load_static_data('otraf', '2022_03_22', remove_unused_stations=True)

MessageType = gtfs_realtime_pb2.FeedMessage()
df = read_protobuf.read_protobuf('../data/feed/07/otraf-vehiclepositions-2022-03-22T07-20-00Z.pb', MessageType)    # use file instead of bytes
df = pd.DataFrame(df['entity'].tolist())
print(df)
df.to_csv("test.csv")
