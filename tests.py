import glob
import os
import pandas as pd
import read_protobuf
import modules.libs.gtfs_realtime_pb2 as gtfs_realtime_pb2
import time
from datetime import datetime, timezone
import requests


def test():
    df = pd.DataFrame()
    MessageType = gtfs_realtime_pb2.FeedMessage()
    input_dir = 'output/imported_data/stm/2026/06/25/09'
    for pb_path in sorted(glob.glob(os.path.join(input_dir, '*.pb'))):
        print(f'Processing {pb_path}')
        temp_df = read_protobuf.read_protobuf(pb_path, MessageType)
        print(temp_df.iloc[0])
        if df.empty:
            df = temp_df
        else:
            df = pd.concat([df, temp_df], ignore_index=True)
    df = pd.DataFrame(df['entity'].tolist())
    print(df.shape)
    print(len(df['timestamp']))
    timestamps = df['timestamp'].tolist()
    timestamps.sort()
    print(timestamps)
    df.to_csv("test.csv")


def download_pb():
    request_time = datetime.now()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "apiKey": "l77599f4d902504c50b46172cc86032a71"
    }
    response = requests.get("https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions", timeout=30, headers=headers)
    response.raise_for_status()

    directory = os.path.join(
        "output/imported_data/stm",
        f"{request_time.year:04d}",
        f"{request_time.month:02d}",
        f"{request_time.day:02d}",
        f"{request_time.hour:02d}",
    )
    os.makedirs(directory, exist_ok=True)
    filename = request_time.strftime("%Y-%m-%d_%H-%M-%S.%f.pb")
    filepath = os.path.join(directory, filename)

    with open(filepath, "wb") as f:
        f.write(response.content)

    print(f"Saved: {filepath}")


def main():
    while True:
        start = time.time()

        try:
            download_pb()
        except Exception as e:
            print(f"Error: {e}")

        # Maintain approximately 1 request per second
        elapsed = time.time() - start
        time.sleep(max(0, 1.0 - elapsed))


if __name__ == "__main__":
    test()