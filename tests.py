import glob
import os
import pandas as pd
import read_protobuf
import libs.gtfs_realtime_pb2 as gtfs_realtime_pb2
import time
from datetime import datetime, timezone
import requests


def test():
    MessageType = gtfs_realtime_pb2.FeedMessage()
    input_dir = 'output/imported_data/ttc/2026/06/23/16'
    for pb_path in sorted(glob.glob(os.path.join(input_dir, '*.pb'))):
        print(f'Processing {pb_path}')
        df = read_protobuf.read_protobuf(pb_path, MessageType)
        print(df.iloc[0])
    df = pd.DataFrame(df['entity'].tolist())
    print(df)


def download_pb():
    request_time = datetime.now()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    response = requests.get("https://gtfsrt.ttc.ca/vehicles/position?format=binary", timeout=30, headers=headers)
    response.raise_for_status()

    directory = os.path.join(
        "output/imported_data/ttc",
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
    main()