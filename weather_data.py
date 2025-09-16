import requests
import csv
import polars as pl

# Date to fetch, output CSV path
TARGET_DATE = "2025-05-02" # YYYY-MM-DD
OUTPUT_CSV = "output/smhi_precipitation_{}.csv".format(TARGET_DATE)


# Stations list URL
STATIONS_URL = "https://opendata-download-metobs.smhi.se/api/version/latest/parameter/7.json"
# Observations data URL
OBS_URL = lambda id : f"https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/7/station/{id}/period/corrected-archive/data.csv"


# Fetch all stations
def get_all_stations():
    resp = requests.get(STATIONS_URL)
    resp.raise_for_status()
    stations = resp.json().get("station")
    # Build a dict mapping station id -> metadata
    station_meta = {}
    for st in stations:
        sid = st.get("key")
        station_meta[sid] = {
            "name": st.get("name"),
            "latitude": st.get("latitude"),
            "longitude": st.get("longitude"),
            "height": st.get("height"),
        }
    return station_meta

# Fetch the data for each station
def fetch_weather_for_date(station_meta):
    df = pl.DataFrame(schema={"Date": pl.Utf8, "Time": pl.Utf8, "Value": pl.Float32, "Quality": pl.Utf8, "StationID": pl.Utf8, "StationLatitude": pl.Float64, "StationLongitude": pl.Float64})
    i = 0
    for station_id in station_meta.keys():
        resp = requests.get(OBS_URL(station_id))
        resp.raise_for_status()
        data = resp.content.decode('utf-8')
        cr = csv.reader(data.splitlines(), delimiter=';')
        cr_list = list(cr)
        # Find the first row of the actual data
        header_row_index = -1
        for j, e in enumerate(cr_list):
            if len(e) > 0:
                if e[0] == "Datum":
                    header_row_index = j
                    break
        # Drop the extra information that should not be part of the CSV, as well as empty rows + filter based on desired date
        cr_list = [x[:4] for x in cr_list[header_row_index+1:] if x[2] != "" and x[0] == TARGET_DATE]
        # Transform into DataFrame, append station info and merge to the central dataframe
        temp_df = pl.DataFrame(cr_list, schema={"Date": pl.Utf8, "Time": pl.Utf8, "Value": pl.Float32, "Quality": pl.Utf8}, orient="row")
        temp_df = temp_df.with_columns(pl.lit(station_id).alias("StationID"))
        temp_df = temp_df.with_columns(pl.lit(station_meta[station_id]["latitude"]).alias("StationLatitude"))
        temp_df = temp_df.with_columns(pl.lit(station_meta[station_id]["longitude"]).alias("StationLongitude"))
        df = pl.concat([df, temp_df])
        print(str(i)+"/"+str(len(station_meta))+" stations done"); i+=1
    return df


def main():
    station_meta = get_all_stations()
    weather_data = fetch_weather_for_date(station_meta)
    weather_data.write_csv(OUTPUT_CSV)
    print(f"Wrote precipitation data for {len(station_meta)} stations to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
