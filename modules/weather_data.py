import requests
import csv
import polars as pl
import matplotlib.pyplot as plt
import datetime


# Date(s) to fetch, output CSV path
ENTIRE_YEAR = True # True if you want data for a whole year, False if you want data for a specific date
TARGET_DATE = "2025-05-02" # "YYYY-MM-DD". It is also possible to write only "YYYY" if ENTIRE_YEAR has been set to True.
METEO_PARAMS = ["1", "6", "7", "9"] # List of the desired parameters to observe (e.g. "7" is the amount ot precipitation aggregated per hour). Source: https://opendata.smhi.se/metobs/resources/parameter#available-meterology-parameters
DOWNLOAD_DATA = True # True if data to be requested to SMHI, False if CSVs are already there
STATIONS_LIST_CSV_PATH = "weather_stations.csv" # CSV with the list of stations to fetch data for
DESIRED_TIMEZONE = "Europe/Stockholm" # Default: "Europe/Stockholm"

# Path for the output CSV containing raw data from SMHI
# Note on time in the output file: for amount of precipitation, row is up to that time (i.e. 6:00:00 -> data from 5:00:01 to 6:00:00)
# Note2: time is in UTC!
OUTPUT_CSV_RAW = lambda param : f"output/smhi_{param}_{TARGET_DATE}.csv"
# Path for the output CSV with analysis of all stations and parameters
OUTPUT_CSV_ANALYSIS = f"output/smhi_analysis_{TARGET_DATE}.csv"
# Path for the merged raw data CSV
OUTPUT_CSV_MERGED = f"output/smhi_merged_{TARGET_DATE}.csv"
# Parameters list URL
PARAMETERS_URL = f"https://opendata-download-metobs.smhi.se/api/version/latest.json"
# Stations list URL
STATIONS_URL = lambda param : f"https://opendata-download-metobs.smhi.se/api/version/latest/parameter/{param}.json"
# Observations data URL
OBS_URL = lambda param, id : f"https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{param}/station/{id}/period/corrected-archive/data.csv"


# Fetch all stations
def get_all_stations(stations_url):
    resp = requests.get(stations_url)
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
def fetch_weather_for_date(param, station_meta, obs_url_lambda):
    df = pl.DataFrame(schema={"Date": pl.Utf8, "Time": pl.Utf8, "Value": pl.Float32, "Quality": pl.Utf8, "StationID": pl.Utf8, "StationLatitude": pl.Float64, "StationLongitude": pl.Float64})
    i = 0
    desired_stations = pl.read_csv(STATIONS_LIST_CSV_PATH)["ID"].to_list()
    for station_id in station_meta.keys():
        if len(desired_stations) > 0 and int(station_id) in desired_stations:
            resp = requests.get(obs_url_lambda(param, station_id))
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
            if ENTIRE_YEAR:
                cr_list = [x[:4] for x in cr_list[header_row_index+1:] if x[2] != "" and TARGET_DATE[:4] in x[0]]
            else:
                cr_list = [x[:4] for x in cr_list[header_row_index+1:] if x[2] != "" and x[0] == TARGET_DATE]
            # Transform into DataFrame, append station info and merge to the central dataframe
            temp_df = pl.DataFrame(cr_list, schema={"Date": pl.Utf8, "Time": pl.Utf8, "Value": pl.Float32, "Quality": pl.Utf8}, orient="row")
            # Convert UTC date/time into desired local timezone and overwrite Date/Time columns
            temp_df = temp_df.with_columns(
                pl.concat_str([pl.col("Date"), pl.col("Time")], separator=" ")
                .str.strptime(pl.Datetime(time_zone="UTC"), "%Y-%m-%d %H:%M:%S", strict=True)
                .dt.convert_time_zone(DESIRED_TIMEZONE)
                .alias("LocalTimestamp")
            )
            temp_df = temp_df.with_columns([
                pl.col("LocalTimestamp").dt.strftime("%Y-%m-%d").alias("Date"),
                pl.col("LocalTimestamp").dt.strftime("%H:%M").alias("Time"),
            ]).drop("LocalTimestamp")
            temp_df = temp_df.with_columns(pl.lit(station_id).alias("StationID"))
            temp_df = temp_df.with_columns(pl.lit(station_meta[station_id]["latitude"]).alias("StationLatitude"))
            temp_df = temp_df.with_columns(pl.lit(station_meta[station_id]["longitude"]).alias("StationLongitude"))
            df = pl.concat([df, temp_df])
            if len(desired_stations) > 0:
                print(str(i+1)+"/"+str(len(desired_stations))+" stations done")
            else:
                print(str(i+1)+"/"+str(len(station_meta))+" stations done")
            i += 1
    return df

def merge_raw_weather_files(csv_path_lambda, param_descs, meteo_params):
    merged = None
    for param in meteo_params:
        df = pl.read_csv(csv_path_lambda(param))
        key_cols = [col for col in df.columns if col not in ("Value", "Quality")]
        print(key_cols)
        value_col = param_descs[param]
        quality_col = f"{value_col}_Quality"
        df = df.rename({"Value": value_col, "Quality": quality_col})
        print("df:")
        print(df)

        if merged is None:
            merged = df
        else:
            merged = merged.join(df, on=key_cols, how="full", coalesce=True)
        print("Merged:")
        print(merged)
    return merged

def analyze_results(csv_path_lambda, param_descs, meteo_params):
    # Concatenate all CSVs into one DataFrame
    dfs = []
    for param in meteo_params:
        df = (
            pl.read_csv(csv_path_lambda(param))
            .with_columns([
                pl.col("Value").cast(pl.Float64),
                pl.lit(param_descs[param]).alias("Source")
            ])
        )
        dfs.append(df)
    df_all = pl.concat(dfs)

    # Compute interesting metrics per StationID
    metrics = (
        df_all
        .group_by(["Source", "StationID"])
        .agg([
            pl.count().alias("count"),
            pl.col("Value").min().alias("min"),
            pl.col("Value").max().alias("max"),
            pl.col("Value").mean().alias("mean"),
            pl.col("Value").median().alias("median"),
            pl.col("Value").std().alias("std"),
            pl.col("Value").quantile(0.25).alias("q25"),
            pl.col("Value").quantile(0.75).alias("q75"),
        ])
        .sort(["StationID", "Source"])
    )
    return df_all, metrics

def plot_results(df_all):
    for i, param in enumerate(df_all.unique(subset="Source").select("Source").iter_rows()):
        param = param[0]
        df = df_all.filter(pl.col("Source") == param)
        grouped = (
            df
            .group_by(["Source", "StationID"])
            .agg(pl.col("Value"))
            .sort(["Source", "StationID"])
        )

        data = [row["Value"] for row in grouped.iter_rows(named=True)]
        stations_csv = pl.read_csv(STATIONS_LIST_CSV_PATH)
        labels = [stations_csv.filter(pl.col("ID") == row["StationID"])["Name"][0] for row in grouped.iter_rows(named=True)]

        plt.subplot(2, 2, i+1)
        plt.violinplot(data, showmeans=True, showmedians=True)
        if i >= 2:
            plt.xticks(range(1, len(labels) + 1), labels, rotation=90)

        plt.title(param)
        if param == "Nederbördsmängd":
            plt.yscale("symlog", linthresh=1e-3)
            plt.ylim(bottom=0)
            plt.ylabel("Value (symlog scale)")
        else:
            plt.ylabel("Value")
        plt.tight_layout()
    plt.subplots_adjust(left=0.05, bottom=0.3, wspace=0.13, hspace=0.2)
    plt.show()


def main():
    # Get the name for each parameter
    resp = requests.get(PARAMETERS_URL)
    resp.raise_for_status()
    param_descs = dict([(x['key'], x['title']) for x in resp.json().get("resource")])

    if DOWNLOAD_DATA:
        # Raw data download and processing
        for param in METEO_PARAMS:
            #weather_data = fetch_weather_for_date(param, get_all_stations(STATIONS_URL(param)), OBS_URL)
            #weather_data.write_csv(OUTPUT_CSV_RAW(param))
            print(f"Wrote weather data to {OUTPUT_CSV_RAW(param)}")
        # Merging of the raw files
        merged_raw = merge_raw_weather_files(OUTPUT_CSV_RAW, param_descs, METEO_PARAMS)
        merged_raw.write_csv(OUTPUT_CSV_MERGED)
        print(f"Wrote merged weather data to {OUTPUT_CSV_MERGED}")

    # Analysis and plotting
    #df_all, metrics = analyze_results(OUTPUT_CSV_RAW, param_descs, METEO_PARAMS)
    #print(metrics)
    #metrics.write_csv(OUTPUT_CSV_ANALYSIS)
    #plot_results(df_all)

if __name__ == "__main__":
    main()
