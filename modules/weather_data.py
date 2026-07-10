import requests
import csv
import polars as pl
import matplotlib.pyplot as plt


# Note on time in the output file: for amount of precipitation and other aggregated data, row is up to that time (i.e. "6:00:00" -> data from 5:00:01 to 6:00:00)
# Note2: time in the SMHI data is in UTC, the program translates it to local time (so time fields in output files will be in local time)


# Path for the output CSV containing raw data from SMHI
OUTPUT_CSV_RAW = lambda param, target_date : f"output/smhi/smhi_{param}_{target_date}.csv"
# Path for the merged raw data CSV
OUTPUT_CSV_MERGED = lambda target_date : f"output/smhi/smhi_merged_{target_date}.csv"
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
        # Skip inactive stations
        if st.get("active") is not True:
            continue
        sid = st.get("key")
        station_meta[sid] = {
            "name": st.get("name"),
            "latitude": st.get("latitude"),
            "longitude": st.get("longitude")
        }
    return station_meta

# Fetch the data for each requested station
def fetch_weather_for_date(param, station_meta, obs_url_lambda, target_date, desired_timezone="Europe/Stockholm"):
    resp = requests.get(obs_url_lambda(param, station_meta["id"]))
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
    cr_list = [x[:4] for x in cr_list[header_row_index+1:] if x[2] != "" and x[0] == target_date]
    # Transform into DataFrame, append station info and merge to the central dataframe
    df = pl.DataFrame(cr_list, schema={"Date": pl.Utf8, "Time": pl.Utf8, "Value": pl.Float32, "Quality": pl.Utf8}, orient="row")
    # Convert UTC date/time into desired local timezone and overwrite Date/Time columns
    df = df.with_columns(
        pl.concat_str([pl.col("Date"), pl.col("Time")], separator=" ")
        .str.strptime(pl.Datetime(time_zone="UTC"), "%Y-%m-%d %H:%M:%S", strict=True)
        .dt.convert_time_zone(desired_timezone)
        .alias("LocalTimestamp")
    )
    df = df.with_columns([
        pl.col("LocalTimestamp").dt.strftime("%Y-%m-%d").alias("Date"),
        pl.col("LocalTimestamp").dt.strftime("%H:%M").alias("Time"),
    ]).drop("LocalTimestamp")
    df = df.with_columns(pl.lit(station_meta["id"]).alias("StationID"))
    df = df.with_columns(pl.lit(station_meta["latitude"]).alias("StationLatitude"))
    df = df.with_columns(pl.lit(station_meta["longitude"]).alias("StationLongitude"))
    return df

def merge_raw_weather_files(csv_path_lambda, param_descs, meteo_params, target_date):
    merged = None
    for param in meteo_params:
        df = pl.read_csv(csv_path_lambda(param, target_date))
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


def get_weather(
    lat,
    lon,
    desired_datetime,
    desired_timezone,
    weather_parameters,
    output_csv=False
):
    """Get the weather values for specific location, date, and time.
    Note: this function uses data from SMHI, which means that it is only usable in Sweden.

    :param lat: Latitude in decimal degrees.
    :type lat: float
    :param lon: Longitude in decimal degrees.
    :type lon: float
    :param desired_datetime: Date and time values in local time as a list of
        [year, month, day, hour, minute, second]. Note: for aggregated parameters, 
        it takes the hour up to that time (i.e. 6:00:00 -> data from 5:00:01 to 6:00:00)
    :type desired_datetime: list[int]
    :param desired_timezone: Time zone identifier for the requested local time (for example: Europe/Stockholm).
    :type desired_timezone: str
    :param weather_parameters: List of the desired parameters to observe 
        (e.g. "7" is the amount ot precipitation aggregated per hour). 
        Source: https://opendata.smhi.se/metobs/resources/parameter#available-meterology-parameters
    :type weather_parameters: list[str]
    :param output_csv: Whether to output the CSV of weather data for that date
    :type output_csv: bool
    :return: The weather values per parameter.
    :rtype: dict
    """
    
    # Get the name for each parameter
    resp = requests.get(PARAMETERS_URL)
    resp.raise_for_status()
    param_descs = dict([(x['key'], x['title']) for x in resp.json().get("resource")])

    # Raw data download and processing
    target_date = f"{desired_datetime[0]}-{desired_datetime[1]:02d}-{desired_datetime[2]:02d}"
    weather_values = {}
    for param in weather_parameters:
        all_stations = get_all_stations(STATIONS_URL(param))
        # Get closest station
        closest_id, closest_meta = min(
            all_stations.items(),
            key=lambda item: (item[1]["latitude"] - lat) ** 2 + (item[1]["longitude"] - lon) ** 2,
        )
        station_meta = {"id": closest_id, **closest_meta}
        print(f"Selected station for parameter {param}: {station_meta['name']} ({station_meta['id']})")

        weather_data = fetch_weather_for_date(
            param,
            station_meta,
            OBS_URL,
            target_date=target_date,
            desired_timezone=desired_timezone,
        )
        target_time = f"{desired_datetime[3]:02d}:00"
        filtered_values = weather_data.filter(pl.col("Time") == target_time)["Value"].to_list()
        weather_values[param_descs[param]] = {"value": filtered_values[0], "station_id": station_meta["id"]}

        if output_csv:
            weather_data.write_csv(OUTPUT_CSV_RAW(param, target_date))
            print(f"Wrote weather data to {OUTPUT_CSV_RAW(param, target_date)}")

    # Merging of the raw files
    if output_csv:
        merged_raw = merge_raw_weather_files(OUTPUT_CSV_RAW(param, target_date), param_descs, weather_parameters, target_date)
        merged_raw.write_csv(OUTPUT_CSV_MERGED(target_date))
        print(f"Wrote merged weather data to {OUTPUT_CSV_MERGED(target_date)}")

    print("Weather values for coordinates:", lat, lon, "at", target_date, target_time, "are:", weather_values)
    return weather_values
