import requests
import csv
import polars as pl
import gnss_lib_py as glp
from skyfield import api
import json
import os
import libs.gdoper.src as gdoper # downloaded from https://github.com/FelipeTJ/gdoper


#NOTES
# Times in the TLE data use UTC
# All TLE measurements from the same sat return the same position because it is the position extrapolated for that satellite at the requested time (20250515-16:00), not the position of the satellite at the time of TLE measurement (so it's all good)
# For now the TLE data includes all Galileo satellites, in the future need to see what constellations the bus pos systems use
# Also worth mentioning: we use a simplified method and only estimate the visible satellite, not 100% reliable. But probably OK for comparing from one place/date/time to another
#
# Altitude data from open-elevation is limited to below 60deg latitude

#TODO
# Use gdoper to calculate hdop

#LATER
# For HDOP: still to find how to obtain: altitude (should be doable via lantmateriet or https://en-gb.topographic-map.com/map-v1zs/Sweden/), nb of visible satellites. Then, can use gdoper
# Ionospheric delay?
# Tropospheric delay?


VIS_THRESH_DEG = 12 # Below satellite visibility threshold (in degrees), default is 12
DESIRED_DATETIME = [2025, 5, 1, 16, 0, 0] # Date and time to study. Format: [year, month, day, hours, minutes, seconds]

ELEVATION_API = "local" # "open" if you want to use open-elevation.com, "google" for Google Elevation, "local" for the local file
GOOGLE_API_KEY_PATH = "google_api_key.txt" # Path to the key for the Google API usage. Can be ignored if Google is unused
LOCAL_ELEVATION_PATH = "tempdata/elevation_data_archive.txt" # Path to the local elevation data archive file (txt). Leave blank if you don't want one

LOCATIONS_CSV_PATH = "weather_stations.csv" # Path to the CSV containing the list of locations to study, with their coordinates
SATELLITE_TLE_PATH = [ # List of paths to the TLE files of satellites. Each file is treated separately. These have to be downloaded separately and must cover at least the desired date
    "../data/celestrak/galileo_may2024/combined.txt",
    "../data/celestrak/gps_may2024/combined.txt",
    "../data/celestrak/glonass_may2024/combined.txt",
    "../data/celestrak/beidou_may2024/combined.txt"
    ]


def get_visible_satellites_count(tle_path, lat, lon):
    satellites = []
    with open(tle_path) as f:
        lines = f.readlines()
        for i in range(0, len(lines), 3):
            name = lines[i].strip()
            l1 = lines[i+1].strip()
            l2 = lines[i+2].strip()
            satellites.append(api.EarthSatellite(l1, l2, name))

    ts = api.load.timescale()
    t = ts.utc(*DESIRED_DATETIME)

    # Get altitude
    def call_elevation_api(url, params):
        response = requests.get(url, params=params)
        response.raise_for_status()
        elevation_data = response.json()
        if "results" not in elevation_data or len(elevation_data["results"]) == 0:
            raise RuntimeError("No elevation data returned")
        elevation = int(elevation_data["results"][0]["elevation"])
        print(f"Returned altitude: {elevation}m")
        # Local file creation/update, kip if we don't want a local file
        if LOCAL_ELEVATION_PATH != "":
            # Make sure file exists (create if it doesn't)
            with open(LOCAL_ELEVATION_PATH, 'a+') as f:
                # Add elevation to corresponding latitude and longitude (dict_key)
                dict_key = f"{lat}/{lon}"
                # Check if file is empty
                if os.stat(LOCAL_ELEVATION_PATH).st_size == 0:
                    elevation_dict = {}
                else:
                    f.seek(0)
                    elevation_dict = json.loads(f.read())
                elevation_dict[dict_key] = elevation
                f.seek(0)
                f.truncate()
                f.write(json.dumps(elevation_dict))
        return elevation
    if ELEVATION_API == "open":
        url = "https://api.open-elevation.com/api/v1/lookup"
        params = {"locations": f"{lat},{lon}"}
        elevation = call_elevation_api(url, params)
    elif ELEVATION_API == "google":
        url = "https://maps.googleapis.com/maps/api/elevation/json"
        params = {"locations": f"{lat},{lon}", "key": open(GOOGLE_API_KEY_PATH, "r").read()}
        elevation = call_elevation_api(url, params)
    elif ELEVATION_API == "local":
        if LOCAL_ELEVATION_PATH == "": raise RuntimeError("No LOCAL_ELEVATION_PATH provided")
        with open(LOCAL_ELEVATION_PATH, 'r') as f:
            elevation_dict = json.load(f)
            if elevation_dict[f"{lat}/{lon}"] == "": raise RuntimeError("No elevation value in local file for provided coordinates")
            elevation = elevation_dict[f"{lat}/{lon}"]

    observer = api.wgs84.latlon(
        latitude_degrees=lat,
        longitude_degrees=lon,
        elevation_m=elevation
    )
    visible_count = 0

    # Contains the sat occurrence that is the closest in time to the desired date/time
    best_sat_occ = {}
    for sat_occ in satellites:
        # Keep only if closer in time
        try:
            current_best_time_diff = abs(best_sat_occ[sat_occ.name].epoch - t)
            sat_time_diff = abs(sat_occ.epoch - t)
            if sat_time_diff < current_best_time_diff:
                best_sat_occ[sat_occ.name] = sat_occ
            else:
                continue
        except KeyError:
            best_sat_occ[sat_occ.name] = sat_occ

    for sat_name in best_sat_occ:
        sat = best_sat_occ[sat_name]
        #print(sat)
        difference = sat - observer
        topocentric = difference.at(t)

        alt, az, distance = topocentric.altaz()
        #print(f"alt {alt}, az {az}, distance {distance}")

        if alt.degrees >= VIS_THRESH_DEG:
            #print("Line-of-sight detected")
            visible_count += 1

    print(f"Visible satellites: {visible_count} out of {len(best_sat_occ)}")
    return visible_count



def main():
    locations_df = pl.read_csv(LOCATIONS_CSV_PATH)
    for location in locations_df.iter_rows(named=True):
        print("LOCATION:", location["Name"])
        for tle_path in SATELLITE_TLE_PATH:
            print("Getting visible satellites for the following TLE file:", tle_path)
            visible_count = get_visible_satellites_count(tle_path, location["Lat"], location["Lon"])

if __name__ == "__main__":
    main()
