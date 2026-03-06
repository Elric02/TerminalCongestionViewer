import requests
import csv
import polars as pl
import gnss_lib_py as glp
from skyfield import api
import json
import os
from datetime import datetime
import shutil
import gzip
import numpy as np
import warnings


#NOTES
# Times in the TLE data use UTC
# All TLE measurements from the same sat return the same position because it is the position extrapolated for that satellite at the requested time (20250515-16:00), not the position of the satellite at the time of TLE measurement (so it's all good)
# For now the TLE data includes all Galileo satellites, in the future need to see what constellations the bus pos systems use
# Also worth mentioning: we use a simplified method and only estimate the visible satellite, not 100% reliable. But probably OK for comparing from one place/date/time to another
#
# Altitude data from open-elevation is limited to below 60deg latitude
#
# Feb 2026: skyfield / CelesTrak usage replaced by gnss_lib_py / EarthData
# IMPORTANT NOTE: for now, login is done according to this page: https://nsidc.org/data/user-resources/help-center/creating-netrc-file-earthdata-login
# Which means that the _netrc file in the folder here is unused. Need to find out what to do with that long term
#
# Currently we only use GPS data, the other constellations create an error on gnss-lib / georinex trying to load the data

#TODO
# Verify get_hdop calulations/results
# Remove get_visible_satellites_count
# Implement other source of altitude (should be doable via lantmateriet or https://en-gb.topographic-map.com/map-v1zs/Sweden/)
# Think about how to handle the login process (_netrc) for the future

#LATER
# Ionospheric delay?
# Tropospheric delay?


VIS_THRESH_DEG = 12 # Below satellite visibility threshold (in degrees), default is 12
DESIRED_DATETIME = [2024, 5, 12, 17, 0, 0] # Date and time to study. Format: [year, month, day, hours, minutes, seconds]

ELEVATION_API = "local" # "open" if you want to use open-elevation.com, "google" for Google Elevation, "local" for the local file
GOOGLE_API_KEY_PATH = "google_api_key.txt" # Path to the key for the Google API usage. Can be ignored if Google is unused
LOCAL_ELEVATION_PATH = "tempdata/elevation_data_archive.txt" # Path to the local elevation data archive file (txt). Leave blank if you don't want one
LOCAL_RINEX_PATH = "tempdata/rinex_nav" # Path which the RINEX files will be saved in

LOCATIONS_CSV_PATH = "weather_stations.csv" # Path to the CSV containing the list of locations to study, with their coordinates
SATELLITE_TLE_PATH = [ # List of paths to the TLE files of satellites. Each file is treated separately. These have to be downloaded separately and must cover at least the desired date
    "../data/celestrak/galileo_may2024/combined.txt",
    "../data/celestrak/gps_may2024/combined.txt",
    "../data/celestrak/glonass_may2024/combined.txt",
    "../data/celestrak/beidou_may2024/combined.txt"
    ]


# Return altitude based on provided coordinates
def get_alt(lat, lon):
    def call_elevation_api(url, params):
        response = requests.get(url, params=params)
        response.raise_for_status()
        elevation_data = response.json()
        if "results" not in elevation_data or len(elevation_data["results"]) == 0:
            raise RuntimeError("No elevation data returned")
        elevation = int(elevation_data["results"][0]["elevation"])
        print(f"Returned altitude: {elevation}m")
        # Local file creation/update, skip if we don't want a local file
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
    return elevation


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

    observer = api.wgs84.latlon(
        latitude_degrees=lat,
        longitude_degrees=lon,
        elevation_m=get_alt(lat, lon)
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


def get_hdop(lat, lon, constellation):
    alt = get_alt(lat, lon)
    epoch = datetime(*DESIRED_DATETIME)
    download_dir = LOCAL_RINEX_PATH
    os.makedirs(download_dir, exist_ok=True)

    # Build CDDIS URL and paths
    year = epoch.year
    yy = str(year)[-2:]
    # Day in format 000-356 as a string
    day_str = f"{epoch.timetuple().tm_yday:03d}"
    # Daily broadcast navigation file
    filename = f"KIR800SWE_R_{year}{day_str}0000_01D_{constellation[2]}.rnx.gz"
    url = (
        f"https://cddis.nasa.gov/archive/gnss/data/daily/"
        f"{year}/{day_str}/{yy}{constellation[1]}/{filename}"
    )
    local_gz = os.path.join(download_dir, filename)
    local_rnx = local_gz.replace(".gz", "")

    # Download RINEX data (if not already downloaded)
    if not os.path.exists(local_rnx):
        print("Downloading RINEX navigation file...")
        print(url)
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_gz, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        # Decompress
        with gzip.open(local_gz, 'rb') as f_in:
            with open(local_rnx, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(local_gz)
    print("Navigation file ready:", local_rnx)
    # Suppress unused warning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # Parse the navigation file
        navdata = glp.parsers.rinex_nav.RinexNav(local_rnx)

    # Compute satellite positions
    sat_states = glp.utils.sv_models.find_sv_states(epoch.timestamp(), navdata)

    # Convert coordinates to ECEF format
    rx_ecef = glp.utils.coordinates.geodetic_to_ecef(np.array([[lat, lon, alt]])).flatten()
    # Get satellite positions in elevation/azimuth
    el_az = glp.utils.coordinates.ecef_to_el_az(rx_ecef, sat_states[["x_sv_m", "y_sv_m", "z_sv_m"]])
    elev = el_az[0] # Elevation in radians
    # This array contains Boolean values indicating for each sat if they are visible or not
    visible = elev > np.deg2rad(VIS_THRESH_DEG)
    # Only keep visible satellites
    sat_states = pl.DataFrame(sat_states.pandas_df()[visible])
    # For satellites present twice, only keep the first one
    sat_states = sat_states.unique(subset=['sv_id'], keep='first')
    with pl.Config(tbl_rows=1000):
        print(sat_states.sort("sv_id"))
    sat_positions = sat_states[["x_sv_m", "y_sv_m", "z_sv_m"]].to_numpy()

    print(f"Visible satellites (constellation: {constellation[0]}):", sat_positions.shape[0])
    if sat_positions.shape[0] < 4:
        raise RuntimeError("Not enough satellites for DOP calculation")

    # Build Geometry Matrix G
    G = []
    for sat in sat_positions:
        #TODO: swap rx_ecef and sat + add a "minus" to rho?
        diff = sat - rx_ecef
        rho = diff / np.linalg.norm(diff)
        G.append(np.hstack((rho, 1)))
    G = np.array(G)

    # Compute DOP values
    Q = np.linalg.inv(G.T @ G)
    #TODO: Q is Q_deltaX. Compute Q_enu?
    HDOP = np.sqrt(Q[0, 0] + Q[1, 1])
    VDOP = np.sqrt(Q[2, 2])
    PDOP = np.sqrt(Q[0, 0] + Q[1, 1] + Q[2, 2])
    print("HDOP:", round(HDOP, 3))
    print("VDOP:", round(VDOP, 3))
    print("PDOP:", round(PDOP, 3))


def main():
    locations_df = pl.read_csv(LOCATIONS_CSV_PATH)
    for location in locations_df.iter_rows(named=True):
        print("LOCATION:", location["Name"])
        for tle_path in SATELLITE_TLE_PATH:
            print("Getting visible satellites for the following TLE file:", tle_path)
            visible_count = get_visible_satellites_count(tle_path, location["Lat"], location["Lon"])
        #constellations = [["Beidou", "f", "CN"], ["GLONASS", "g", "RN"], ["Galileo", "l", "EN"], ["GPS", "n", "GN"]]
        constellations = [["GPS", "n", "GN"]]
        for constellation in constellations:
            get_hdop(location["Lat"], location["Lon"], constellation)

if __name__ == "__main__":
    main()
