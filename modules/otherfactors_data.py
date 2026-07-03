import requests
import polars as pl
import gnss_lib_py as glp
import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import shutil
import gzip
import numpy as np
import warnings
import libs.ionex as ionex
import pandas as pd
from pyproj import Transformer


#NOTES
# Times in the TLE data use UTC
# All TLE measurements from the same sat return the same position because it is the position extrapolated for that satellite at the requested time (20250515-16:00), not the position of the satellite at the time of TLE measurement (so it's all good)
# For now the TLE data includes all Galileo satellites, in the future need to see what constellations the bus pos systems use
# Also worth mentioning: we use a simplified method and only estimate the visible satellite, not 100% reliable. But probably OK for comparing from one place/date/time to another


# IMPORTANT NOTE: for now, login is done according to this page: https://nsidc.org/data/user-resources/help-center/creating-netrc-file-earthdata-login

#TODO
# Coordinates as a parameter
# Better output and CSV output
# Take the closest IGS station (https://network.igs.org/) (see commented code and COORDINATES variable in main())
# Possibility of using other constellations (Currently we only use GPS data, the other constellations create an error on gnss-lib / georinex trying to load the data)


VIS_THRESH_DEG = 12 # Below satellite visibility threshold (in degrees), default is 12
DESIRED_DATETIME = [2024, 9, 13, 7, 0, 0] # Date and time to study in Sweden local time. Format: [year, month, day, hours, minutes, seconds]
DESIRED_TIMEZONE = ZoneInfo("Europe/Stockholm") # Default: ZoneInfo("Europe/Stockholm")

ELEVATION_API = "geotorget" # "open" if you want to use open-elevation.com (limited to below 60deg latitude), "google" for Google Elevation (requires API key), "geotorget" for Lantmäteriet/Geotorget (Sweden only, requires account credentials), "local" for the local file
GOOGLE_API_KEY_PATH = "google_api_key.txt" # Path to the key for the Google API usage. Can be ignored if Google is unused
GEOTORGET_CREDS_PATH = "geotorget_creds.txt" # Path to the file containing the Geotorget credentials (username and password). Format: username in the first line, password in the second line.
LOCAL_ELEVATION_PATH = "tempdata/elevation_data_archive.txt" # Path to the local elevation data archive file (txt). Leave blank if you don't want one
LOCAL_RINEX_PATH = "tempdata/rinex_nav" # Path which the RINEX files will be saved in
LOCAL_IONEX_PATH = "tempdata/ionex" # Path which the IONEX files will be saved in

LOCATIONS_CSV_PATH = "weather_stations.csv" # Path to the CSV containing the list of locations to study, with their coordinates


def get_desired_datetime_local():
    return datetime(*DESIRED_DATETIME, tzinfo=DESIRED_TIMEZONE)
def get_desired_datetime_utc():
    return get_desired_datetime_local().astimezone(timezone.utc)

# Return altitude based on provided coordinates
def get_alt(lat, lon, verbose=True):
    def call_elevation_api(url, params, auth=None, verbose=True):
        if ELEVATION_API == "geotorget":
            # WGS84 → SWEREF 99 TM (EPSG:3006), the only CRS the API accepts for simple GET queries
            _transformer = Transformer.from_crs("EPSG:4326", "EPSG:3006", always_xy=True)
            easting, northing = _transformer.transform(lon, lat)
            params = {"srid": 3006, "e": round(easting, 2), "n": round(northing, 2)}
            resp = requests.get(url, params=params, auth=auth, timeout=15)
            resp.raise_for_status()
            feature = resp.json()
            # The elevation is the third coordinate (Z) of the returned GeoJSON Point
            coords = feature["geometry"]["coordinates"]
            elevation = coords[2]
            nodata = feature.get("properties", {}).get("nodatavalue", -9999)
            if elevation == nodata:
                raise ValueError("No elevation data at this location (NoData). The point may be over water or outside Sweden's coverage.")
            elevation = float(elevation)
        else:
            response = requests.get(url, params=params)
            response.raise_for_status()
            elevation_data = response.json()
            if "results" not in elevation_data or len(elevation_data["results"]) == 0:
                raise RuntimeError("No elevation data returned")
            elevation = int(elevation_data["results"][0]["elevation"])
        if verbose:
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
        elevation = call_elevation_api(url, params, verbose=verbose)
    elif ELEVATION_API == "google":
        url = "https://maps.googleapis.com/maps/api/elevation/json"
        params = {"locations": f"{lat},{lon}", "key": open(GOOGLE_API_KEY_PATH, "r").read()}
        elevation = call_elevation_api(url, params, verbose=verbose)
    elif ELEVATION_API == "geotorget":
        url = "https://api.lantmateriet.se/distribution/produkter/markhojd/v1/hojd"
        params = {"locations": f"{lat},{lon}"}
        with open(GEOTORGET_CREDS_PATH, "r") as f:
            username, password = f.read().strip().splitlines()
        elevation = call_elevation_api(url, params, (username, password), verbose=verbose)
    elif ELEVATION_API == "local":
        if LOCAL_ELEVATION_PATH == "": raise RuntimeError("No LOCAL_ELEVATION_PATH provided")
        with open(LOCAL_ELEVATION_PATH, 'r') as f:
            elevation_dict = json.load(f)
            if elevation_dict[f"{lat}/{lon}"] == "": raise RuntimeError("No elevation value in local file for provided coordinates")
            elevation = elevation_dict[f"{lat}/{lon}"]
    return elevation


def get_cddis_data(constellation, type, verbose=True):
    epoch = get_desired_datetime_utc()

    # Build CDDIS URL and paths
    year = epoch.year
    yy = str(year)[-2:]
    # Day in format 000-356 as a string
    day_str = f"{epoch.timetuple().tm_yday:03d}"

    if type == "RINEX":
        download_dir = LOCAL_RINEX_PATH
        os.makedirs(download_dir, exist_ok=True)
        # Daily broadcast navigation file
        filename = f"ONS100SWE_R_{year}{day_str}0000_01D_{constellation[2]}.rnx.gz"
        url = (
            f"https://cddis.nasa.gov/archive/gnss/data/daily/"
            f"{year}/{day_str}/{yy}{constellation[1]}/{filename}"
        )
    elif type == "IONEX":
        download_dir = LOCAL_IONEX_PATH
        os.makedirs(download_dir, exist_ok=True)
        # Daily TEC map file from ESA
        filename = f"ESA0OPSRAP_{year}{day_str}0000_01D_01H_GIM.INX.gz"
        url = (
            f"https://cddis.nasa.gov/archive/gnss/products/ionex/"
            f"{year}/{day_str}/{filename}"
        )
    local_gz = os.path.join(download_dir, filename)
    local_file = local_gz.replace(".gz", "")

    # Download RINEX or IONEX data (if not already downloaded)
    if not os.path.exists(local_file):
        if verbose:
            print(f"Downloading {type} file...")
            print(url)
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_gz, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        # Decompress
        with gzip.open(local_gz, 'rb') as f_in:
            with open(local_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(local_gz)
    if verbose:
        print("Navigation file ready:", local_file)
    # Suppress unused warning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        pd.options.future.infer_string = False # disable new StringDtype inference, else loading Rinex data causes a crash
        # Parse the navigation file
        if type == "RINEX":
            navdata = glp.parsers.rinex_nav.RinexNav(local_file)
            return navdata
        elif type == "IONEX":
            ionodata = ionex.read_ionex(local_file)
            return ionodata


def get_hdop(lat, lon, navdata, constellation=["GPS", "n", "GN"], timestamp=None, alt=None, verbose=True):
    if timestamp is None:
        timestamp = get_desired_datetime_utc()
    elif isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=DESIRED_TIMEZONE)
        timestamp = timestamp.astimezone(timezone.utc)
    timestamp = timestamp.timestamp()
    alt = get_alt(lat, lon)

    # Compute satellite positions
    sat_states = glp.utils.sv_models.find_sv_states(timestamp, navdata)

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
    if verbose:
        with pl.Config(tbl_rows=1000):
            print(sat_states.sort("sv_id"))
    sat_positions = sat_states[["x_sv_m", "y_sv_m", "z_sv_m"]].to_numpy()

    if verbose:
        print(f"Visible satellites (constellation: {constellation[0]}):", sat_positions.shape[0])
    if sat_positions.shape[0] < 4:
        raise RuntimeError("Not enough satellites for DOP calculation")

    # Build Geometry Matrix G
    G = []
    for sat in sat_positions:
        diff = sat - rx_ecef
        rho = diff / np.linalg.norm(diff)
        G.append(np.hstack((rho, 1)))
    G = np.array(G)

    # Compute DOP values
    Q = np.linalg.inv(G.T @ G)
    hdop = np.sqrt(Q[0, 0] + Q[1, 1])
    print(datetime.now().strftime("%H:%M:%S"), "HDOP:", round(hdop, 3))
    return hdop


def get_iono_delay(ionodata, lat, lon, timestamp=None):
    if timestamp is None:
        timestamp = get_desired_datetime_local()
    elif isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=DESIRED_TIMEZONE)
    orig_timestamp = timestamp
    timestamp = timestamp.astimezone(timezone.utc)
    vtec_value = ionex.get_vtec_value(ionodata, lat, lon, timestamp, variable='tec')
    print(f"{datetime.now().strftime("%H:%M:%S")} VTEC at ({lat}, {lon}) / {orig_timestamp.isoformat()} local time, {timestamp.isoformat()} UTC / {vtec_value} TECU")
    return vtec_value


def main():
    # Rework locations, I want the terminal locations rather than the weather stations
    '''
    locations_df = pl.read_csv(LOCATIONS_CSV_PATH)
    for location in locations_df.iter_rows(named=True):
        print("LOCATION:", location["Name"])
        constellations = [["GPS", "n", "GN"]]
        for constellation in constellations:
            navdata = get_cddis_data(constellation, "RINEX")
            get_hdop(location["Lat"], location["Lon"], navdata, constellation)
            #get_iono_delay(navdata)
            #get_iono_delay2(navdata)
    '''
    COORDINATES = [58.4170492, 15.6238495] # Coordinates for Linköping Resecentrum in EPSG:4326/WGS84 (lat, lon)
    print("Getting HDOP and iono delay for coordinates:", COORDINATES)
    #constellations = [["Beidou", "f", "CN"], ["GLONASS", "g", "RN"], ["Galileo", "l", "EN"], ["GPS", "n", "GN"]]
    constellations = [["GPS", "n", "GN"]]
    for constellation in constellations:
        navdata = get_cddis_data(constellation, "RINEX", verbose=True)
        ionodata = get_cddis_data(constellation, "IONEX", verbose=True)
        hdop = get_hdop(COORDINATES[0], COORDINATES[1], navdata, constellation, verbose=True)
        vtec = get_iono_delay(ionodata, COORDINATES[0], COORDINATES[1], get_desired_datetime_local())

if __name__ == "__main__":
    main()
