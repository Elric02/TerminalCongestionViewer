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


# IMPORTANT NOTE: for now, login is done according to this page: https://nsidc.org/data/user-resources/help-center/creating-netrc-file-earthdata-login
# Also worth mentioning: we use a simplified method and only estimate the visible satellite, not 100% reliable. But probably OK for comparing from one place/date/time to another

#TODO
# Coordinates as a parameter
# Better output and CSV output
# Take the closest IGS station (https://network.igs.org/) (see commented code and COORDINATES variable in main())
# Possibility of using other constellations (Currently we only use GPS data, the other constellations create an error on gnss-lib / georinex trying to load the data)


def get_desired_datetime_local(desired_datetime, desired_timezone):
    return datetime(*desired_datetime, tzinfo=desired_timezone)
def get_desired_datetime_utc(desired_datetime, desired_timezone):
    return get_desired_datetime_local(desired_datetime, desired_timezone).astimezone(timezone.utc)

# Return altitude based on provided coordinates
def get_alt(
    lat,
    lon,
    verbose=True,
    elevation_api="geotorget",
    google_api_key_path="../google_api_key.txt",
    geotorget_creds_path="../geotorget_creds.txt",
    local_elevation_path="../tempdata/elevation_data_archive.txt",
):
    """Return the elevation at the given WGS84 coordinates.

    :param lat: Latitude in decimal degrees.
    :type lat: float
    :param lon: Longitude in decimal degrees.
    :type lon: float
    :param verbose: Whether to print status information.
    :type verbose: bool
    :param elevation_api: Elevation provider to use. Supported values are
        "open" for open-elevation.com (only works up to a certain latitude), "google" for Google Elevation
        (requires an API key), "geotorget" for Lantmäteriet/Geotorget
        (Sweden only, requires credentials), or "local" for the local archive
        file.
    :type elevation_api: str
    :param google_api_key_path: Optional path to the Google API key file when using the
        Google provider.
    :type google_api_key_path: str
    :param geotorget_creds_path: Optional path to the file containing Geotorget
        credentials (username and password).
    :type geotorget_creds_path: str
    :param local_elevation_path: Optional path to the local elevation archive file.
        Leave blank to disable local caching.
    :type local_elevation_path: str
    :return: The elevation in meters at the requested coordinates.
    :rtype: float
    """

    def call_elevation_api(url, params, auth=None, verbose=True):
        if elevation_api == "geotorget":
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
        if local_elevation_path != "":
            # Make sure file exists (create if it doesn't)
            with open(local_elevation_path, 'a+') as f:
                # Add elevation to corresponding latitude and longitude (dict_key)
                dict_key = f"{lat}/{lon}"
                # Check if file is empty
                if os.stat(local_elevation_path).st_size == 0:
                    elevation_dict = {}
                else:
                    f.seek(0)
                    elevation_dict = json.loads(f.read())
                elevation_dict[dict_key] = elevation
                f.seek(0)
                f.truncate()
                f.write(json.dumps(elevation_dict))
        return elevation
    
    if elevation_api == "open":
        url = "https://api.open-elevation.com/api/v1/lookup"
        params = {"locations": f"{lat},{lon}"}
        elevation = call_elevation_api(url, params, verbose=verbose)
    elif elevation_api == "google":
        url = "https://maps.googleapis.com/maps/api/elevation/json"
        with open(google_api_key_path, "r") as f:
            api_key = f.read().strip()
        params = {"locations": f"{lat},{lon}", "key": api_key}
        elevation = call_elevation_api(url, params, verbose=verbose)
    elif elevation_api == "geotorget":
        url = "https://api.lantmateriet.se/distribution/produkter/markhojd/v1/hojd"
        params = {"locations": f"{lat},{lon}"}
        with open(geotorget_creds_path, "r") as f:
            username, password = f.read().strip().splitlines()
        elevation = call_elevation_api(url, params, (username, password), verbose=verbose)
    elif elevation_api == "local":
        if local_elevation_path == "": raise RuntimeError("No local_elevation_path provided")
        with open(local_elevation_path, 'r') as f:
            elevation_dict = json.load(f)
            if elevation_dict[f"{lat}/{lon}"] == "": raise RuntimeError("No elevation value in local file for provided coordinates")
            elevation = elevation_dict[f"{lat}/{lon}"]
    return elevation


def get_cddis_data(
    constellation,
    type,
    desired_datetime,
    desired_timezone,
    verbose=True,
    local_rinex_path="../tempdata/rinex_nav",
    local_ionex_path="../tempdata/ionex",
):
    epoch = get_desired_datetime_utc(desired_datetime, desired_timezone)

    # Build CDDIS URL and paths
    year = epoch.year
    yy = str(year)[-2:]
    # Day in format 000-356 as a string
    day_str = f"{epoch.timetuple().tm_yday:03d}"

    if type == "RINEX":
        download_dir = local_rinex_path
        os.makedirs(download_dir, exist_ok=True)
        # Daily broadcast navigation file
        filename = f"ONS100SWE_R_{year}{day_str}0000_01D_{constellation[2]}.rnx.gz"
        url = (
            f"https://cddis.nasa.gov/archive/gnss/data/daily/"
            f"{year}/{day_str}/{yy}{constellation[1]}/{filename}"
        )
    elif type == "IONEX":
        download_dir = local_ionex_path
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


def get_hdop(
    lat,
    lon,
    desired_datetime,
    desired_timezone="Europe/Stockholm",
    constellation=["GPS", "n", "GN"],
    verbose=True,
    visibility_threshold_deg=12,
    elevation_api="geotorget",
    google_api_key_path="../google_api_key.txt",
    geotorget_creds_path="../geotorget_creds.txt",
    local_elevation_path="../tempdata/elevation_data_archive.txt",
    local_rinex_path="../tempdata/rinex_nav",
    local_ionex_path="../tempdata/ionex",
):
    """Estimate the horizontal dilution of precision for a location and time.

    The calculation uses broadcast navigation data from CDDIS and a simplified
    satellite visibility check.

    :param lat: Latitude in decimal degrees.
    :type lat: float
    :param lon: Longitude in decimal degrees.
    :type lon: float
    :param desired_datetime: Date and time values in local time as a list of
        [year, month, day, hour, minute, second].
    :type desired_datetime: list[int]
    :param desired_timezone: Time zone identifier for the requested local time (for example: Europe/Stockholm).
    :type desired_timezone: str
    :param constellation: GNSS constellation descriptor as [name, short_code,
        filename_code].
    :type constellation: list[str]
    :param verbose: Whether to print intermediate information.
    :type verbose: bool
    :param visibility_threshold_deg: Minimum satellite elevation in degrees for
        a satellite to be considered visible.
    :type visibility_threshold_deg: float
    :param elevation_api: Elevation provider to use for altitude lookup. Supported values are
        "open" for open-elevation.com (only works up to a certain latitude), "google" for Google Elevation
        (requires an API key), "geotorget" for Lantmäteriet/Geotorget
        (Sweden only, requires credentials), or "local" for the local archive
        file.
    :type elevation_api: str
    :param google_api_key_path: Optional path to the Google API key file when using the
        Google provider.
    :type google_api_key_path: str
    :param geotorget_creds_path: Optional path to the file containing Geotorget
        credentials.
    :type geotorget_creds_path: str
    :param local_elevation_path: Optional path to the local elevation archive file.
    :type local_elevation_path: str
    :param local_rinex_path: Optional path where RINEX navigation files are stored.
    :type local_rinex_path: str
    :param local_ionex_path: Optional path where IONEX files are stored.
    :type local_ionex_path: str
    :return: The estimated HDOP value.
    :rtype: float
    """
    desired_timezone = ZoneInfo(desired_timezone)
    navdata = get_cddis_data(
        constellation,
        "RINEX",
        desired_datetime,
        desired_timezone,
        verbose=verbose,
        local_rinex_path=local_rinex_path,
        local_ionex_path=local_ionex_path,
    )

    timestamp = get_desired_datetime_utc(desired_datetime, desired_timezone)
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=desired_timezone)
        timestamp = timestamp.astimezone(timezone.utc)
    timestamp = timestamp.timestamp()
    alt = get_alt(
        lat,
        lon,
        verbose=verbose,
        elevation_api=elevation_api,
        google_api_key_path=google_api_key_path,
        geotorget_creds_path=geotorget_creds_path,
        local_elevation_path=local_elevation_path,
    )

    # Compute satellite positions
    sat_states = glp.utils.sv_models.find_sv_states(timestamp, navdata)

    # Convert coordinates to ECEF format
    rx_ecef = glp.utils.coordinates.geodetic_to_ecef(np.array([[lat, lon, alt]])).flatten()
    # Get satellite positions in elevation/azimuth
    el_az = glp.utils.coordinates.ecef_to_el_az(rx_ecef, sat_states[["x_sv_m", "y_sv_m", "z_sv_m"]])
    elev = el_az[0] # Elevation in radians
    # This array contains Boolean values indicating for each sat if they are visible or not
    visible = elev > np.deg2rad(visibility_threshold_deg)
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


def get_iono_delay(
    lat,
    lon,
    desired_datetime,
    desired_timezone="Europe/Stockholm",
    constellation=["GPS", "n", "GN"],
    verbose=True,
    local_ionex_path="../tempdata/ionex",
):
    """Estimate the ionospheric delay for a location and time.

    The calculation uses daily IONEX maps from CDDIS to evaluate the vertical
    total electron content (VTEC) at the requested coordinates and datetime.

    :param lat: Latitude in decimal degrees.
    :type lat: float
    :param lon: Longitude in decimal degrees.
    :type lon: float
    :param desired_datetime: Date and time values in local time as a list of
        [year, month, day, hour, minute, second].
    :type desired_datetime: list[int]
    :param desired_timezone: Time zone identifier for the requested local time (for example: Europe/Stockholm).
    :type desired_timezone: str
    :param constellation: GNSS constellation descriptor as [name, short_code,
        filename_code].
    :type constellation: list[str]
    :param timestamp: Optional datetime to override the requested time.
    :type timestamp: datetime
    :param verbose: Whether to print intermediate information.
    :type verbose: bool
    :param local_ionex_path: Optional path where IONEX files are stored.
    :type local_ionex_path: str
    :return: The estimated VTEC value in TECU.
    :rtype: float
    """
    desired_timezone = ZoneInfo(desired_timezone)
    ionodata = get_cddis_data(
        constellation,
        "IONEX",
        desired_datetime,
        desired_timezone,
        verbose=verbose,
        local_ionex_path=local_ionex_path,
    )

    timestamp = get_desired_datetime_local(desired_datetime, desired_timezone)
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=desired_timezone)
    orig_timestamp = timestamp
    timestamp = timestamp.astimezone(timezone.utc)
    vtec_value = ionex.get_vtec_value(ionodata, lat, lon, timestamp, variable='tec')
    print(f"{datetime.now().strftime('%H:%M:%S')} VTEC at ({lat}, {lon}) / {orig_timestamp.isoformat()} local time, {timestamp.isoformat()} UTC / {vtec_value} TECU")
    return vtec_value


def main():
    # Rework locations, I want the terminal locations rather than the weather stations
    '''
    locations_csv_path="weather_stations.csv"
    locations_df = pl.read_csv(locations_csv_path)
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
    desired_datetime = [2024, 9, 13, 13, 0, 0] # Date and time to study in Sweden local time. Format: [year, month, day, hours, minutes, seconds]
    desired_timezone = "Europe/Stockholm" # Default: "Europe/Stockholm"
    visibility_threshold_deg=12
    elevation_api="geotorget"
    for constellation in constellations:
        hdop = get_hdop(
            COORDINATES[0],
            COORDINATES[1],
            desired_datetime,
            desired_timezone,
            constellation,
            verbose=True,
            visibility_threshold_deg=visibility_threshold_deg,
            elevation_api=elevation_api
        )
        vtec = get_iono_delay(
            COORDINATES[0],
            COORDINATES[1],
            desired_datetime,
            desired_timezone,
            constellation,
            verbose=True
        )

if __name__ == "__main__":
    main()
