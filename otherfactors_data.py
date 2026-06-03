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
import libs.ionex as ionex


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
# Implement other source of altitude (should be doable via lantmateriet or https://en-gb.topographic-map.com/map-v1zs/Sweden/)
# Give the option to use Swepos instead https://www.lantmateriet.se/en/geodata/gps-geodesy-and-swepos/lantmateriets-doi-objects/swepos-rinex-data/?utm_source=chatgpt.com
# Take the closest IGS station (https://network.igs.org/)


VIS_THRESH_DEG = 12 # Below satellite visibility threshold (in degrees), default is 12
DESIRED_DATETIME = [2024, 9, 13, 7, 0, 0] # Date and time to study. Format: [year, month, day, hours, minutes, seconds]

ELEVATION_API = "google" # "open" if you want to use open-elevation.com, "google" for Google Elevation, "local" for the local file
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


def get_cddis_data(constellation, type):
    epoch = datetime(*DESIRED_DATETIME)
    download_dir = LOCAL_RINEX_PATH
    os.makedirs(download_dir, exist_ok=True)

    # Build CDDIS URL and paths
    year = epoch.year
    yy = str(year)[-2:]
    # Day in format 000-356 as a string
    day_str = f"{epoch.timetuple().tm_yday:03d}"

    if type == "RINEX":
        # Daily broadcast navigation file
        filename = f"ONS100SWE_R_{year}{day_str}0000_01D_{constellation[2]}.rnx.gz"
        url = (
            f"https://cddis.nasa.gov/archive/gnss/data/daily/"
            f"{year}/{day_str}/{yy}{constellation[1]}/{filename}"
        )
    elif type == "IONEX":
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
    print("Navigation file ready:", local_file)
    # Suppress unused warning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # Parse the navigation file
        navdata = glp.parsers.rinex_nav.RinexNav(local_file)
        return navdata


def get_hdop(timestamp, lat, lon, alt, constellation, navdata):
    timestamp = datetime(*DESIRED_DATETIME).timestamp()
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
    with pl.Config(tbl_rows=1000):
        print(sat_states.sort("sv_id"))
    sat_positions = sat_states[["x_sv_m", "y_sv_m", "z_sv_m"]].to_numpy()

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
    HDOP = np.sqrt(Q[0, 0] + Q[1, 1])
    print("HDOP:", round(HDOP, 3))


# ACTUALLY, the iono and tropo delays are per-satellite. Find how to get a single value
# Iono: Vertical Total Electron Content (VTEC)?
# Tropo: Zenith Tropospheric Delay (ZTD)?

def get_iono_delay(timestamp, lat, lon, alt, navdata):
    timestamp = datetime(*DESIRED_DATETIME).timestamp()

    # Compute satellite positions
    sat_states = glp.utils.sv_models.find_sv_states(timestamp, navdata)
    print(type(sat_states))
    print(navdata.iono_params)
    alpha = [1.6764e-08, 0.0, -1.1921e-07, 0.0]
    beta = [9.0112e4, 0.0, -1.3107e5, 0.0]
    
    c = 299792458.0
    psi = 0.0137 / (elev / np.pi + 0.11) - 0.022

    phi_i = lat / np.pi + psi * np.cos(az)
    phi_i = np.clip(phi_i, -0.416, 0.416)

    lam_i = lon / np.pi + psi * np.sin(az) / np.cos(phi_i * np.pi)

    phi_m = phi_i + 0.064 * np.cos((lam_i - 1.617) * np.pi)

    t_local = (43200 * lam_i + t) % 86400

    amp = alpha[0] + alpha[1]*phi_m + alpha[2]*phi_m**2 + alpha[3]*phi_m**3
    per = beta[0] + beta[1]*phi_m + beta[2]*phi_m**2 + beta[3]*phi_m**3

    amp = max(0, amp)
    per = max(72000, per)

    x = 2 * np.pi * (t_local - 50400) / per

    if abs(x) < 1.57:
        delay = 5e-9 + amp * (1 - x**2/2 + x**4/24)
    else:
        delay = 5e-9

    f = 1 + 16 * (0.53 - elev/np.pi)**3

    return c * f * delay
    print(len(output))
    print("Ionospheric delay (m):", round(iono, 3))
    print("Tropospheric delay (m):", round(tropo, 3))


def get_iono_delay2():
    ds = ionex.read_ionex('C:/Users/ElricM/OneDrive - VTI/Thesis/TerminalCongestionViewer/TerminalCongestionViewer/tempdata/ionex/ESA0OPSRAP_20251620000_01D_01H_GIM.INX')
    print(ds)
    ionex.plot_tec_map(ds.tec.isel(time=0))
    plt.show()

    # Plot the time series for a specific latitude and longitude
    ionex.plot_time_series(ds, lat=68.4418, lon=22.4435, variable='tec')
    plt.show()


def main():
    locations_df = pl.read_csv(LOCATIONS_CSV_PATH)
    for location in locations_df.iter_rows(named=True):
        print("LOCATION:", location["Name"])
        #constellations = [["Beidou", "f", "CN"], ["GLONASS", "g", "RN"], ["Galileo", "l", "EN"], ["GPS", "n", "GN"]]
        constellations = [["GPS", "n", "GN"]]
        for constellation in constellations:
            navdata = get_cddis_data(constellation, "RINEX")
            get_hdop(location["Lat"], location["Lon"], constellation, navdata)
            #get_iono_delay(navdata)
            get_iono_delay2(navdata)

if __name__ == "__main__":
    main()
