import requests
import csv
import polars as pl
import gnss_lib_py as glp
from skyfield import api


#NOTES
# Times in the TLE data use UTC
# All TLE measurements from the same sat return the same position because it is the position extrapolated for that satellite at the requested time (20250515-16:00), not the position of the satellite at the time of TLE measurement (so it's all good)
# For now the TLE data includes all Galileo satellites, in the future need to see what constellations the bus pos systems use
# Also worth mentioning: we use a simplified method and only estimate the visible satellite, not 100% reliable. But probably OK for comparing from one place/date/time to another

#TODO
# Change requested date/time to global parameter
# For each sat, only select the TLE measurement that is the closest to the requested time
# Get automatically altitude (should be doable via lantmateriet or https://en-gb.topographic-map.com/map-v1zs/Sweden/)
# Possibly increase vis threshold? possibly do something with az if relevant? (Basically, the question is: how can we consider that a satellite has line-of-sight?)
# Refactor the whole process in a single function, taking as parameter location, date/time. Also, figure out what to do with TLE data.
# Do it for all 12 stations, not just Karesuando

#LATER
# For HDOP: still to find how to obtain: altitude (should be doable via lantmateriet or https://en-gb.topographic-map.com/map-v1zs/Sweden/), nb of visible satellites. Then, can use gdoper
# Ionospheric delay?
# Tropospheric delay?


VIS_THRESH_DEG = 10 # Below visibility threshold (in degrees)






satellites = []
with open("../celestrak_may2024/combined.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines), 3):
        name = lines[i].strip()
        l1 = lines[i+1].strip()
        l2 = lines[i+2].strip()
        satellites.append(api.EarthSatellite(l1, l2, name))

ts = api.load.timescale()
t = ts.utc(2025, 5, 15, 16, 0, 0)

# Karesuando: 68.4418	22.4435
observer = api.wgs84.latlon(
    latitude_degrees=68.4418,
    longitude_degrees=22.4435,
    elevation_m=333
)
visible_count = 0

for sat in satellites:
    print(sat)
    difference = sat - observer
    topocentric = difference.at(t)

    alt, az, distance = topocentric.altaz()
    print(f"alt {alt}, az {az}, distance {distance}")

    if alt.degrees >= VIS_THRESH_DEG:
       print("Line-of-sight detected")
       visible_count += 1

print("Visible satellites:", visible_count)
