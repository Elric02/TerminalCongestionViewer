import math
import polars as pl
from shapely.geometry import Point, Polygon
from pyproj import Geod


# Radius (in meters) around the coordinates of a berth to consider a bus as stopped at that berth. For example: 6
DIST_THRESHOLD = 6
# Input here how many times a row a bus must be at speed=0 to be considered stopped. For example: 3
NB_CONSECUTIVE = 5
# What is the start of all stop_id's for the stops at the terminal? For example: "90220050000500" (all stops at Linköping Centrum start with this sequence, and no other stop does)
STOP_ID_PATTERN = "90220050000500" # Linköping
# Maximum speed (in km/h) for a bus to be considered as stopped. For example: 1 km/h
MAX_SPEED = 1

# Input here the special zones. The zone has to be defined by at least 3 geographical points (its ends), and if a bus stopping inside should be considered as a regular stop or not.
SPECIAL_ZONES = [
    {'name': "traffic_lights",
     'regular': True,
     'coordinates': [(15.62236, 58.41773), (15.62214, 58.41761), (15.62192, 58.41773), (15.62214, 58.41785)]}
]

OUTPUT_CSV = "trajectory_stops_lkpg_220322.csv"



# Function to check if a point is within some special zones of the terminal
def check_special_zones(longitude, latitude, zones):
    is_inside = []
    for special_zone in zones:
        if len(special_zone['coordinates']) < 3:
            print("Error while checking for special zones: a polygon must have at least 3 points!")
            continue
        polygon = Polygon(special_zone['coordinates'])
        point_obj = Point((longitude, latitude))
        if polygon.contains(point_obj):
            is_inside.append(special_zone['regular'])
    if len(is_inside) > 1:
        print("More than one special zone detected! Check that they don't overlap. The first one will be kept.")
    if len(is_inside) > 0:
        return is_inside[0]
    else:
        return False

# Function that will add remarks to the stop based on some criteria
def check_special_stopping_conditions(longitude, latitude, bearing, timestart, entire_hour_stopped_df, crossings_df):
    remarks = {}
    # If there's any vehicle stopped in front of the stopped vehicle, and what that vehicle is (thought of) stopped for
    # Note: this feature seems to miss some occurrences
    geod = Geod(ellps="WGS84")
    # Tip of triangle, 2 meters in the given bearing direction
    tip_lon, tip_lat, _ = geod.fwd(longitude, latitude, bearing, 2)
    # Base of triangle, 22 meters away from origin at ±20° from bearing
    left_bearing, right_bearing = ((bearing - 20) % 360, (bearing + 20) % 360)
    base_left_lon, base_left_lat, _ = geod.fwd(longitude, latitude, left_bearing, 22)
    base_right_lon, base_right_lat, _ = geod.fwd(longitude, latitude, right_bearing, 22)
    time_filtered_df = entire_hour_stopped_df.filter(
        pl.col('timestamp').is_between(timestart - 5, timestart + 5)
    )
    is_inside = []
    for vehicle in time_filtered_df.iter_rows(named=True):
        if Polygon([(tip_lon, tip_lat), (base_left_lon, base_left_lat), (base_right_lon, base_right_lat)]).contains(Point((vehicle['longitude'], vehicle['latitude']))):
            # Only append a vehicle's id once
            if not any(x['vehicle'] == vehicle['vehicle.id'] for x in is_inside):
                is_inside.append({'vehicle': vehicle['vehicle.id']})
    if len(is_inside) > 0:
        remarks['vehicle_infront'] = is_inside
    # If the vehicle is stopped at a pedestrian crossing
    for crossing in crossings_df.iter_rows(named=True):
        _, _, distance = geod.inv(longitude, latitude, crossing['longitude'], crossing['latitude'])
        if distance <= 6:
            remarks['at_crossing'] = crossing['name']
    return remarks


def trip_details(row, trips, routes, stops, stop_times, stop_id_pattern):
    """Return route, direction, and planned berth for one position."""
    route = row.get('route_short_name', -1)
    direction = row.get('direction_id', -1)
    planned_berth = -1
    trip_id = row.get('trip_id', None)

    def is_missing(value):
        return value is None or (isinstance(value, float) and math.isnan(value))

    if is_missing(trip_id) or trips is None:
        return route if not is_missing(route) else -1, direction if not is_missing(direction) else -1, planned_berth

    trip = trips.filter(pl.col('trip_id').cast(pl.String) == str(trip_id))
    if trip.is_empty():
        return route if not is_missing(route) else -1, direction if not is_missing(direction) else -1, planned_berth

    trip = trip.row(0, named=True)
    route_id = trip.get('route_id')
    if (is_missing(route) or route == '') and routes is not None:
        matching_routes = routes.filter(pl.col('route_id') == route_id)
        route = matching_routes.row(0, named=True)['route_short_name'] if not matching_routes.is_empty() else -1
    if is_missing(direction):
        direction = trip.get('direction_id', -1)

    trip_stops = stop_times.filter(pl.col('trip_id').cast(pl.String) == str(trip_id)).get_column('stop_id').to_list()
    terminal_stops = [stop for stop in trip_stops if str(stop).startswith(stop_id_pattern)]
    if terminal_stops and stops is not None:
        matching_stops = stops.filter(pl.col('stop_id') == terminal_stops[0])
        if not matching_stops.is_empty():
            planned_berth = matching_stops.row(0, named=True)['platform_code']
    return route if not is_missing(route) else -1, direction if not is_missing(direction) else -1, planned_berth


def detect_trajectory_stops(entire_hour_df, trips=None, routes=None, stops=None,
                            stop_times=None, gap_seconds=30, nb_consecutive=3,
                            stop_id_pattern=STOP_ID_PATTERN, special_zones=None,
                            crossings_df=None, berth_df=None, dist_threshold=DIST_THRESHOLD):
    """Split positions into trajectories and return one descriptive row per stop."""
    required = {'vehicle.id', 'timestamp', 'speed'}
    missing = required - set(entire_hour_df.columns)
    if missing:
        raise ValueError(f'Missing required columns: {sorted(missing)}')

    positions = (
        entire_hour_df.sort(['vehicle.id', 'timestamp'])
        .with_columns(
            trajectory_break=pl.col('timestamp').diff().over('vehicle.id').fill_null(0).ge(gap_seconds)
        )
        .with_columns(
            trajectory_id=(
                pl.col('vehicle.id').cast(pl.String) + pl.lit('-') +
                pl.col('trajectory_break').cum_sum().over('vehicle.id').cast(pl.String)
            ),
            is_zero_speed=pl.col('speed').lt(MAX_SPEED),
        )
        .drop('trajectory_break')
    )

    if berth_df is None:
        berth_df = pl.read_csv('data/lkpg/berths.csv')
    if crossings_df is None:
        crossings_df = pl.read_csv('data/lkpg/pedestrian_crossings.csv')
    geod = Geod(ellps='WGS84')
    output_columns = [
        'trajectory_id', 'vehicle', 'trajectory_start_timestamp', 'trajectory_end_timestamp',
        'stop_number', 'stop_start_timestamp', 'stop_end_timestamp', 'detected_berth',
        'start_route', 'start_direction', 'start_assigned_berth', 'end_route',
        'end_direction', 'end_assigned_berth', 'start_longitude', 'start_latitude', 'remarks'
    ]
    rows = []

    for trajectory in positions.partition_by('trajectory_id', as_dict=False, maintain_order=True):
        trajectory_id = trajectory.row(0, named=True)['trajectory_id']
        stop_groups = [
            group for group in trajectory.with_columns(
                zero_group=pl.col('is_zero_speed').ne(pl.col('is_zero_speed').shift()).fill_null(True).cum_sum()
            ).filter(pl.col('is_zero_speed'))
            .partition_by('zero_group', as_dict=False, maintain_order=True)
        ]
        qualifying_stops = [group.drop('zero_group') for group in stop_groups if group.height >= nb_consecutive]
        trajectory_start = trajectory.row(0, named=True)['timestamp']
        trajectory_end = trajectory.row(-1, named=True)['timestamp']

        if not qualifying_stops:
            rows.append({
                'trajectory_id': trajectory_id, 'vehicle': trajectory.row(0, named=True)['vehicle.id'],
                'trajectory_start_timestamp': trajectory_start, 'trajectory_end_timestamp': trajectory_end,
                'stop_number': -1, **{column: -1 for column in output_columns[5:]}
            })
            continue

        for stop_number, stop in enumerate(qualifying_stops):
            start = stop.row(0, named=True)
            end = stop.row(-1, named=True)
            start_route, start_direction, start_berth = trip_details(start, trips, routes, stops, stop_times, stop_id_pattern)
            end_route, end_direction, end_berth = trip_details(end, trips, routes, stops, stop_times, stop_id_pattern)
            detected_berth = -1
            if {'longitude', 'latitude'}.issubset(stop.columns):
                for berth in berth_df.iter_rows(named=True):
                    _, _, distance = geod.inv(start['longitude'], start['latitude'], berth['longitude'], berth['latitude'])
                    if distance <= dist_threshold:
                        detected_berth = berth['berth']
                        break
            remarks = {}
            if special_zones is not None and {'longitude', 'latitude'}.issubset(start):
                zone = check_special_zones(start['longitude'], start['latitude'], special_zones)
                if zone:
                    remarks['zone'] = zone
            remarks.update(check_special_stopping_conditions(start['longitude'], start['latitude'], start.get('bearing', 0), start['timestamp'], stop, crossings_df))
            rows.append({
                'trajectory_id': trajectory_id, 'vehicle': start['vehicle.id'],
                'trajectory_start_timestamp': trajectory_start, 'trajectory_end_timestamp': trajectory_end,
                'stop_number': stop_number, 'stop_start_timestamp': start['timestamp'],
                'stop_end_timestamp': end['timestamp'], 'detected_berth': detected_berth,
                'start_route': start_route, 'start_direction': start_direction,
                'start_assigned_berth': start_berth, 'end_route': end_route,
                'end_direction': end_direction, 'end_assigned_berth': end_berth,
                'start_longitude': start.get('longitude', -1), 'start_latitude': start.get('latitude', -1),
                'remarks': remarks
            })
    return pl.DataFrame(rows, schema=output_columns, orient='row')




if __name__ == "__main__":
    trips = pl.read_csv('data/lkpg/trips.txt')
    routes = pl.read_csv('data/lkpg/routes.txt')
    stops = pl.read_csv('data/lkpg/stops.txt')
    stop_times = pl.read_csv('data/lkpg/stop_times.txt')

    entire_hour_df = pl.read_csv('data/lkpg/vehiclepositions_terminal_linköping_klt_otraf_2022-03-22_071600to083850.csv', schema_overrides={'vehicle.id': pl.String, 'trip_id': pl.String, 'route_id': pl.String})
    trajectory_stops_df = detect_trajectory_stops(
        entire_hour_df,
        trips=trips,
        routes=routes,
        stops=stops,
        stop_times=stop_times,
        gap_seconds=30,
        nb_consecutive=NB_CONSECUTIVE,
        stop_id_pattern=STOP_ID_PATTERN,
        special_zones=SPECIAL_ZONES,
        dist_threshold=DIST_THRESHOLD,
    )
    trajectory_stops_df.to_csv(OUTPUT_CSV, index=False)
    print(trajectory_stops_df)