"""
Clean and merge vehicle tracks.

1. Cleaning : remove every track_id that has fewer than MIN_ROWS rows.
2. Merging  : if the FIRST row of track B lies within MAX_DISTANCE_M of the LAST row
              of another track A, and B starts less than MAX_TIME_GAP_S after A ends,
              then B gets the track_id of A.
3. Stops    : when a vehicle is stopped, its position drifts. Every stop is detected and
              replaced by a single anchor position (see clean_stops() for the details).
4. Logging  : everything that changes is printed on the console and written to LOG_FILE.

Order matters: stops are cleaned AFTER merging, so a stop that was split over two
track fragments is seen as one stop.
"""

import logging
import math
import os

import numpy as np
import pandas as pd

# ============================ PARAMETERS ============================
INPUT_FILE = "output/cleaned_trajs/lkpg_part4_clipped.csv"
OUTPUT_FILE = "output/cleaned_trajs/lkpg_part4_clipped_cleaned1.csv"
LOG_FILE = "output/cleaned_trajs/track_cleaning_log_part4.txt"

COL_TRACK = "track_id"
COL_TIME = "time_sec"
COL_LON = "longitude"
COL_LAT = "latitude"

# --- cleaning / merging ---
MIN_ROWS = 20            # Param1: tracks with fewer rows than this are removed
MAX_DISTANCE_M = 8.0     # Param2: max distance (metres) between end of A and start of B
MAX_TIME_GAP_S = 2.0     # Param3: max time gap (seconds) between end of A and start of B
ALLOW_TIME_OVERLAP = True  # if True, B may start slightly before A ends (gap < 0 allowed)
MAX_TIME_OVERLAP_S = 2.0   # ... but at most this many seconds before A ends

# --- stop / drift cleaning ---
CLEAN_STOPS = True       # switch the whole step on/off
STOP_MODE = "snap"   # "collapse": a stop becomes 2 rows (start, end) at the anchor
                         # "snap"    : all rows are kept, positions are set to the anchor
STOP_WINDOW_S = 3.0      # half-window (s) for the net-speed test
STOP_V_NET = 0.5         # m/s: net speed over the window below this = stationary
STOP_MIN_DURATION_S = 2.0  # minimum duration (s) of a stop
STOP_MAX_GAP_S = 6.0     # stationary runs closer than this (s) may be merged ...
STOP_R_MERGE_M = 2.0     # ... if their median positions are within this distance (m)
STOP_R_EXT_M = 1.0       # points within this distance (m) of the anchor belong to the stop
STOP_T_RETURN_S = 6.0    # an excursion outside STOP_R_EXT_M is absorbed if it returns within this time (s)
STOP_MIN_SPAN_S = 1.0    # windows shorter than this (track ends) are not evaluated
# ====================================================================

R_EARTH = 6371000.0


def setup_logger():
    for path in (LOG_FILE, OUTPUT_FILE):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    logger = logging.getLogger("tracks")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for handler in (logging.StreamHandler(), logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")):
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


def haversine_m(lon1, lat1, lon2, lat2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R_EARTH * math.asin(math.sqrt(a))


# ----------------------------------------------------------------------
# 1. short tracks
# ----------------------------------------------------------------------
def clean_short_tracks(df, log):
    counts = df.groupby(COL_TRACK).size()
    short = counts[counts < MIN_ROWS]
    log.info(f"CLEANING: {len(counts)} tracks found, {len(short)} have fewer than {MIN_ROWS} rows")
    for tid, n in short.items():
        log.info(f"  removed track_id {tid} ({n} rows)")
    out = df[~df[COL_TRACK].isin(short.index)].copy()
    log.info(f"CLEANING: removed {len(df) - len(out)} rows in total, {len(out)} rows remain")
    return out


# ----------------------------------------------------------------------
# 2. merging
# ----------------------------------------------------------------------
def merge_tracks(df, log):
    # first / last row of every track (by time)
    ordered = df.sort_values([COL_TRACK, COL_TIME], kind="stable")
    first = ordered.groupby(COL_TRACK).first()
    last = ordered.groupby(COL_TRACK).last()
    ids = list(first.index)

    # all candidate pairs (A ends -> B starts)
    candidates = []
    for a in ids:
        la = last.loc[a]
        for b in ids:
            if a == b:
                continue
            fb = first.loc[b]
            gap = fb[COL_TIME] - la[COL_TIME]
            # NOTE: the original condition had a third term "or (gap < MAX_TIME_GAP_S)",
            # which is true whenever the first term is false, so every pair was skipped
            # and no merge could ever happen. It has been removed. Once it is removed,
            # an unbounded overlap would accept a B that starts minutes before A ends,
            # so the overlap is now limited to MAX_TIME_OVERLAP_S.
            if gap >= MAX_TIME_GAP_S:
                continue
            if gap < 0 and (not ALLOW_TIME_OVERLAP or -gap > MAX_TIME_OVERLAP_S):
                continue
            dist = haversine_m(la[COL_LON], la[COL_LAT], fb[COL_LON], fb[COL_LAT])
            if dist < MAX_DISTANCE_M:
                candidates.append((dist, abs(gap), a, b, gap))

    log.info(f"MERGING: {len(candidates)} candidate pair(s) satisfy distance < {MAX_DISTANCE_M} m "
             f"and time gap < {MAX_TIME_GAP_S} s")

    # Greedy one-to-one matching, best (closest) pairs first.
    # Each track can be continued by only one track and can continue only one track.
    parent = {t: t for t in ids}          # union-find, prevents cycles

    def root(t):
        while parent[t] != t:
            t = parent[t]
        return t

    used_as_a, used_as_b = set(), set()
    merges = []                            # (a, b, dist, gap)
    for dist, _, a, b, gap in sorted(candidates):
        if a in used_as_a or b in used_as_b or root(a) == root(b):
            log.info(f"  skipped candidate {a} -> {b} (dist {dist:.2f} m, gap {gap:.3f} s): "
                     f"a better match was already used")
            continue
        used_as_a.add(a)
        used_as_b.add(b)
        parent[b] = a                      # b continues a
        merges.append((a, b, dist, gap))

    # Resolve chains: final id = id of the first track of the chain
    mapping = {t: root(t) for t in ids if root(t) != t}
    for a, b, dist, gap in merges:
        log.info(f"  merged track_id {b} -> {a} (end/start distance {dist:.2f} m, time gap {gap:.3f} s)")
    for t, r in mapping.items():
        if any(b == t and a != r for a, b, _, _ in merges):
            log.info(f"  (chain) track_id {t} ends up with final track_id {r}")

    changed = df[COL_TRACK].isin(mapping.keys()).sum()
    df = df.copy()
    df[COL_TRACK] = df[COL_TRACK].replace(mapping)
    log.info(f"MERGING: {len(merges)} merge(s) done, {changed} rows got a new track_id, "
             f"{df[COL_TRACK].nunique()} tracks remain")
    return df


# ----------------------------------------------------------------------
# 3. stops (position drift while the vehicle is stationary)
# ----------------------------------------------------------------------
def _to_xy(lat, lon, lat0, lon0):
    x = np.radians(lon - lon0) * R_EARTH * np.cos(np.radians(lat0))
    y = np.radians(lat - lat0) * R_EARTH
    return np.c_[x, y]


def _from_xy(xy, lat0, lon0):
    lat = lat0 + np.degrees(xy[:, 1] / R_EARTH)
    lon = lon0 + np.degrees(xy[:, 0] / (R_EARTH * np.cos(np.radians(lat0))))
    return lat, lon


def find_stops(t, xy):
    """
    One track (t sorted ascending, xy in metres). Returns [(start_idx, end_idx, anchor_xy), ...].

    1. Net speed over a centred window of +-STOP_WINDOW_S: drift averages out, real motion does not.
    2. Runs of stationary points are merged if they are close in time AND in space.
    3. The anchor is the median position of the run (dominated by the dense core).
    4. The stop is grown outwards in time: points within STOP_R_EXT_M of the anchor belong to it,
       and so does any excursion that returns within STOP_T_RETURN_S (the drift spurs).
    """
    n = len(t)
    if n < 3:
        return []

    # 1. windowed net speed
    lo = np.searchsorted(t, t - STOP_WINDOW_S, side="left")
    hi = np.searchsorted(t, t + STOP_WINDOW_S, side="right") - 1
    span = t[hi] - t[lo]
    net = np.linalg.norm(xy[hi] - xy[lo], axis=1)
    vnet = np.full(n, np.inf)
    ok = span >= STOP_MIN_SPAN_S
    vnet[ok] = net[ok] / span[ok]
    stationary = vnet < STOP_V_NET

    # 2. runs of stationary points
    edges = np.diff(np.r_[0, stationary.astype(int), 0])
    starts = np.where(edges == 1)[0]
    ends = np.where(edges == -1)[0] - 1

    runs = []                                  # [start, end, median_xy]
    for s, e in zip(starts, ends):
        med = np.median(xy[s:e + 1], axis=0)
        if (runs and t[s] - t[runs[-1][1]] < STOP_MAX_GAP_S
                and np.linalg.norm(med - runs[-1][2]) < STOP_R_MERGE_M):
            runs[-1][1] = e
            runs[-1][2] = np.median(xy[runs[-1][0]:e + 1], axis=0)
        else:
            runs.append([s, e, med])
    runs = [r for r in runs if t[r[1]] - t[r[0]] >= STOP_MIN_DURATION_S]

    # 3./4. anchor + grow outwards from the core
    stops = []
    for s, e, anchor in runs:
        near = np.linalg.norm(xy - anchor, axis=1) < STOP_R_EXT_M

        i, last_near = s, s                    # backwards
        while i > 0:
            i -= 1
            if near[i]:
                last_near = i
            elif t[last_near] - t[i] > STOP_T_RETURN_S:
                break
        s2 = last_near

        j, last_near = e, e                    # forwards
        while j < n - 1:
            j += 1
            if near[j]:
                last_near = j
            elif t[j] - t[last_near] > STOP_T_RETURN_S:
                break
        e2 = last_near

        if stops and s2 <= stops[-1][1]:       # consecutive stops must not overlap
            s2 = stops[-1][1] + 1
        stops.append((s2, e2, anchor))
    return stops


def clean_stops(df, log):
    if STOP_MODE not in ("collapse", "snap"):
        raise ValueError(f"STOP_MODE must be 'collapse' or 'snap', got {STOP_MODE!r}")

    df = df.reset_index(drop=True)             # unique index, original row order is kept
    lat0, lon0 = df[COL_LAT].mean(), df[COL_LON].mean()
    lat = df[COL_LAT].to_numpy(float, copy=True)
    lon = df[COL_LON].to_numpy(float, copy=True)
    drop = np.zeros(len(df), bool)

    n_stops = n_moved = 0
    log.info(f"STOPS: mode={STOP_MODE}, window=+-{STOP_WINDOW_S}s, v_net<{STOP_V_NET} m/s, "
             f"min duration={STOP_MIN_DURATION_S}s, r_ext={STOP_R_EXT_M} m, t_return={STOP_T_RETURN_S}s")

    for tid, g in df.groupby(COL_TRACK, sort=False):
        g = g.sort_values(COL_TIME, kind="stable")
        idx = g.index.to_numpy()
        t = g[COL_TIME].to_numpy(float)
        xy = _to_xy(g[COL_LAT].to_numpy(float), g[COL_LON].to_numpy(float), lat0, lon0)

        for s, e, anchor in find_stops(t, xy):
            rows = idx[s:e + 1]
            dist = np.linalg.norm(xy[s:e + 1] - anchor, axis=1)
            alat, alon = _from_xy(anchor[None, :], lat0, lon0)
            lat[rows] = alat[0]
            lon[rows] = alon[0]
            n_stops += 1
            n_moved += len(rows)
            removed = 0
            if STOP_MODE == "collapse":
                drop[idx[s + 1:e]] = True      # keep first and last row (preserves the dwell time)
                removed = max(len(rows) - 2, 0)
            log.info(f"  track_id {tid}: stop t={t[s]:.2f}-{t[e]:.2f}s ({t[e] - t[s]:.1f}s), "
                     f"{len(rows)} rows, max drift {dist.max():.2f} m, mean {dist.mean():.2f} m"
                     + (f", {removed} rows removed" if STOP_MODE == "collapse" else ", rows snapped to anchor"))

    out = df.copy()
    out[COL_LAT] = lat
    out[COL_LON] = lon
    out = out[~drop].reset_index(drop=True)
    log.info(f"STOPS: {n_stops} stop(s) found, {n_moved} rows set to their anchor, "
             f"{drop.sum()} rows removed, {len(out)} rows remain")
    return out


# ----------------------------------------------------------------------
def main():
    log = setup_logger()
    log.info(f"Reading {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    log.info(f"Loaded {len(df)} rows, {df[COL_TRACK].nunique()} tracks")
    log.info(f"Parameters: MIN_ROWS={MIN_ROWS}, MAX_DISTANCE_M={MAX_DISTANCE_M}, "
             f"MAX_TIME_GAP_S={MAX_TIME_GAP_S}, ALLOW_TIME_OVERLAP={ALLOW_TIME_OVERLAP} "
             f"(max {MAX_TIME_OVERLAP_S}s), "
             f"CLEAN_STOPS={CLEAN_STOPS}")

    df = clean_short_tracks(df, log)
    df = merge_tracks(df, log)
    if CLEAN_STOPS:
        df = clean_stops(df, log)

    df.to_csv(OUTPUT_FILE, index=False)
    log.info(f"Wrote {len(df)} rows ({df[COL_TRACK].nunique()} tracks) to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()