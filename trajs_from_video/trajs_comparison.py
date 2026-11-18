#!/usr/bin/env python3
"""Compare and match trajectories from two point CSV datasets.

The script performs time-overlap screening, spatial-bounding-box screening,
linear interpolation onto a common time grid (without extrapolation), and a
global one-to-one assignment. Distances are Euclidean distances in a projected
CRS whose coordinate units must be metres.
"""

import json
import math
import sys
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyproj import CRS, Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import min_weight_full_bipartite_matching
from shapely.geometry import LineString, Point


EPS = 1e-9


### SETTINGS

# Path to CSV dataset of trajs issued from video
DATASET_A_CSV = "output/cleaned_trajs/lkpg_merged_clipped_cleaned1.csv"
# Path to CSV dataset of trajs issued from GNSS
DATASET_B_CSV = "output/trajs_comparison/vehiclepositions_terminal_linköping_klt_otraf_2022-03-22_071600to083850_treated_nostop.csv"
# Output directory
OUTPUT_DIR = "output/trajs_comparison/lkpg_merged_nostop"

# CRS of both CSV coordinate columns (x=longitude, y=latitude)
INPUT_CRS = "EPSG:4326"
# Projected CRS in metres, e.g. EPSG:3006; "auto" estimates UTM for geographic input
METRIC_CRS = "auto"

# Trajectory ID column in dataset A
A_ID_COLUMN = "track_id"
# Timestamp column in dataset A
A_TIME_COLUMN = "timestamp"
# X/easting/longitude column in dataset A
A_X_COLUMN = "longitude"
# Y/northing/latitude column in dataset A
A_Y_COLUMN = "latitude"
# Trajectory ID column in dataset B
B_ID_COLUMN = "trajectory_id"
# Timestamp column in dataset B
B_TIME_COLUMN = "timestamp"
# X/easting/longitude column in dataset B
B_X_COLUMN = "longitude"
# Y/northing/latitude column in dataset B
B_Y_COLUMN = "latitude"

# Common comparison-grid interval in seconds
RESAMPLE_SECONDS = 1.0
# Do not interpolate across a source-data gap longer than this, in seconds
MAX_INTERP_GAP_SECONDS = 300.0
# Minimum actual temporal overlap, in seconds
MIN_OVERLAP_SECONDS = 5.0
# Minimum fraction of A's time span covered by B (0..1)
MIN_A_TIME_COVERAGE = 0.50
# Minimum fraction of overlap-grid times usable after gap checks (0..1)
MIN_VALID_SAMPLE_COVERAGE = 0.50
# Minimum valid interpolated samples for a candidate
MIN_SAMPLES = 1
# Maximum bounding-box separation for candidate screening, in metres
COARSE_DISTANCE_M = 500.0
# Maximum eligible-pair mean distance, in metres
MAX_MEAN_DISTANCE_M = 30.0
# Maximum eligible-pair 95th-percentile distance, in metres
MAX_P95_DISTANCE_M = 250.0
# Maximum matched-pair profiles drawn in the profile graph
MAX_PLOT_MATCHES = 500

# Output settings
# If True, all points are labeled with dataset and trajectory ID in the scatter plot; if False, only matched pairs are labeled.
SCATTER_LABEL_ON_ALL = True 
# Relative-position KDE graphs
# Matplotlib colormap used for the density (its lowest colour is made transparent)
KDE_CMAP = "Blues"
# The view spans +/- this percentile of the absolute offsets, so outliers don't squash the plot
KDE_VIEW_PERCENTILE = 98.0
# Lowest drawn density contour, as a fraction of the peak density (lower = fainter tails visible)
KDE_MIN_LEVEL_FRACTION = 0.001
# Number of density contour levels (log-spaced between the minimum fraction and the peak)
KDE_N_LEVELS = 20
# Opacity of the faintest / strongest density contour (0..1)
KDE_MIN_ALPHA = 0.3
KDE_MAX_ALPHA = 0.9
# Evaluation grid resolution per axis
KDE_GRID_SIZE = 200
# Individual-point marker size and opacity in the "with points" graph
KDE_POINT_SIZE = 1.5
KDE_POINT_ALPHA = 0.6
KDE_POINT_COLOR = "crimson"


def read_csv_normalized(path: str, ds_name: str, id_col: str, time_col: str,
                        x_col: str, y_col: str) -> tuple[pd.DataFrame, int]:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"CSV not found: {file_path}")

    raw = pd.read_csv(file_path)
    required = [id_col, time_col, x_col, y_col]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise ValueError(f"{ds_name}: missing column(s) {missing}. Available columns: {list(raw.columns)}")

    out = pd.DataFrame()
    out["trajectory_id"] = raw[id_col].astype("string").str.strip()
    timestamp_seconds = pd.to_numeric(raw[time_col], errors="coerce")
    out["t"] = timestamp_seconds.astype(np.float64)
    out["timestamp"] = pd.to_datetime(timestamp_seconds, unit="s", errors="coerce", utc=True)
    out["x_input"] = pd.to_numeric(raw[x_col], errors="coerce")
    out["y_input"] = pd.to_numeric(raw[y_col], errors="coerce")
    before = len(out)
    valid = (out["trajectory_id"].notna() & (out["trajectory_id"] != "") &
             out["timestamp"].notna() & np.isfinite(out["x_input"]) & np.isfinite(out["y_input"]))
    out = out.loc[valid].copy()
    dropped = before - len(out)
    if out.empty:
        raise ValueError(f"{ds_name}: no valid rows remain after parsing IDs, timestamps, and coordinates.")

    # Numeric seconds are used internally; UTC-aware strings are retained for output.
    out["timestamp_utc"] = out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    out["dataset"] = ds_name
    return out.reset_index(drop=True), dropped


def choose_metric_crs(input_crs_text: str, metric_crs_text: str,
                      a: pd.DataFrame, b: pd.DataFrame) -> CRS:
    input_crs = CRS.from_user_input(input_crs_text)
    if metric_crs_text.lower() == "auto":
        if input_crs.is_geographic:
            all_xy = pd.concat([a[["x_input", "y_input"]], b[["x_input", "y_input"]]], ignore_index=True)
            geom = gpd.GeoDataFrame(
                geometry=gpd.points_from_xy(all_xy["x_input"], all_xy["y_input"]),
                crs=input_crs,
            )
            metric_crs = geom.estimate_utm_crs()
            if metric_crs is None:
                raise ValueError("Could not estimate a UTM CRS. Supply --metric-crs explicitly.")
            metric_crs = CRS.from_user_input(metric_crs)
        else:
            # Keep an already projected CRS only if its coordinate units are metres.
            if not input_crs.is_projected or not input_crs.axis_info:
                raise ValueError("For this input CRS, supply an explicit projected --metric-crs in metres.")
            factor = input_crs.axis_info[0].unit_conversion_factor
            if factor is None or not math.isclose(float(factor), 1.0, rel_tol=0, abs_tol=1e-9):
                raise ValueError("Input CRS is not in metres. Supply --metric-crs as a projected CRS in metres.")
            metric_crs = input_crs
    else:
        metric_crs = CRS.from_user_input(metric_crs_text)

    if not metric_crs.is_projected:
        raise ValueError(f"Metric CRS must be projected, not geographic: {metric_crs.to_string()}")
    if not metric_crs.axis_info:
        raise ValueError(f"Cannot determine axis units for metric CRS: {metric_crs.to_string()}")
    factor = metric_crs.axis_info[0].unit_conversion_factor
    if factor is None or not math.isclose(float(factor), 1.0, rel_tol=0, abs_tol=1e-9):
        raise ValueError(
            f"Metric CRS {metric_crs.to_string()} does not use metres as coordinate units. "
            "Choose a projected CRS whose units are metres."
        )
    return metric_crs


def project_points(df: pd.DataFrame, input_crs: CRS, metric_crs: CRS) -> pd.DataFrame:
    tx = Transformer.from_crs(input_crs, metric_crs, always_xy=True)
    xx, yy = tx.transform(df["x_input"].to_numpy(dtype=float), df["y_input"].to_numpy(dtype=float))
    result = df.copy()
    result["x_m"] = np.asarray(xx, dtype=float)
    result["y_m"] = np.asarray(yy, dtype=float)
    ok = np.isfinite(result["x_m"]) & np.isfinite(result["y_m"])
    result = result.loc[ok].copy()
    if result.empty:
        raise ValueError("All points became invalid during CRS transformation; check --input-crs and coordinates.")
    return result


def build_tracks(points: pd.DataFrame) -> dict[str, dict[str, Any]]:
    tracks: dict[str, dict[str, Any]] = {}
    for tid, raw_group in points.groupby("trajectory_id", sort=True):
        # Collapse duplicate timestamps to their mean coordinate so interpolation is well-defined.
        g = (raw_group.groupby("t", as_index=False)
             .agg(x_m=("x_m", "mean"), y_m=("y_m", "mean"))
             .sort_values("t"))
        if len(g) < 2:
            continue
        ts = g["t"].to_numpy(dtype=float)
        xs = g["x_m"].to_numpy(dtype=float)
        ys = g["y_m"].to_numpy(dtype=float)
        if ts[-1] <= ts[0]:
            continue
        coords = list(zip(xs.tolist(), ys.tolist()))
        line = LineString(coords)
        tracks[str(tid)] = {
            "id": str(tid), "t": ts, "x": xs, "y": ys,
            "start": float(ts[0]), "end": float(ts[-1]),
            "duration": float(ts[-1] - ts[0]),
            "minx": float(np.min(xs)), "maxx": float(np.max(xs)),
            "miny": float(np.min(ys)), "maxy": float(np.max(ys)),
            "line": line, "n_points": int(len(g)),
        }
    return tracks


def bounds_distance(a: dict[str, Any], b: dict[str, Any]) -> float:
    dx = max(0.0, b["minx"] - a["maxx"], a["minx"] - b["maxx"])
    dy = max(0.0, b["miny"] - a["maxy"], a["miny"] - b["maxy"])
    return math.hypot(dx, dy)


def interp_track(track: dict[str, Any], grid: np.ndarray, max_gap_s: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Interpolate on grid and return x, y, valid mask. Never extrapolates or bridges long gaps."""
    t = track["t"]
    n = len(t)
    right = np.searchsorted(t, grid, side="left")
    in_right = right < n
    right_safe = np.minimum(right, n - 1)
    exact = in_right & (t[right_safe] == grid)
    left = right - 1
    bracketed = (left >= 0) & in_right
    valid = (grid >= t[0]) & (grid <= t[-1]) & exact
    idx = np.where(bracketed)[0]
    if len(idx):
        gaps = t[right[idx]] - t[left[idx]]
        valid[idx] |= gaps <= max_gap_s

    # np.interp returns endpoint values outside the input span; the mask invalidates them.
    x = np.interp(grid, t, track["x"])
    y = np.interp(grid, t, track["y"])
    return x, y, valid


def sample_grid(start: float, end: float, step_s: float) -> np.ndarray:
    if end <= start:
        return np.asarray([], dtype=float)
    grid = np.arange(start, end, step_s, dtype=float)
    if len(grid) == 0:
        grid = np.asarray([start], dtype=float)
    if not np.isclose(grid[-1], end, atol=1e-6, rtol=0):
        grid = np.append(grid, end)
    return grid


def compare_pair(a: dict[str, Any], b: dict[str, Any]) -> tuple[dict[str, Any], dict[str, np.ndarray] | None]:
    start = max(a["start"], b["start"])
    end = min(a["end"], b["end"])
    overlap = max(0.0, end - start)
    a_cov = overlap / a["duration"] if a["duration"] > 0 else 0.0
    b_cov = overlap / b["duration"] if b["duration"] > 0 else 0.0
    row: dict[str, Any] = {
        "A_id": a["id"], "B_id": b["id"],
        "overlap_seconds": overlap,
        "A_time_coverage": a_cov,
        "B_time_coverage": b_cov,
        "valid_sample_coverage": 0.0,
        "n_valid_samples": 0,
        "mean_distance_m": np.nan,
        "median_distance_m": np.nan,
        "p95_distance_m": np.nan,
        "max_distance_m": np.nan,
        "rmse_distance_m": np.nan,
        "match_score": np.nan,
        "eligible": False,
        "reason": "",
    }
    reasons: list[str] = []
    if overlap < MIN_OVERLAP_SECONDS:
        reasons.append("insufficient_temporal_overlap")
    if a_cov < MIN_A_TIME_COVERAGE:
        reasons.append("low_A_time_coverage")

    samples: dict[str, np.ndarray] | None = None
    if overlap > 0:
        grid = sample_grid(start, end, RESAMPLE_SECONDS)
        if len(grid):
            ax, ay, av = interp_track(a, grid, MAX_INTERP_GAP_SECONDS)
            bx, by, bv = interp_track(b, grid, MAX_INTERP_GAP_SECONDS)
            valid = av & bv
            n_valid = int(np.count_nonzero(valid))
            row["n_valid_samples"] = n_valid
            row["valid_sample_coverage"] = float(n_valid / len(grid)) if len(grid) else 0.0
            if n_valid:
                distances = np.hypot(ax[valid] - bx[valid], ay[valid] - by[valid])
                mean_d = float(np.mean(distances))
                median_d = float(np.median(distances))
                p95_d = float(np.percentile(distances, 95))
                max_d = float(np.max(distances))
                rmse_d = float(np.sqrt(np.mean(np.square(distances))))
                row.update({
                    "mean_distance_m": mean_d,
                    "median_distance_m": median_d,
                    "p95_distance_m": p95_d,
                    "max_distance_m": max_d,
                    "rmse_distance_m": rmse_d,
                })
                if mean_d > MAX_MEAN_DISTANCE_M:
                    reasons.append("mean_distance_over_limit")
                if p95_d > MAX_P95_DISTANCE_M:
                    reasons.append("p95_distance_over_limit")
                # Score is normalized to the user-set distance gates; lower is better.
                row["match_score"] = (
                    0.55 * min(mean_d / MAX_MEAN_DISTANCE_M, 1.0) +
                    0.35 * min(p95_d / MAX_P95_DISTANCE_M, 1.0) +
                    0.10 * (1.0 - min(a_cov, 1.0))
                )
                samples = {
                    "t": grid[valid], "ax": ax[valid], "ay": ay[valid],
                    "bx": bx[valid], "by": by[valid], "distance": distances,
                }
            if n_valid < MIN_SAMPLES:
                reasons.append("too_few_valid_samples")
            if row["valid_sample_coverage"] < MIN_VALID_SAMPLE_COVERAGE:
                reasons.append("low_interpolation_coverage")
    else:
        reasons.append("no_temporal_overlap")

    # Keep reason names unique and in a stable order.
    reasons = list(dict.fromkeys(reasons))
    row["eligible"] = len(reasons) == 0
    row["reason"] = "eligible" if row["eligible"] else ";".join(reasons)
    return row, samples


def assign_one_to_one(a_tracks: dict[str, dict[str, Any]], b_tracks: dict[str, dict[str, Any]],
                      candidates: list[dict[str, Any]], unmatched_cost: float = 1.00001) -> dict[str, str]:
    """Sparse global one-to-one assignment with a dummy unmatched choice for every A track."""
    a_ids = list(a_tracks)
    b_ids = list(b_tracks)
    if not a_ids or not b_ids:
        return {}
    a_idx = {tid: i for i, tid in enumerate(a_ids)}
    b_idx = {tid: i for i, tid in enumerate(b_ids)}

    rows: list[int] = []
    cols: list[int] = []
    vals: list[float] = []
    for c in candidates:
        if not c["eligible"]:
            continue
        rows.append(a_idx[c["A_id"]])
        cols.append(b_idx[c["B_id"]])
        vals.append(max(float(c["match_score"]), 0.0) + 1e-8) # sparse solver treats explicit zero as absent
    for i in range(len(a_ids)):
        rows.append(i)
        cols.append(len(b_ids) + i)
        vals.append(unmatched_cost)

    cost = coo_matrix((np.asarray(vals), (np.asarray(rows), np.asarray(cols))),
                      shape=(len(a_ids), len(b_ids) + len(a_ids))).tocsr()
    rr, cc = min_weight_full_bipartite_matching(cost)
    mapping: dict[str, str] = {}
    for r, c in zip(rr, cc):
        if c < len(b_ids):
            mapping[a_ids[int(r)]] = b_ids[int(c)]
    return mapping


def utc_string(seconds: np.ndarray | float) -> Any:
    return pd.to_datetime(seconds, unit="s", utc=True).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_point_gdf(points: pd.DataFrame, crs: CRS) -> gpd.GeoDataFrame:
    cols = ["dataset", "trajectory_id", "timestamp_utc", "x_input", "y_input", "x_m", "y_m"]
    out = points[cols].copy()
    out = out.rename(columns={"trajectory_id": "trajectory_id", "timestamp_utc": "timestamp_utc"})
    return gpd.GeoDataFrame(out, geometry=gpd.points_from_xy(out["x_m"], out["y_m"]), crs=crs)


def track_line_gdf(tracks: dict[str, dict[str, Any]], ds_name: str, crs: CRS,
                   match_map: dict[str, str], matched_records: dict[tuple[str, str], dict[str, Any]],
                   inverse: bool = False) -> gpd.GeoDataFrame:
    rows = []
    for tid, tr in tracks.items():
        if not inverse:
            other = match_map.get(tid)
            match = matched_records.get((tid, other)) if other is not None else None
            rows.append({
                "dataset": ds_name, "trajectory_id": tid,
                "match_id": other or "", "match_status": "matched" if other else "unmatched",
                "mean_dist_m": float(match["mean_distance_m"]) if match else np.nan,
                "p95_dist_m": float(match["p95_distance_m"]) if match else np.nan,
                "overlap_s": float(match["overlap_seconds"]) if match else np.nan,
                "n_points": tr["n_points"], "geometry": tr["line"],
            })
        else:
            other = match_map.get(tid)
            match = matched_records.get((other, tid)) if other is not None else None
            rows.append({
                "dataset": ds_name, "trajectory_id": tid,
                "match_id": other or "", "match_status": "matched" if other else "unmatched",
                "mean_dist_m": float(match["mean_distance_m"]) if match else np.nan,
                "p95_dist_m": float(match["p95_distance_m"]) if match else np.nan,
                "overlap_s": float(match["overlap_seconds"]) if match else np.nan,
                "n_points": tr["n_points"], "geometry": tr["line"],
            })
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)


def write_gpkg_layers(gpkg: Path, layers: list[tuple[str, gpd.GeoDataFrame]]) -> None:
    if gpkg.exists():
        gpkg.unlink()
    for layer_name, frame in layers:
        if frame is None or frame.empty:
            continue
        mode = "w" if not gpkg.exists() else "a"
        frame.to_file(gpkg, layer=layer_name, driver="GPKG", mode=mode, index=False)


def write_graphs(out_dir: Path, matched: pd.DataFrame,
                 series_by_pair: dict[tuple[str, str], dict[str, np.ndarray]],
                 max_plot_matches: int) -> None:
    # Distribution of all aligned point-to-point distances for selected trajectory matches.
    fig, ax = plt.subplots(figsize=(8, 5))
    vals = np.concatenate([s["distance"] for s in series_by_pair.values()]) if series_by_pair else np.asarray([])
    if len(vals):
        ax.hist(vals, bins=30)
        ax.axvline(np.median(vals), linestyle="--", label=f"Median {np.median(vals):.1f} m")
        ax.axvline(np.mean(vals), linestyle=":", label=f"Mean {np.mean(vals):.1f} m")
        ax.legend()
    else:
        ax.text(0.5, 0.5, "No matched trajectories with valid comparison samples", ha="center", va="center")
        ax.set_axis_off()
    ax.set_title("Distance distribution in matched trajectories")
    ax.set_xlabel("Distance between video and GNSS")
    ax.set_ylabel("Number of datapoint pairs")
    fig.tight_layout()
    fig.savefig(out_dir / "distance_distribution.png", dpi=160)
    plt.close(fig)

    # Normalized time profiles make tracks of different duration comparable.
    # Produce a clean version and a second version with small A-ID labels at line ends.
    profile_grid = np.linspace(0.0, 1.0, 101)
    ranked = matched.sort_values("mean_distance_m", ascending=False)
    keep = set((str(r.A_id), str(r.B_id)) for r in ranked.head(max_plot_matches).itertuples())
    time_profiles: list[tuple[tuple[str, str], np.ndarray]] = []
    # Distance-based profiles use the cumulative path length travelled by A (video
    # trajectory) over the shared samples as the progress axis.
    dist_profiles: list[tuple[tuple[str, str], np.ndarray]] = []
    if series_by_pair:
        for key, s in series_by_pair.items():
            if key not in keep:
                continue
            ts = s["t"]
            if len(ts) < 2 or ts[-1] <= ts[0]:
                continue
            rel = (ts - ts[0]) / (ts[-1] - ts[0])
            time_profiles.append((key, np.interp(profile_grid, rel, s["distance"])))

            steps = np.hypot(np.diff(s["ax"]), np.diff(s["ay"]))
            cum = np.concatenate([[0.0], np.cumsum(steps)])
            if cum[-1] <= EPS:
                continue
            # Drop samples where the vehicle did not move so the x-axis is strictly increasing.
            inc = np.concatenate([[True], np.diff(cum) > EPS])
            rel_d = cum[inc] / cum[-1]
            if len(rel_d) < 2:
                continue
            dist_profiles.append((key, np.interp(profile_grid, rel_d, s["distance"][inc])))

    def draw_profiles(plotted_profiles: list[tuple[tuple[str, str], np.ndarray]],
                      by_distance: bool, labeled: bool, filename: str) -> None:
        fig, ax = plt.subplots(figsize=(10 if labeled else 9, 5.5))
        if plotted_profiles:
            all_profiles = []
            for (a_id, b_id), profile in plotted_profiles:
                all_profiles.append(profile)
                ax.plot(profile_grid * 100, profile, alpha=0.32, linewidth=0.9)
                if labeled:
                    # Label the A trajectory at the right edge; tiny text keeps the
                    # second plot useful when many matched pairs are shown.
                    ax.annotate(
                        f"A {a_id}",
                        xy=(100, float(profile[-1])),
                        xytext=(4, 0), textcoords="offset points",
                        fontsize=2, alpha=0.85, va="center", clip_on=False,
                    )
            ax.plot(profile_grid * 100, np.mean(np.vstack(all_profiles), axis=0),
                    color="black", linewidth=2.3,
                    label=f"Mean profile ({len(all_profiles)} pairs)")
            ax.legend(loc="upper left", fontsize=8)
            if labeled:
                ax.set_xlim(0, 108)
        else:
            ax.text(0.5, 0.5, "No matched trajectories with valid comparison samples",
                    ha="center", va="center")
            ax.set_axis_off()
        what = "normalized travelled distance" if by_distance else "normalized travelled time"
        ax.set_title(f"Error profile over {what}" +
                     (" — A trajectory IDs labeled" if labeled else ""))
        ax.set_xlabel("Progress along the travelled path (%)" if by_distance
                      else "Progress through the travelled time (%)")
        ax.set_ylabel("Error (m)")
        fig.tight_layout()
        fig.savefig(out_dir / filename, dpi=300 if labeled else 200)
        plt.close(fig)

    draw_profiles(time_profiles, False, False, "distance_profiles.png")
    draw_profiles(time_profiles, False, True, "distance_profiles_labeled.png")
    draw_profiles(dist_profiles, True, False, "distance_profiles_by_distance.png")
    draw_profiles(dist_profiles, True, True, "distance_profiles_by_distance_labeled.png")

    # A compact view of pair-level means vs upper-tail error.
    fig, ax = plt.subplots(figsize=(8, 5.5))
    if len(matched):
        ax.scatter(matched["mean_distance_m"], matched["p95_distance_m"])
        top = matched.nlargest(min(10, len(matched)), "p95_distance_m")
        if SCATTER_LABEL_ON_ALL:
            for r in matched.itertuples():
                ax.annotate(f"{r.A_id} ↔ {r.B_id}", (r.mean_distance_m, r.p95_distance_m),
                            xytext=(4, 3), textcoords="offset points", fontsize=6)
        else:
            for r in top.itertuples():
                ax.annotate(f"{r.A_id} ↔ {r.B_id}", (r.mean_distance_m, r.p95_distance_m),
                            xytext=(4, 3), textcoords="offset points", fontsize=7)
    else:
        ax.text(0.5, 0.5, "No matched trajectories", ha="center", va="center")
        ax.set_axis_off()
    ax.set_title("Pair-level distance quality")
    ax.set_xlabel("Mean distance per matched pair (m)")
    ax.set_ylabel("95th-percentile distance per matched pair (m)")
    fig.tight_layout()
    fig.savefig(out_dir / "pair_distance_quality.png", dpi=300)
    plt.close(fig)

    # Show each matched pair's aligned sample distances as one box.
    box_data = [s["distance"] for s in series_by_pair.values() if len(s["distance"])]
    pair_labels = [f"{a_id} ↔ {b_id}" for (a_id, b_id), s in series_by_pair.items()
                   if len(s["distance"])]
    if box_data:
        all_distances = np.concatenate(box_data)
        data_min = float(np.min(all_distances))
        data_max = float(np.max(all_distances))
        data_span = data_max - data_min
        scale = max(abs(data_min), abs(data_max), 1.0)
        display_span = data_span if data_span > 0 else max(scale * 0.01, 0.01)
        min_box_height = display_span * 0.01
        stats = []
        for label, distances in zip(pair_labels, box_data):
            q1, median, q3 = np.percentile(distances, [25, 50, 75])
            if q3 - q1 < min_box_height:
                q1 = median - min_box_height / 2
                q3 = median + min_box_height / 2
            stats.append({
                "label": label,
                "q1": float(q1),
                "med": float(median),
                "q3": float(q3),
                "whislo": min(float(np.min(distances)), float(q1)),
                "whishi": max(float(np.max(distances)), float(q3)),
                "fliers": [],
            })
        stats.sort(key=lambda stat: stat["med"])
        positions = np.arange(len(stats)) * 0.55 + 1
        x_limits = (positions[0] - 0.3, positions[-1] + 0.3)
        y_margin = max(display_span * 0.05, np.finfo(float).eps * scale * 10)
        y_limits = (data_min - y_margin, data_max + y_margin)
    else:
        stats = []
        positions = np.asarray([])
        x_limits = (0, 1)
        y_limits = None

    for show_labels, filename in (
        (True, "pair_distance_boxplot.png"),
        (False, "pair_distance_boxplot_no_labels.png"),
    ):
        fig_width = max(6, min(18, 0.13 * len(box_data)))
        fig, ax = plt.subplots(figsize=(fig_width, 4.8))
        if stats:
            boxplot = ax.bxp(stats, positions=positions, widths=0.46, showfliers=False,
                             patch_artist=True)
            ax.legend(
                [boxplot["boxes"][0], boxplot["medians"][0], boxplot["whiskers"][0]],
                ["25th-75th percentile", "Median", "Range"],
                loc="upper left", fontsize=7,
            )
            ax.set_xlim(*x_limits)
            ax.set_ylim(*y_limits)
            ax.set_xticks(positions)
            if show_labels:
                ax.set_xticklabels([stat["label"] for stat in stats], rotation=90, fontsize=6)
            else:
                ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)
            ax.set_ylabel("Error (m)")
        else:
            ax.text(0.5, 0.5, "No matched trajectories with valid comparison samples",
                    ha="center", va="center")
            ax.set_axis_off()
        ax.set_title("Per-trajectory error distributions")
        ax.set_xlabel("GNSS trajectory")
        fig.tight_layout()
        fig.savefig(out_dir / filename, dpi=300)
        plt.close(fig)

    # Each exact B - A offset contributes only once to the relative-position graphs.
    if series_by_pair:
        dx = np.concatenate([s["bx"] - s["ax"] for s in series_by_pair.values()])
        dy = np.concatenate([s["by"] - s["ay"] for s in series_by_pair.values()])
        offsets = np.unique(np.column_stack((dx, dy)), axis=0)
        dx, dy = offsets[:, 0], offsets[:, 1]
    else:
        dx = dy = np.asarray([])
    for with_points, filename in ((False, "relative_position_kde.png"),
                                  (True, "relative_position_kde_points.png")):
        fig = plt.figure(figsize=(7.5, 7.5))
        grid = fig.add_gridspec(2, 2, width_ratios=(5, 1), height_ratios=(1, 5),
                                left=0.1, right=0.97, bottom=0.08, top=0.9, wspace=0.04, hspace=0.04)
        ax = fig.add_subplot(grid[1, 0])
        ax_top = fig.add_subplot(grid[0, 0], sharex=ax)
        ax_right = fig.add_subplot(grid[1, 1], sharey=ax)
        drawn = False
        if len(dx):
            lim = float(np.percentile(np.abs(np.concatenate([dx, dy])), KDE_VIEW_PERCENTILE)) * 1.1
            lim = lim if lim > EPS else 1.0
        if len(dx) > 2:
            try:
                from scipy.stats import gaussian_kde
                kde = gaussian_kde(np.vstack([dx, dy]))
                axis_vals = np.linspace(-lim, lim, KDE_GRID_SIZE)
                gx, gy = np.meshgrid(axis_vals, axis_vals)
                dens = kde(np.vstack([gx.ravel(), gy.ravel()])).reshape(gx.shape)
                # Colors are assigned per contour level (BoundaryNorm), not linearly in density;
                # otherwise log-spaced levels all land in the near-transparent end of the colormap.
                base = plt.get_cmap(KDE_CMAP)(np.linspace(0.15, 1.0, 256))
                base[:, 3] = np.linspace(KDE_MIN_ALPHA, KDE_MAX_ALPHA, 256)
                kde_cmap = matplotlib.colors.ListedColormap(base)
                levels = dens.max() * np.geomspace(KDE_MIN_LEVEL_FRACTION, 1.0, KDE_N_LEVELS)
                norm = matplotlib.colors.BoundaryNorm(levels, kde_cmap.N)
                ax.contourf(gx, gy, dens, levels=levels, cmap=kde_cmap, norm=norm, zorder=2)

                # Marginal densities: integrate the 2D density along the other axis.
                step = axis_vals[1] - axis_vals[0]
                marg_x = dens.sum(axis=0) * step
                marg_y = dens.sum(axis=1) * step
                line_color = base[-1, :3]
                fill_color = base[len(base) // 2, :3]
                ax_top.fill_between(axis_vals, marg_x, color=fill_color, alpha=0.5)
                ax_top.plot(axis_vals, marg_x, color=line_color, linewidth=1.5)
                ax_right.fill_betweenx(axis_vals, marg_y, color=fill_color, alpha=0.5)
                ax_right.plot(marg_y, axis_vals, color=line_color, linewidth=1.5)
                ax_top.set_ylim(bottom=0)
                ax_right.set_xlim(left=0)
                drawn = True
            except (np.linalg.LinAlgError, ValueError):
                ax.text(0.5, 0.5, "KDE could not be computed (degenerate data)",
                        transform=ax.transAxes, ha="center", va="center")
        elif len(dx):
            ax.text(0.5, 0.95, "Not enough unique offsets for KDE",
                    transform=ax.transAxes, ha="center", va="top")
        if len(dx):
            if with_points:
                ax.scatter(dx, dy, s=KDE_POINT_SIZE, color=KDE_POINT_COLOR, edgecolors="none",
                           alpha=KDE_POINT_ALPHA, zorder=1)
            ax.set_xlim(-lim, lim)
            ax.set_ylim(-lim, lim)
            ax.axhline(0, color="black", linewidth=0.6, alpha=0.5)
            ax.axvline(0, color="black", linewidth=0.6, alpha=0.5)
            ax.plot(0, 0, "k+", markersize=12)
        if not drawn:
            ax_top.set_axis_off()
            ax_right.set_axis_off()
        if not len(dx):
            ax.text(0.5, 0.5, "Not enough matched samples", ha="center", va="center")
            ax.set_axis_off()
        ax_top.tick_params(labelbottom=False, labelleft=False, left=False)
        ax_right.tick_params(labelleft=False, labelbottom=False, bottom=False)
        fig.suptitle("Relative position of GNSS point vs. its paired video point" +
                     (" (with individual points)" if with_points else ""), fontsize=11)
        ax.set_xlabel("East offset (m)")
        ax.set_ylabel("North offset (m)")
        fig.savefig(out_dir / filename, dpi=300)
        plt.close(fig)


def main() -> int:
    if RESAMPLE_SECONDS <= 0 or MAX_INTERP_GAP_SECONDS <= 0:
        raise ValueError("RESAMPLE_SECONDS and MAX_INTERP_GAP_SECONDS must be positive.")
    if MIN_OVERLAP_SECONDS < 0 or COARSE_DISTANCE_M < 0:
        raise ValueError("Minimum overlap and coarse distance must be non-negative.")
    if MAX_MEAN_DISTANCE_M <= 0 or MAX_P95_DISTANCE_M <= 0:
        raise ValueError("Distance thresholds must be positive.")
    if not (0 <= MIN_A_TIME_COVERAGE <= 1 and 0 <= MIN_VALID_SAMPLE_COVERAGE <= 1):
        raise ValueError("Coverage thresholds must be between 0 and 1.")
    if MIN_SAMPLES < 1:
        raise ValueError("MIN_SAMPLES must be at least 1.")

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    a_raw, a_dropped = read_csv_normalized(
        DATASET_A_CSV, "A", A_ID_COLUMN, A_TIME_COLUMN, A_X_COLUMN, A_Y_COLUMN
    )
    b_raw, b_dropped = read_csv_normalized(
        DATASET_B_CSV, "B", B_ID_COLUMN, B_TIME_COLUMN, B_X_COLUMN, B_Y_COLUMN
    )
    input_crs = CRS.from_user_input(INPUT_CRS)
    metric_crs = choose_metric_crs(INPUT_CRS, METRIC_CRS, a_raw, b_raw)
    a_pts = project_points(a_raw, input_crs, metric_crs)
    b_pts = project_points(b_raw, input_crs, metric_crs)

    a_tracks = build_tracks(a_pts)
    b_tracks = build_tracks(b_pts)
    for name, tracks in [("A", a_tracks), ("B", b_tracks)]:
        starts = [tr["start"] for tr in tracks.values()]
        ends = [tr["end"] for tr in tracks.values()]

        print(f"\nDataset {name}")
        print("Number of tracks:", len(tracks))
        print("Overall start (Unix seconds):", min(starts))
        print("Overall end   (Unix seconds):", max(ends))
        print("Overall span (seconds):", max(ends) - min(starts))
        print("Example track:", next(iter(tracks.values()))["id"])
        print("Example track duration:", next(iter(tracks.values()))["duration"])
    if not a_tracks:
        raise ValueError("Dataset A has no trajectories with at least two distinct valid timestamps.")
    if not b_tracks:
        raise ValueError("Dataset B has no trajectories with at least two distinct valid timestamps.")

    # Sorted starts avoid considering B tracks that start after a given A track ends.
    b_ids_sorted = sorted(b_tracks, key=lambda tid: b_tracks[tid]["start"])
    b_starts_sorted = np.asarray([b_tracks[tid]["start"] for tid in b_ids_sorted], dtype=float)
    candidate_rows: list[dict[str, Any]] = []
    coarse_a: set[str] = set()
    coarse_b: set[str] = set()

    for aid, atr in a_tracks.items():
        upto = int(np.searchsorted(b_starts_sorted, atr["end"], side="left"))
        for bid in b_ids_sorted[:upto]:
            btr = b_tracks[bid]
            overlap = min(atr["end"], btr["end"]) - max(atr["start"], btr["start"])
            if overlap <= 0:
                continue
            if bounds_distance(atr, btr) > COARSE_DISTANCE_M:
                continue
            coarse_a.add(aid)
            coarse_b.add(bid)
            row, _ = compare_pair(atr, btr)
            row["bbox_separation_m"] = bounds_distance(atr, btr)
            candidate_rows.append(row)

    # Use sparse optimal assignment so each B trajectory can be used at most once.
    mapping = assign_one_to_one(a_tracks, b_tracks, candidate_rows)
    inverse_mapping = {bid: aid for aid, bid in mapping.items()}
    candidate_lookup = {(str(r["A_id"]), str(r["B_id"])): r for r in candidate_rows if r["eligible"]}
    matched_records = {(aid, bid): candidate_lookup[(aid, bid)] for aid, bid in mapping.items()}

    match_rows = []
    for aid, bid in mapping.items():
        row = dict(matched_records[(aid, bid)])
        row["assignment"] = "one_to_one_optimal"
        match_rows.append(row)
    matches = pd.DataFrame(match_rows)
    if matches.empty:
        matches = pd.DataFrame(columns=[
            "A_id", "B_id", "overlap_seconds", "A_time_coverage", "B_time_coverage",
            "valid_sample_coverage", "n_valid_samples", "mean_distance_m", "median_distance_m",
            "p95_distance_m", "max_distance_m", "rmse_distance_m", "match_score", "eligible", "reason"
        ])

    candidate_df = pd.DataFrame(candidate_rows)
    if not candidate_df.empty:
        candidate_df = candidate_df.sort_values(["eligible", "match_score"], ascending=[False, True], na_position="last")
    candidate_df.to_csv(out_dir / "candidate_pairs.csv", index=False)
    matches.to_csv(out_dir / "matches.csv", index=False)

    unmatched_a_rows = []
    for aid, atr in a_tracks.items():
        if aid in mapping:
            continue
        n_rows = sum(1 for r in candidate_rows if r["A_id"] == aid)
        n_ok = sum(1 for r in candidate_rows if r["A_id"] == aid and r["eligible"])
        reason = ("eligible_candidate_assigned_to_other_A" if n_ok else
                  "no_candidate_passed_thresholds" if n_rows else
                  "no_temporal_and_spatial_candidate")
        unmatched_a_rows.append({"A_id": aid, "start_utc": utc_string(atr["start"]),
                                 "end_utc": utc_string(atr["end"]), "duration_seconds": atr["duration"],
                                 "n_candidate_pairs": n_rows, "n_eligible_candidates": n_ok, "reason": reason})
    unmatched_a = pd.DataFrame(unmatched_a_rows, columns=["A_id", "start_utc", "end_utc", "duration_seconds",
                                                        "n_candidate_pairs", "n_eligible_candidates", "reason"])
    unmatched_a.to_csv(out_dir / "unmatched_A.csv", index=False)

    unmatched_b_rows = []
    for bid, btr in b_tracks.items():
        if bid in inverse_mapping:
            continue
        n_rows = sum(1 for r in candidate_rows if r["B_id"] == bid)
        n_ok = sum(1 for r in candidate_rows if r["B_id"] == bid and r["eligible"])
        reason = ("eligible_candidate_not_selected_in_one_to_one_assignment" if n_ok else
                  "no_candidate_passed_thresholds" if n_rows else
                  "no_temporal_and_spatial_candidate")
        unmatched_b_rows.append({"B_id": bid, "start_utc": utc_string(btr["start"]),
                                 "end_utc": utc_string(btr["end"]), "duration_seconds": btr["duration"],
                                 "n_candidate_pairs": n_rows, "n_eligible_candidates": n_ok, "reason": reason})
    unmatched_b = pd.DataFrame(unmatched_b_rows, columns=["B_id", "start_utc", "end_utc", "duration_seconds",
                                                        "n_candidate_pairs", "n_eligible_candidates", "reason"])
    unmatched_b.to_csv(out_dir / "unmatched_B.csv", index=False)

    # Recompute selected pairs and retain their aligned samples for CSV, GIS and plots.
    ts_rows: list[dict[str, Any]] = []
    vector_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    series_by_pair: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    for aid, bid in mapping.items():
        _, s = compare_pair(a_tracks[aid], b_tracks[bid])
        if s is None or not len(s["distance"]):
            continue
        series_by_pair[(aid, bid)] = s
        times = pd.to_datetime(s["t"], unit="s", utc=True).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for k, (tstamp, dist) in enumerate(zip(times, s["distance"])):
            ax, ay, bx, by = float(s["ax"][k]), float(s["ay"][k]), float(s["bx"][k]), float(s["by"][k])
            ts_rows.append({"A_id": aid, "B_id": bid, "timestamp_utc": tstamp,
                            "distance_m": float(dist), "A_x_m": ax, "A_y_m": ay, "B_x_m": bx, "B_y_m": by})
            vector_rows.append({"A_id": aid, "B_id": bid, "timestamp_utc": tstamp,
                                "distance_m": float(dist), "geometry": LineString([(ax, ay), (bx, by)])})
            sample_rows.append({"A_id": aid, "B_id": bid, "timestamp_utc": tstamp,
                                "distance_m": float(dist), "geometry": Point((ax + bx) / 2.0, (ay + by) / 2.0)})

    ts_columns = ["A_id", "B_id", "timestamp_utc", "distance_m", "A_x_m", "A_y_m", "B_x_m", "B_y_m"]
    ts_df = pd.DataFrame(ts_rows, columns=ts_columns)
    ts_df.to_csv(out_dir / "distance_timeseries.csv", index=False)

    crs = metric_crs
    a_line = track_line_gdf(a_tracks, "A", crs, mapping, matched_records, inverse=False)
    b_line = track_line_gdf(b_tracks, "B", crs, inverse_mapping, matched_records, inverse=True)
    a_point_gdf = make_point_gdf(a_pts, crs)
    b_point_gdf = make_point_gdf(b_pts, crs)
    vectors_gdf = gpd.GeoDataFrame(vector_rows, geometry="geometry", crs=crs) if vector_rows else gpd.GeoDataFrame(
        {"A_id": [], "B_id": [], "timestamp_utc": [], "distance_m": []}, geometry=gpd.GeoSeries([], crs=crs), crs=crs)
    samples_gdf = gpd.GeoDataFrame(sample_rows, geometry="geometry", crs=crs) if sample_rows else gpd.GeoDataFrame(
        {"A_id": [], "B_id": [], "timestamp_utc": [], "distance_m": []}, geometry=gpd.GeoSeries([], crs=crs), crs=crs)

    gpkg = out_dir / "trajectory_comparison.gpkg"
    write_gpkg_layers(gpkg, [
        ("A_points", a_point_gdf), ("B_points", b_point_gdf),
        ("A_trajectories", a_line), ("B_trajectories", b_line),
        ("distance_vectors", vectors_gdf), ("distance_samples", samples_gdf),
    ])
    write_graphs(out_dir, matches, series_by_pair, MAX_PLOT_MATCHES)

    matched_means = matches["mean_distance_m"].dropna().astype(float) if len(matches) else pd.Series(dtype=float)
    sample_dist = ts_df["distance_m"].astype(float) if len(ts_df) else pd.Series(dtype=float)
    summary = {
        "input_A": str(Path(DATASET_A_CSV).resolve()),
        "input_B": str(Path(DATASET_B_CSV).resolve()),
        "input_crs": input_crs.to_string(),
        "metric_crs": metric_crs.to_string(),
        "input_rows_A_dropped_as_invalid": int(a_dropped),
        "input_rows_B_dropped_as_invalid": int(b_dropped),
        "valid_point_rows_A": int(len(a_pts)),
        "valid_point_rows_B": int(len(b_pts)),
        "trajectories_A": int(len(a_tracks)),
        "trajectories_B": int(len(b_tracks)),
        "coarse_candidate_pairs": int(len(candidate_rows)),
        "eligible_candidate_pairs": int(sum(bool(r["eligible"]) for r in candidate_rows)),
        "matched_pairs": int(len(mapping)),
        "unmatched_A": int(len(unmatched_a)),
        "unmatched_B": int(len(unmatched_b)),
        "distance_time_samples": int(len(ts_df)),
        "sample_distance_mean_m": float(sample_dist.mean()) if len(sample_dist) else None,
        "sample_distance_median_m": float(sample_dist.median()) if len(sample_dist) else None,
        "sample_distance_std_m": float(sample_dist.std()) if len(sample_dist) else None,
        "sample_distance_p95_m": float(sample_dist.quantile(0.95)) if len(sample_dist) else None,
        "mean_of_pair_mean_distance_m": float(matched_means.mean()) if len(matched_means) else None,
        "median_of_pair_mean_distance_m": float(matched_means.median()) if len(matched_means) else None,
        "parameters": {
            "resample_seconds": RESAMPLE_SECONDS,
            "max_interp_gap_seconds": MAX_INTERP_GAP_SECONDS,
            "min_overlap_seconds": MIN_OVERLAP_SECONDS,
            "min_A_time_coverage": MIN_A_TIME_COVERAGE,
            "min_valid_sample_coverage": MIN_VALID_SAMPLE_COVERAGE,
            "min_samples": MIN_SAMPLES,
            "coarse_distance_m": COARSE_DISTANCE_M,
            "max_mean_distance_m": MAX_MEAN_DISTANCE_M,
            "max_p95_distance_m": MAX_P95_DISTANCE_M,
        },
        "notes": [
            "Distances are planar Euclidean distances in the selected projected CRS, whose units are metres.",
            "Interpolation is linear and only performed within each track's observed time range.",
            "No interpolation is performed across source gaps longer than max_interp_gap_seconds.",
            "Matching is globally optimized one-to-one; unmatched trajectories are retained in separate CSV reports.",
            "Timestamps are parsed as UTC; timezone-naive input timestamps are interpreted as UTC.",
        ],
    }
    with open(out_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(json.dumps({k: summary[k] for k in ["metric_crs", "trajectories_A", "trajectories_B",
          "coarse_candidate_pairs", "eligible_candidate_pairs", "matched_pairs", "unmatched_A",
          "unmatched_B", "distance_time_samples", "sample_distance_median_m", "sample_distance_std_m", "sample_distance_p95_m"]}, indent=2))
    print(f"\nWrote results to: {out_dir.resolve()}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
