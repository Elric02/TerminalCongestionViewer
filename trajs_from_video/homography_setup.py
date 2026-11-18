"""
Manual homography setup for geo-trax georeferencing.
Produces the master frame PNG + cached homography .txt that geo-trax loads automatically
(master-frame usage is controlled by cfg -> georef -> processing -> use_master, or --no-master
on 'geotrax batch'/'geotrax georeference').
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path
import cv2
import numpy as np
import pandas as pd
import vidtraf_tracking_master.homography_tools as ht
sys.stdout.reconfigure(encoding='utf-8')

# Configuration
source           = ("T2", "Linköping", "Resecentrum", "Drone") # ("Project", "City", "Location", "Drone")
location_id      = "lkpg_resecentrum"
location_lonlat  = (15.62367059274179, 58.417174503195035)  # (lon, lat) for cropping orthophoto (center of cropped orthophoto)
subset_crop_size = 4000    # square crop in source pixels (≈1.28 km at 0.16 m/px)
ransac_threshold = 5.0     # only used to flag/color points for review — never gates what goes into the final H
max_avg_error    = 0.3     # target: least-squares fit's average reprojection error, in meters
max_point_error  = 0.5     # target: least-squares fit's worst single-point reprojection error, in meters
point_radius     = 2       # radius (px) of control-point markers
shared_ortho_points = {    # cross-drone shared control points: this drone's point number -> (other drone id, other drone's point number)
    # e.g. 1: ("D1", 8),
}
ref_frame        = 0       # must match geo-trax cfg/georef/default.yaml -> processing -> ref_frame
master_video     = 0       # 0 = closest video to mean hover position; 1, 2, ... = next-best
add_objects      = False   # open the interactive object-marking tool (add) on the ortho
edit_objects     = False   # open the interactive object-marking tool (modify/remove) on the ortho (drag/nudge/extend existing objects)
run_diagnostics  = False   # save a local-ground-scale heatmap for this drone's H (near-vs-far error falloff)
measure_points   = False   # click points on the master frame to spot-check real-world distance vs. an independent source

type_colors = {   # BGR, by object "type" (lane types shown here; any other type defaults to black)
    "lane": (255, 0, 0),                  # blue
    "turn_lane": (0, 255, 255),           # yellow
    "bike_lane": (0, 255, 0),             # green
    "sidewalk": (255, 255, 255),          # white
    "bus_lane": (255, 0, 255),            # magenta
    "intersection": (0, 0, 255),          # red
}
section_alphas = {   # fill opacity (polygons only), matched against "section" first, then as a substring of "type"
    "intersection": 0.2,
    "from_city": 0.2,
    "to_city": 0.6,
}

# MANUAL PATHS
drone_dir = Path("Bus_Trajectory-main/notebooks/modules/Tracking/runs/track_lkpg")
location_dir = Path("output/georef")
master_png = Path("output/frame_0.png")
ortho_dir = Path("test_ortofoto_lkpg")
ortho_tif_epsg4326 = ortho_dir / f"647_53_5050_2023_clipped.tif"
ortho_png = ortho_dir / f"647_53_5050_2023_clipped.png"

# %% Derived paths =====================================================================================================
drone_id = source[-1]
location_georef_dir = location_dir / "georef"
master_dir = drone_dir / "master_frames"
ortho_params_txt = location_georef_dir / "ortho_parameters.txt"
ortho_center_txt = location_georef_dir / f"{location_id}_center.txt"
ref_drone_png = master_png.with_stem(f"{master_png.stem}_ref_drone")
pts_ortho_npy = master_dir / f"{location_id}_pts_ortho.npy"
pts_drone_npy = master_dir / f"{location_id}_pts_drone.npy"
ref_ortho_png = master_png.with_stem(f"{ortho_png.stem}_ref_ortho")
homography_txt = master_dir / f"{location_id}.txt"
objects_json = location_georef_dir / f"{location_id}_objects.json"
objects_html = location_georef_dir / f"{location_id}_objects.html"
drone_objects_html = master_png.with_stem(f"{master_png.stem}_objects").with_suffix(".html")
ground_scale_html = master_png.with_stem(f"{master_png.stem}_ground_scale").with_suffix(".html")
measured_points_png = master_png.with_stem(f"{master_png.stem}_measured_points")
measured_points_json = measured_points_png.with_suffix(".json")
measured_points_ortho_png = master_dir / f"{location_id}_measured_points_ortho.png"
measured_points_ortho_json = measured_points_ortho_png.with_suffix(".json")
run_log_path = master_dir / f"{location_id}_run_log.json"
location_georef_dir.mkdir(parents=True, exist_ok=True)
master_dir.mkdir(parents=True, exist_ok=True)

# %% Load previous run log, if any =====================================================================================
if run_log_path.exists():
    prev_run_log = json.loads(run_log_path.read_text(encoding="utf-8"))
    print(f"Previous run log found: {run_log_path}")
    print(f"  location_lonlat used previously: {prev_run_log.get('location_lonlat')}")
    print(f"  ortho_gsd_m_per_px previously measured: {prev_run_log.get('ortho_gsd_m_per_px')}")

# %% Orthophoto ========================================================================================================
if not ortho_tif_epsg4326.exists():
    raise FileNotFoundError(f"{ortho_tif_epsg4326} not found — run ortho_setup.py for '{source[1]}' first.")

# Measure ortho ground sampling distance, at this location's latitude
ortho_gsd_m_per_px = ht.measure_ortho_gsd(ortho_tif_epsg4326, lonlat=location_lonlat)

# Subset orthophoto → PNG + metadata
if not ortho_png.exists():
    locations_json = location_georef_dir / "locations.json"
    # geo-trax's subset_orthophoto.py hardcodes [latitude, longitude] order for this file — swap here.
    locations_json.write_text(json.dumps({location_id: [location_lonlat[1], location_lonlat[0]]}))
    subprocess.run([
        sys.executable,
        r"C:\Users\GPCastro\repos\geo-trax\tools\subset_orthophoto.py",
        "--orthophoto-filepath", str(ortho_tif_epsg4326),
        "--ortho-cutout-folder", str(location_georef_dir),
        "--location-dict-filepath", str(locations_json),
        "--crop-size", str(subset_crop_size),
        "--scale-factor", "1.0",
    ], check=True)

# %% Object configuration (lanes, poles, signals, etc.) on the ortho image ============================================
if not objects_json.exists() and add_objects:
    objects = ht.mark_objects(ortho_png, objects_json)
    for obj in objects:
        lonlat = np.atleast_2d(ht.pixel_to_lonlat(obj["points"], ortho_params_txt, ortho_center_txt, subset_crop_size))
        obj["lonlat"] = [[float(lon), float(lat)] for lon, lat in lonlat]
    ht.save_json_list(objects, objects_json, "object(s)")
    ht.plot_objects(ortho_png, objects_json, objects_html, type_colors, section_alphas)
if edit_objects and objects_json.exists():
    objects = ht.refine_objects(ortho_png, None, objects_json, type_colors, section_alphas)
    for obj in objects:
        lonlat = np.atleast_2d(ht.pixel_to_lonlat(obj["points"], ortho_params_txt, ortho_center_txt, subset_crop_size))
        obj["lonlat"] = [[float(lon), float(lat)] for lon, lat in lonlat]
    ht.save_json_list(objects, objects_json, "object(s)")
    ht.plot_objects(ortho_png, objects_json, objects_html, type_colors, section_alphas)

# %% Create master frame ===============================================================================================
# Ranks videos by how close their GPS at ref_frame is to the mean hover position across all flights (best first).
# Requires CSVs generated from SRT files by drone_processing.py to already exist in drone_dir.
selected_video_name = None
if not master_png.exists():
    positions = []
    for csv_path in sorted(p for p in drone_dir.glob("*.csv")):
        df = pd.read_csv(csv_path)
        row = df[df['frame_idx'] == ref_frame]
        if row.empty or 'longitude' not in df.columns or 'latitude' not in df.columns:
            continue
        positions.append({
            'video': csv_path.with_suffix('.mp4'),
            'lon': row.iloc[0]['longitude'],
            'lat': row.iloc[0]['latitude'],
        })
    if not positions:
        raise RuntimeError(f"No valid CSV files found in {drone_dir}")
    mean_lon = sum(p['lon'] for p in positions) / len(positions)
    mean_lat = sum(p['lat'] for p in positions) / len(positions)
    ranked_videos = sorted(positions, key=lambda p: (p['lon'] - mean_lon) ** 2 + (p['lat'] - mean_lat) ** 2)

    print("Videos ranked by closeness to mean hover position (best first):")
    for rank, p in enumerate(ranked_videos):
        marker = "  <-- selected" if rank == master_video else ""
        print(f"  {rank}. {p['video'].name}{marker}")
    if master_video >= len(ranked_videos):
        raise IndexError(f"master_video={master_video} but only {len(ranked_videos)} video(s) available")
    selected_video = ranked_videos[master_video]['video']
    selected_video_name = selected_video.name

    cap = cv2.VideoCapture(str(selected_video))
    cap.set(cv2.CAP_PROP_POS_FRAMES, ref_frame)
    ret, frame = cap.read()
    cap.release()
    if not ret:
        raise RuntimeError(f"Failed to extract frame {ref_frame} from {selected_video.name}")
    cv2.imwrite(str(master_png), frame)
    print(f"Master frame saved: {master_png.name}")

# %% Control points ====================================================================================================
if pts_ortho_npy.exists():
    pts_ortho = np.load(str(pts_ortho_npy))
    print(f"{len(pts_ortho)} ortho control points loaded.")
else:
    if pts_drone_npy.exists() and ref_drone_png.exists():
        n_expected = len(np.load(str(pts_drone_npy)))
        print(f"Click the same {n_expected} points as in {ref_drone_png.name}, in the SAME ORDER.")
    pts_ortho = ht.pick_pixel_points(ortho_png, point_radius=point_radius)
    np.save(str(pts_ortho_npy), pts_ortho)
    ht.save_annotated_image(ortho_png, pts_ortho, ref_ortho_png)

if pts_drone_npy.exists():
    pts_drone = np.load(str(pts_drone_npy))
    print(f"{len(pts_drone)} drone control points loaded.")
else:
    if ref_ortho_png.exists():
        print(f"Click the same {len(pts_ortho)} points as in {ref_ortho_png.name}, in the SAME ORDER.")
    pts_drone = ht.pick_pixel_points(master_png, point_radius=point_radius)
    np.save(str(pts_drone_npy), pts_drone)
    ht.save_annotated_image(master_png, pts_drone, ref_drone_png)

n = len(pts_ortho)

# %% Sync shared control points across drones ==========================================================================
# Overwrites this drone's ortho point with the reference drone's
if shared_ortho_points:
    for this_point_number, (ref_drone_id, ref_point_number) in shared_ortho_points.items():
        this_idx = this_point_number - 1   # point numbers are 1-indexed; array positions are 0-indexed
        ref_idx = ref_point_number - 1
        if this_idx >= len(pts_ortho):
            raise IndexError(f"This drone has only {len(pts_ortho)} ortho point(s); point {this_point_number} out of range")
        ref_ortho_npy = drone_dir / "master_frames" / f"{location_id}_pts_ortho.npy"
        if not ref_ortho_npy.exists():
            raise FileNotFoundError(f"Reference drone '{ref_drone_id}' has no saved ortho points yet: {ref_ortho_npy}")
        ref_pts_ortho = np.load(str(ref_ortho_npy))
        if ref_idx >= len(ref_pts_ortho):
            raise IndexError(f"'{ref_drone_id}' has only {len(ref_pts_ortho)} ortho point(s); point {ref_point_number} out of range")
        old_point = pts_ortho[this_idx].copy()
        pts_ortho[this_idx] = ref_pts_ortho[ref_idx]
        print(f"Ortho point {this_point_number} synced to {ref_drone_id}'s point {ref_point_number}: "
              f"{old_point} -> {pts_ortho[this_idx]}")
    np.save(str(pts_ortho_npy), pts_ortho)

# %% Compute drone_pixel → ortho_pixel homography ======================================================================
master_img = cv2.imdecode(np.fromfile(str(master_png), dtype=np.uint8), cv2.IMREAD_COLOR)
master_hash = hashlib.md5(master_img.tobytes()).hexdigest()


def compute_fit():
    """RANSAC pass (flags/colors points for review only) + least-squares pass (the fit actually used)."""
    _, ransac_mask, _, _, _ = ht.compute_homography(
        pts_drone, pts_ortho, ransac_threshold=ransac_threshold, gsd_m_per_px=ortho_gsd_m_per_px)
    H_fit, _, errs, su, sv = ht.compute_homography(
        pts_drone, pts_ortho, method=0, gsd_m_per_px=ortho_gsd_m_per_px)
    avg_m = float(errs.mean() * ortho_gsd_m_per_px)
    max_m = float(errs.max() * ortho_gsd_m_per_px)
    print(f"Least-squares error: avg={avg_m:.3f}m (target <= {max_avg_error}m), "
          f"max={max_m:.3f}m (target <= {max_point_error}m); "
          f"RANSAC@{ransac_threshold}px flagged {int((~ransac_mask).sum())} outlier(s)")
    return H_fit, ransac_mask, errs, su, sv, avg_m, max_m

fit_computed = False
if homography_txt.exists():
    print(f"Homography found → {homography_txt}")
    with open(homography_txt, 'r') as f:
        H = np.fromstring(f.readline(), sep=',').reshape(3, 3)
    inlier_mask = np.ones(len(pts_ortho), dtype=bool)
    errors = np.linalg.norm(ht.pixel_to_world(H, pts_drone) - pts_ortho, axis=1)
    n = len(pts_ortho)
    errors_m = errors * ortho_gsd_m_per_px
else:
    fit_computed = True
    print("\nComputing homography (drone frame → orthophoto)...")
    H, inlier_mask, errors, scale_u, scale_v, avg_error_m, max_error_m = compute_fit()

    while (avg_error_m > max_avg_error) or (max_error_m > max_point_error):
        pts_ortho_before, pts_drone_before = pts_ortho.copy(), pts_drone.copy()
        pts_ortho, pts_drone = ht.review_control_points(
            ortho_png, master_png, pts_ortho, pts_drone, H,
            inlier_mask=inlier_mask, only_outliers=False, gsd_m_per_px=ortho_gsd_m_per_px
        )
        changed = not (np.array_equal(pts_ortho, pts_ortho_before) and np.array_equal(pts_drone, pts_drone_before))
        if changed:
            np.save(str(pts_ortho_npy), pts_ortho)
            np.save(str(pts_drone_npy), pts_drone)
            ht.save_annotated_image(ortho_png, pts_ortho, ref_ortho_png)
            ht.save_annotated_image(master_png, pts_drone, ref_drone_png)

        print("\nRecomputing homography after review...")
        H, inlier_mask, errors, scale_u, scale_v, avg_error_m, max_error_m = compute_fit()

        if not changed:
            print("No changes made.")
            break

    n = len(pts_ortho)   # review_control_points may have deleted point(s) via 'd', so recount
    errors_m = errors * ortho_gsd_m_per_px

    # Write cache file in geo-trax format.
    with open(homography_txt, 'w', encoding='utf-8', newline='\n') as f:
        np.savetxt(f, H.reshape(1, -1), fmt='%.20g', delimiter=',')
        f.write('\n# Hash of the master frame\n')
        f.write(f'Hash: {master_hash}\n')
        f.write('\n# Image matching stats\n')
        f.write(f'Stats: Manual - {n} control points, least-squares fit '
                f'(RANSAC@{ransac_threshold}px flagged {int(inlier_mask.sum())}/{n} as clean)\n')
        f.write('\n# Reprojection error (meters)\n')
        f.write(f'Error: min={errors_m.min():.4f}, max={errors_m.max():.4f}, avg={errors_m.mean():.4f}\n')

    print(f"\nSaved homography cache → {homography_txt}")

# %% Diagnostics: local ground-scale falloff + spot-check distances ====================================================
# Ground-scale heatmap: how many real-world meters a 1px homography error covers.
if run_diagnostics:
    ht.report_ground_scale(H, master_png, ortho_gsd_m_per_px, output_html=ground_scale_html)

# Spot-check: click two or more points and get real-world distance.
if measure_points:
    ht.measure_distances(master_png, ortho_params_txt, ortho_center_txt, subset_crop_size, H=H,
                          annotated_output_path=measured_points_png, results_json_path=measured_points_json)
    ht.measure_distances(ortho_png, ortho_params_txt, ortho_center_txt, subset_crop_size, H=None,
                          annotated_output_path=measured_points_ortho_png, results_json_path=measured_points_ortho_json)

# %% Add new objects directly on this drone's frame (H maps drone-pixel -> ortho-pixel) ================================
if add_objects:
    objects = ht.mark_objects(master_png, objects_json, H=H)
    for obj in objects:
        lonlat = np.atleast_2d(
            ht.pixel_to_lonlat(obj["points"], ortho_params_txt, ortho_center_txt, subset_crop_size))
        obj["lonlat"] = [[float(lon), float(lat)] for lon, lat in lonlat]
    ht.save_json_list(objects, objects_json, "object(s)")
    ht.plot_objects(ortho_png, objects_json, objects_html, type_colors, section_alphas)
    ht.plot_objects(master_png, objects_json, drone_objects_html, type_colors, section_alphas, H=H)

# %% Refine objects using this drone's view ============================================================================
if edit_objects:
    objects = ht.refine_objects(master_png, H, objects_json, type_colors, section_alphas,
                                gsd_m_per_px=ortho_gsd_m_per_px, warn_threshold_m=max_point_error)
    for obj in objects:
        lonlat = np.atleast_2d(ht.pixel_to_lonlat(obj["points"], ortho_params_txt, ortho_center_txt, subset_crop_size))
        obj["lonlat"] = [[float(lon), float(lat)] for lon, lat in lonlat]
    ht.save_json_list(objects, objects_json, "object(s)")
    ht.plot_objects(ortho_png, objects_json, objects_html, type_colors, section_alphas)
    ht.plot_objects(master_png, objects_json, drone_objects_html, type_colors, section_alphas, H=H)

# %% Save run log ======================================================================================================
# Snapshot of every tunable parameter used for this location/drone - only when the homography fit runs
if fit_computed:
    run_log = {
        "source": source[0],
        "location_parts": source[1:],
        "location_id": location_id,
        "drone_id": drone_id,
        "ref_frame": ref_frame,
        "master_video": master_video,
        "selected_video": selected_video_name,
        "ortho_tif_epsg4326": str(ortho_tif_epsg4326),
        "subset_crop_size": subset_crop_size,
        "location_lonlat": location_lonlat,
        "ortho_gsd_m_per_px": ortho_gsd_m_per_px,
        "n_control_points": n,
        "shared_ortho_points": shared_ortho_points,
        "point_radius": point_radius,
        "ransac_threshold": ransac_threshold,
        "master_frame_hash": master_hash,
        "max_avg_error": max_avg_error,
        "max_point_error": max_point_error,
        "reprojection_error_min": float(errors_m.min()),
        "reprojection_error_max": float(errors_m.max()),
        "reprojection_error_avg": float(errors_m.mean()),
    }
    run_log_path.write_text(json.dumps(run_log, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Run log saved → {run_log_path}")

print(f"\nDone.")