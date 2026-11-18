"""
Elevation-aware pixel-to-world projection for drone traffic videos.

Use this module instead of homography_tools when the scene has significant elevation
changes (hills, ramps, multi-level terrain). If the terrain is flat, use homography_tools
instead — it is simpler and does not require camera intrinsics.

***PENDING TO VERIFY***
"""

import csv
import json
import cv2
import numpy as np
from pathlib import Path

from video_utils import extract_frame
from homography_tools import (
    pick_pixel_points,
    pick_points_from_geotiff,
    coords_to_world_meters,
)


# ---------------------------------------------------------------------------
# DEM loading and sampling
# ---------------------------------------------------------------------------

class DEM:
    """
    Digital Elevation Model wrapper.

    Stores a 2D grid of elevations with georeferencing (origin and pixel size),
    and provides bilinear sampling and ray-intersection functions.

    Coordinate convention: x increases east, y increases north, z is elevation (metres).

    Construction:
        dem = DEM(elevation_grid, x0, y0, dx, dy)
        dem = DEM.from_geotiff("terrain.tif")   # requires rasterio
        dem = DEM.from_npz("terrain.npz")
    """

    def __init__(self, elevation, x0, y0, dx, dy):
        """
        :param elevation  [np.ndarray (H, W)]  2D grid of elevation values in metres.
                                                elevation[i, j] is the height at world
                                                position (x0 + j*dx, y0 + i*dy).
        :param x0         [float]              World x-coordinate of the grid origin cell.
        :param y0         [float]              World y-coordinate of the grid origin cell.
        :param dx         [float]              Cell size in metres along x (east).
        :param dy         [float]              Cell size in metres along y (north).
        """
        self.elevation = np.asarray(elevation, dtype=np.float64)
        self.x0 = float(x0)
        self.y0 = float(y0)
        self.dx = float(dx)
        self.dy = float(dy)
        self.height, self.width = self.elevation.shape

    @classmethod
    def from_geotiff(cls, path):
        """
        Load a DEM from a GeoTIFF file.

        :param path  [str or Path]  Path to the GeoTIFF DEM file. Requires rasterio.

        :return  [DEM]
        """
        try:
            import rasterio
        except ImportError as e:
            raise ImportError("rasterio is required to load GeoTIFF DEMs. "
                              "Install with: pip install rasterio") from e

        with rasterio.open(str(path)) as src:
            elevation = src.read(1)
            transform = src.transform
            x0 = transform.c
            y0 = transform.f
            dx = transform.a
            dy = transform.e  # usually negative for north-up rasters
            if dy < 0:
                elevation = np.flipud(elevation)
                y0 = y0 + dy * elevation.shape[0]
                dy = -dy
        return cls(elevation, x0, y0, dx, dy)

    @classmethod
    def from_npz(cls, path):
        """
        Load a DEM from a NumPy .npz file (keys: elevation, x0, y0, dx, dy).

        :param path  [str or Path]  Path to the .npz file.

        :return  [DEM]
        """
        data = np.load(str(path))
        return cls(data["elevation"], float(data["x0"]), float(data["y0"]),
                   float(data["dx"]), float(data["dy"]))

    def sample(self, x, y):
        """
        Sample elevation at world coordinates (x, y) using bilinear interpolation.

        :param x  [float or np.ndarray]  World x-coordinate(s) in metres.
        :param y  [float or np.ndarray]  World y-coordinate(s) in metres.

        :return  [float or np.ndarray]  Interpolated elevation in metres. NaN for out-of-bounds.
        """
        x = np.asarray(x, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64)

        j = (x - self.x0) / self.dx
        i = (y - self.y0) / self.dy

        valid = (i >= 0) & (i <= self.height - 1) & (j >= 0) & (j <= self.width - 1)

        i0 = np.clip(np.floor(i).astype(int), 0, self.height - 1)
        i1 = np.clip(i0 + 1, 0, self.height - 1)
        j0 = np.clip(np.floor(j).astype(int), 0, self.width - 1)
        j1 = np.clip(j0 + 1, 0, self.width - 1)

        di = i - i0
        dj = j - j0

        z = (self.elevation[i0, j0] * (1 - di) * (1 - dj) +
             self.elevation[i0, j1] * (1 - di) * dj +
             self.elevation[i1, j0] * di * (1 - dj) +
             self.elevation[i1, j1] * di * dj)

        return np.where(valid, z, np.nan)


# ---------------------------------------------------------------------------
# Camera extrinsics estimation
# ---------------------------------------------------------------------------

def estimate_camera_pose(world_points_3d, pixel_points, K, D, model="pinhole"):
    """
    Estimate camera position and orientation in world coordinates using PnP + RANSAC.

    :param world_points_3d  [array-like (N, 3)]   Known 3D world coordinates (X, Y, Z). N >= 4.
    :param pixel_points     [array-like (N, 2)]   Corresponding pixel coordinates in the image.
    :param K                [np.ndarray (3, 3)]   Camera intrinsic matrix from Stage 1.
    :param D                [np.ndarray]          Distortion coefficients from Stage 1.
    :param model            [str]                 "pinhole" or "fisheye" — must match Stage 1.

    :return  rvec             [np.ndarray (3, 1)]  Rotation vector (Rodrigues) from world to camera.
             tvec             [np.ndarray (3, 1)]  Translation vector from world origin to camera.
             R                [np.ndarray (3, 3)]  Rotation matrix (convenient for ray casting).
             camera_position  [np.ndarray (3,)]    Camera optical centre in world coordinates.
    """
    obj_pts = np.asarray(world_points_3d, dtype=np.float64).reshape(-1, 1, 3)
    img_pts = np.asarray(pixel_points, dtype=np.float64).reshape(-1, 1, 2)

    if len(obj_pts) < 4:
        raise ValueError(f"Need at least 4 point pairs, got {len(obj_pts)}")
    if len(obj_pts) != len(img_pts):
        raise ValueError(f"Mismatch: {len(obj_pts)} 3D points vs {len(img_pts)} 2D points")

    if model == "fisheye":
        img_pts_undist = cv2.fisheye.undistortPoints(img_pts, K, D)
        img_pts_undist = (K @ np.hstack(
            [img_pts_undist.reshape(-1, 2), np.ones((len(img_pts_undist), 1))]
        ).T).T[:, :2].reshape(-1, 1, 2).astype(np.float64)
    else:
        img_pts_undist = cv2.undistortPoints(img_pts, K, D, P=K)

    success, rvec, tvec, inliers = cv2.solvePnPRansac(
        obj_pts, img_pts_undist, K, None,
        flags=cv2.SOLVEPNP_ITERATIVE,
        reprojectionError=3.0,
    )
    if not success:
        raise RuntimeError("solvePnPRansac failed. Check point correspondences.")

    n_inliers = len(inliers) if inliers is not None else len(obj_pts)
    print(f"Camera pose estimated. Inliers: {n_inliers}/{len(obj_pts)}")

    R, _ = cv2.Rodrigues(rvec)
    camera_position = (-R.T @ tvec).ravel()
    print(f"Camera position (world): "
          f"({camera_position[0]:.2f}, {camera_position[1]:.2f}, {camera_position[2]:.2f})")

    return rvec, tvec, R, camera_position


# ---------------------------------------------------------------------------
# Pixel-to-3D-world projection via DEM ray-casting
# ---------------------------------------------------------------------------

def pixel_to_ray(pixel_points, K, D, R, camera_position, model="pinhole"):
    """
    Convert pixel coordinates to world-space rays from the camera centre.

    :param pixel_points    [array-like (N, 2)]   Pixel coordinates.
    :param K               [np.ndarray (3, 3)]   Camera intrinsic matrix.
    :param D               [np.ndarray]          Distortion coefficients.
    :param R               [np.ndarray (3, 3)]   Rotation matrix from estimate_camera_pose().
    :param camera_position [np.ndarray (3,)]     Camera optical centre in world coordinates.
    :param model           [str]                 "pinhole" or "fisheye".

    :return  origins     [np.ndarray (N, 3)]  Ray start points (all equal to camera_position).
             directions  [np.ndarray (N, 3)]  Unit-length ray directions in world coordinates.
    """
    pts = np.asarray(pixel_points, dtype=np.float64).reshape(-1, 1, 2)

    if model == "fisheye":
        normalized = cv2.fisheye.undistortPoints(pts, K, D).reshape(-1, 2)
    else:
        normalized = cv2.undistortPoints(pts, K, D).reshape(-1, 2)

    rays_cam = np.hstack([normalized, np.ones((len(normalized), 1))])
    rays_world = (R.T @ rays_cam.T).T
    norms = np.linalg.norm(rays_world, axis=1, keepdims=True)
    directions = rays_world / norms

    origins = np.tile(camera_position, (len(directions), 1))
    return origins, directions


def intersect_ray_with_dem(origin, direction, dem, t_min=0.0, t_max=1000.0,
                           initial_step=10.0, refine_iters=20):
    """
    Find the intersection of a single ray with a DEM surface.

    Marches along the ray until it drops below the DEM height, then bisects to refine.

    :param origin        [np.ndarray (3,)]  Ray start point in world coordinates.
    :param direction     [np.ndarray (3,)]  Unit ray direction.
    :param dem           [DEM]              DEM to intersect with.
    :param t_min         [float]            Minimum search distance along the ray (metres).
    :param t_max         [float]            Maximum search distance along the ray (metres).
    :param initial_step  [float]            Coarse marching step size (metres).
    :param refine_iters  [int]              Bisection iterations after crossing detection.

    :return  [np.ndarray (3,) or None]  World coordinates of the intersection, or None if not found.
    """
    if direction[2] >= 0:
        return None

    t = t_min
    prev_diff = None

    while t < t_max:
        x = origin[0] + t * direction[0]
        y = origin[1] + t * direction[1]
        z_ray = origin[2] + t * direction[2]
        z_dem = dem.sample(x, y)

        if np.isnan(z_dem):
            t += initial_step
            continue

        diff = z_ray - z_dem
        if prev_diff is not None and prev_diff > 0 and diff <= 0:
            t_lo, t_hi = t - initial_step, t
            for _ in range(refine_iters):
                t_mid = 0.5 * (t_lo + t_hi)
                x_m = origin[0] + t_mid * direction[0]
                y_m = origin[1] + t_mid * direction[1]
                z_ray_m = origin[2] + t_mid * direction[2]
                z_dem_m = dem.sample(x_m, y_m)
                if np.isnan(z_dem_m) or z_ray_m > z_dem_m:
                    t_lo = t_mid
                else:
                    t_hi = t_mid
            t_final = 0.5 * (t_lo + t_hi)
            return np.array([
                origin[0] + t_final * direction[0],
                origin[1] + t_final * direction[1],
                origin[2] + t_final * direction[2],
            ])

        prev_diff = diff
        t += initial_step

    return None


def project_pixels_to_world_3d(pixel_points, K, D, R, camera_position, dem,
                                model="pinhole"):
    """
    Project pixel coordinates to 3D world coordinates via DEM ray-casting.

    :param pixel_points    [array-like (N, 2)]   Pixel coordinates to project.
    :param K               [np.ndarray (3, 3)]   Camera intrinsic matrix.
    :param D               [np.ndarray]          Distortion coefficients.
    :param R               [np.ndarray (3, 3)]   Rotation matrix from estimate_camera_pose().
    :param camera_position [np.ndarray (3,)]     Camera optical centre in world coordinates.
    :param dem             [DEM]                 DEM for ray-surface intersection.
    :param model           [str]                 "pinhole" or "fisheye".

    :return  [np.ndarray (N, 3)]  World coordinates (X, Y, Z). NaN for failed intersections.
    """
    origins, directions = pixel_to_ray(pixel_points, K, D, R, camera_position, model)

    results = np.full((len(pixel_points), 3), np.nan)
    for i in range(len(pixel_points)):
        pt = intersect_ray_with_dem(origins[i], directions[i], dem)
        if pt is not None:
            results[i] = pt
    return results


# ---------------------------------------------------------------------------
# Save / load camera pose
# ---------------------------------------------------------------------------

def save_camera_pose(path, rvec, tvec, R, camera_position):
    """
    Save camera extrinsics to a .npz file.

    :param path             [str or Path]        Output file path.
    :param rvec             [np.ndarray (3, 1)]  Rotation vector.
    :param tvec             [np.ndarray (3, 1)]  Translation vector.
    :param R                [np.ndarray (3, 3)]  Rotation matrix.
    :param camera_position  [np.ndarray (3,)]    Camera centre in world coordinates.
    """
    np.savez(str(path), rvec=rvec, tvec=tvec, R=R, camera_position=camera_position)
    print(f"Saved camera pose to {path}")


def load_camera_pose(path):
    """
    Load camera extrinsics from a .npz file saved by save_camera_pose().

    :param path  [str or Path]  Path to the .npz file.

    :return  rvec, tvec, R, camera_position
    """
    data = np.load(str(path))
    return data["rvec"], data["tvec"], data["R"], data["camera_position"]


# ---------------------------------------------------------------------------
# Interactive 3D coordinate collection helpers
# ---------------------------------------------------------------------------

def _annotate_image(image_path, pixel_points, output_path):
    """Save a copy of the image with numbered point markers."""
    img = cv2.imread(str(image_path))
    if img is None:
        return
    h, w = img.shape[:2]
    r = max(5, max(w, h) // 400)
    thick = max(2, max(w, h) // 800)
    fs = max(0.6, max(w, h) / 1500)
    for i, (x, y) in enumerate(pixel_points):
        x, y = int(round(x)), int(round(y))
        cv2.circle(img, (x, y), r, (0, 0, 255), -1)
        cv2.putText(img, str(i + 1), (x + r + 2, y - r - 2),
                    cv2.FONT_HERSHEY_SIMPLEX, fs, (0, 255, 255), thick, cv2.LINE_AA)
    cv2.imwrite(str(output_path), img)


def _show_highlighted(image_path, pixel_points, active_idx):
    """Show image in a resizable window with the active point highlighted in green."""
    img = cv2.imread(str(image_path))
    if img is None:
        return
    h, w = img.shape[:2]
    r = max(5, max(w, h) // 400)
    thick = max(2, max(w, h) // 800)
    fs = max(0.6, max(w, h) / 1500)
    for i, (x, y) in enumerate(pixel_points):
        x, y = int(round(x)), int(round(y))
        if i == active_idx:
            cv2.circle(img, (x, y), r * 3, (0, 255, 0), thick + 1)
            cv2.circle(img, (x, y), r, (0, 255, 0), -1)
            cv2.putText(img, str(i + 1), (x + r + 2, y - r - 2),
                        cv2.FONT_HERSHEY_SIMPLEX, fs, (0, 255, 0), thick, cv2.LINE_AA)
        else:
            cv2.circle(img, (x, y), r, (0, 0, 255), -1)
            cv2.putText(img, str(i + 1), (x + r + 2, y - r - 2),
                        cv2.FONT_HERSHEY_SIMPLEX, fs, (0, 255, 255), thick, cv2.LINE_AA)
    cv2.namedWindow("Reference points", cv2.WINDOW_NORMAL)
    cv2.resizeWindow("Reference points", 1400, 900)
    cv2.imshow("Reference points", img)
    cv2.waitKey(1)


def _prompt_z_per_point(n_points, pixel_points, image_path):
    """
    Prompt for Z elevation (metres) for each reference point.

    :param n_points      [int]               Number of points.
    :param pixel_points  [np.ndarray (N,2)]  Pixel positions shown in the image overlay.
    :param image_path    [str or Path]       Image shown while prompting.

    :return  [list of float]  Z elevations in metres, one per point.
    """
    z_values = [None] * n_points
    i = 0
    while i < n_points:
        _show_highlighted(image_path, pixel_points, i)
        prev = f" (previously: {z_values[i]:.3f} m)" if z_values[i] is not None else ""
        try:
            s = input(f"  Point {i+1} — Z elevation in metres{prev}: ").strip()
        except EOFError:
            raise RuntimeError("Input closed before all Z values were entered.")
        if s.lower() in ("b", "back"):
            if i == 0:
                print("    (already at first point)")
                continue
            i -= 1
            continue
        try:
            z_values[i] = float(s)
        except ValueError:
            print("    ✗ Enter a number (e.g. 45.3).")
            continue
        i += 1
    try:
        cv2.destroyWindow("Reference points")
    except cv2.error:
        pass
    return z_values


def _prompt_xyz_per_point(n_points, pixel_points, image_path):
    """
    Prompt for (X, Y, Z) in metres for each reference point.

    :param n_points      [int]               Number of points.
    :param pixel_points  [np.ndarray (N,2)]  Pixel positions shown in the image overlay.
    :param image_path    [str or Path]       Image shown while prompting.

    :return  [list of (X, Y, Z) tuples]  3D metric coordinates.
    """
    coords = [None] * n_points
    i = 0
    while i < n_points:
        _show_highlighted(image_path, pixel_points, i)
        prev = f" (previously: {coords[i]})" if coords[i] is not None else ""
        try:
            s = input(f"  Point {i+1} — X Y Z in metres{prev}: ").strip()
        except EOFError:
            raise RuntimeError("Input closed before all coordinates were entered.")
        if s.lower() in ("b", "back"):
            if i == 0:
                print("    (already at first point)")
                continue
            i -= 1
            continue
        s = s.replace(";", " ").replace(",", " ")
        parts = s.split()
        if len(parts) != 3:
            print("    ✗ Enter three numbers: X Y Z  (e.g. 12.5 -3.2 45.1)")
            continue
        try:
            coords[i] = (float(parts[0]), float(parts[1]), float(parts[2]))
        except ValueError:
            print("    ✗ Could not parse numbers.")
            continue
        i += 1
    try:
        cv2.destroyWindow("Reference points")
    except cv2.error:
        pass
    return coords


def _prompt_latlonz_per_point(n_points, pixel_points, image_path):
    """
    Prompt for (lat, lon, Z) for each reference point.

    :param n_points      [int]               Number of points.
    :param pixel_points  [np.ndarray (N,2)]  Pixel positions shown in the image overlay.
    :param image_path    [str or Path]       Image shown while prompting.

    :return  [list of (lat, lon, Z) tuples]
    """
    coords = [None] * n_points
    i = 0
    while i < n_points:
        _show_highlighted(image_path, pixel_points, i)
        prev = f" (previously: {coords[i]})" if coords[i] is not None else ""
        try:
            s = input(f"  Point {i+1} — lat lon Z{prev}: ").strip()
        except EOFError:
            raise RuntimeError("Input closed before all coordinates were entered.")
        if s.lower() in ("b", "back"):
            if i == 0:
                print("    (already at first point)")
                continue
            i -= 1
            continue
        s = s.replace(";", " ").replace(",", " ")
        parts = s.split()
        if len(parts) != 3:
            print("    ✗ Enter three numbers: lat lon Z  (e.g. 55.706012 13.193045 45.3)")
            continue
        try:
            lat, lon, z = float(parts[0]), float(parts[1]), float(parts[2])
        except ValueError:
            print("    ✗ Could not parse numbers.")
            continue
        if not (-90 <= lat <= 90):
            print(f"    ✗ Latitude {lat} out of [-90, 90].")
            continue
        if not (-180 <= lon <= 180):
            print(f"    ✗ Longitude {lon} out of [-180, 180].")
            continue
        coords[i] = (lat, lon, z)
        i += 1
    try:
        cv2.destroyWindow("Reference points")
    except cv2.error:
        pass
    return coords


# ---------------------------------------------------------------------------
# Calibration orchestration
# ---------------------------------------------------------------------------

def calibrate_elevation_video(video_path, n_points, K, D, model="pinhole",
                               frame_index=0, geotiff_path=None,
                               coords_format="metric3d", coords=None,
                               origin_latlon=None, dem=None):
    """
    Estimate camera pose for elevation-aware projection and save all calibration outputs.

    Three coordinate input modes — choose based on available data:
      - coords_format="metric3d" (default, no geotiff_path):
          Click the video frame, then type (X, Y, Z) in metres per point.
          Suitable for theodolite or total-station field measurements.
      - coords_format="latlon3d" (no geotiff_path):
          Click the video frame, then type (lat, lon, Z) per point.
          Suitable when XY comes from GPS/GIS and Z from a level instrument.
      - geotiff_path=... (Lantmäteriet GeoTIFF):
          Click the GeoTIFF — (lat, lon) extracted automatically from the file's CRS.
          Then click the video frame. Z is queried from dem if provided, otherwise typed.

    :param video_path     [str or Path]        Input video file.
    :param n_points       [int]                Number of reference points (>= 4).
    :param K              [np.ndarray (3,3)]   Camera intrinsic matrix from Stage 1.
    :param D              [np.ndarray]         Distortion coefficients from Stage 1.
    :param model          [str]                "pinhole" or "fisheye" — must match Stage 1.
    :param frame_index    [int]                Video frame to use as the calibration reference.
    :param geotiff_path   [str or Path|None]   Georeferenced GeoTIFF. XY auto-extracted from CRS.
    :param coords_format  [str]                "metric3d" or "latlon3d". Ignored if geotiff_path set.
    :param coords         [list of tuples|None] Pre-supplied 3D coordinates; skips interactive entry.
                                               [(X,Y,Z), ...] for metric3d,
                                               [(lat,lon,Z), ...] for latlon3d / GeoTIFF mode.
    :param origin_latlon  [(lat,lon)|None]     Geodetic origin for the latlon→metres projection.
    :param dem            [DEM|None]           DEM for automatic Z. Only used with geotiff_path.

    :return  rvec, tvec, R, camera_position  — camera extrinsics (see estimate_camera_pose).
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")
    if geotiff_path is not None:
        geotiff_path = Path(geotiff_path)
        if not geotiff_path.exists():
            raise FileNotFoundError(f"GeoTIFF not found: {geotiff_path}")
    if geotiff_path is None and coords_format not in ("metric3d", "latlon3d"):
        raise ValueError(f"coords_format must be 'metric3d' or 'latlon3d', got {coords_format!r}")
    if coords is not None and len(coords) != n_points:
        raise ValueError(f"coords has {len(coords)} entries but n_points is {n_points}")

    base = video_path.with_suffix("")
    output_pose = base.with_name(f"{base.name}_pose_f{frame_index}.npz")
    output_frame = base.with_name(f"{base.name}_reference_f{frame_index}.png")
    output_csv = base.with_name(f"{base.name}_refpoints3d_f{frame_index}.csv")

    # Step 1: extract reference frame
    extract_frame(video_path, output_frame, frame_index)

    # Step 2: collect 3D world coordinates and pixel correspondences on video frame
    pixel_on_map = None
    raw_latlon = None
    origin_used = None

    if coords is not None:
        # Pre-supplied 3D coordinates — determine format from geotiff_path or coords_format
        use_latlon = (geotiff_path is not None) or (coords_format == "latlon3d")
        if use_latlon:
            raw_latlon = [(c[0], c[1]) for c in coords]
            z_values = [float(c[2]) for c in coords]
            world_xy, origin_used = coords_to_world_meters(raw_latlon, "latlon", origin_latlon)
        else:
            xyz = np.asarray(coords, dtype=np.float64)
            world_xy = xyz[:, :2]
            z_values = list(xyz[:, 2])
        world_3d = np.column_stack([world_xy, z_values])
        print()
        print("=" * 70)
        print(f"VIDEO FRAME PICKING — click the {n_points} reference points")
        print("=" * 70)
        print("Pre-supplied coordinates will be used.")
        print()
        pixel_video = pick_pixel_points(output_frame)
        if len(pixel_video) != n_points:
            raise ValueError(
                f"Clicked {len(pixel_video)} points on the video, expected {n_points}.")

    elif geotiff_path is not None:
        # GeoTIFF mode: auto-extract lat/lon, click video, get Z
        pixel_on_map, latlon_coords = pick_points_from_geotiff(geotiff_path, n_points)
        raw_latlon = list(latlon_coords)
        world_xy, origin_used = coords_to_world_meters(latlon_coords, "latlon", origin_latlon)
        print()
        print("=" * 70)
        print(f"VIDEO FRAME PICKING — click the same {n_points} points")
        print("=" * 70)
        print("Click the SAME points in the SAME ORDER as on the GeoTIFF.")
        print()
        pixel_video = pick_pixel_points(output_frame)
        if len(pixel_video) != n_points:
            raise ValueError(
                f"Clicked {len(pixel_video)} points on the video, expected {n_points}.")
        if dem is not None:
            z_values = [float(dem.sample(world_xy[i, 0], world_xy[i, 1]))
                        for i in range(n_points)]
            for i, z in enumerate(z_values):
                if np.isnan(z):
                    raise ValueError(
                        f"Point {i+1} is outside the DEM bounds. "
                        "Check that the GeoTIFF and DEM cover the same area.")
            print("\nZ values from DEM:")
            for i, z in enumerate(z_values):
                print(f"  Point {i+1}: Z = {z:.3f} m")
        else:
            print()
            print("Type the elevation Z in metres for each point.")
            print("Controls: b = go back, Enter = accept.")
            z_values = _prompt_z_per_point(n_points, pixel_video, output_frame)
        world_3d = np.column_stack([world_xy, z_values])

    elif coords_format == "latlon3d":
        print()
        print("=" * 70)
        print(f"VIDEO FRAME PICKING — click the {n_points} reference points")
        print("=" * 70)
        print()
        pixel_video = pick_pixel_points(output_frame)
        if len(pixel_video) != n_points:
            raise ValueError(
                f"Clicked {len(pixel_video)} points on the video, expected {n_points}.")
        print()
        print("=" * 70)
        print("TYPE COORDINATES — lat lon Z for each point")
        print("=" * 70)
        print("Format: lat lon Z  (e.g. 55.706012 13.193045 45.3)")
        print("Controls: b = go back")
        print()
        raw_latlonz = _prompt_latlonz_per_point(n_points, pixel_video, output_frame)
        raw_latlon = [(c[0], c[1]) for c in raw_latlonz]
        z_values = [c[2] for c in raw_latlonz]
        world_xy, origin_used = coords_to_world_meters(raw_latlon, "latlon", origin_latlon)
        world_3d = np.column_stack([world_xy, z_values])

    else:  # metric3d
        print()
        print("=" * 70)
        print(f"VIDEO FRAME PICKING — click the {n_points} reference points")
        print("=" * 70)
        print()
        pixel_video = pick_pixel_points(output_frame)
        if len(pixel_video) != n_points:
            raise ValueError(
                f"Clicked {len(pixel_video)} points on the video, expected {n_points}.")
        print()
        print("=" * 70)
        print("TYPE COORDINATES — X Y Z in metres for each point")
        print("=" * 70)
        print("Format: X Y Z  (e.g. 12.5 -3.2 45.1)")
        print("Controls: b = go back")
        print()
        raw_xyz = _prompt_xyz_per_point(n_points, pixel_video, output_frame)
        world_3d = np.asarray(raw_xyz, dtype=np.float64)
        z_values = list(world_3d[:, 2])

    _annotate_image(output_frame, pixel_video, output_frame)

    print(f"\n3D reference points (metres):")
    for i, (x, y, z) in enumerate(world_3d):
        print(f"  Point {i+1}: X={x:.3f}  Y={y:.3f}  Z={z:.3f}")

    # Step 3: estimate camera pose
    rvec, tvec, R, camera_position = estimate_camera_pose(
        world_3d, pixel_video, K, D, model=model)

    # Step 4: save camera pose
    save_camera_pose(output_pose, rvec, tvec, R, camera_position)

    # Step 5: CSV log
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["point_id"]
        if raw_latlon is not None:
            header += ["lat_deg", "lon_deg"]
        header += ["x_world_m", "y_world_m", "z_world_m",
                   "x_pixel_video", "y_pixel_video"]
        writer.writerow(header)
        for i in range(n_points):
            row = [i + 1]
            if raw_latlon is not None:
                row += [f"{raw_latlon[i][0]:.7f}", f"{raw_latlon[i][1]:.7f}"]
            row += [f"{world_3d[i, 0]:.4f}", f"{world_3d[i, 1]:.4f}", f"{world_3d[i, 2]:.4f}",
                    f"{pixel_video[i, 0]:.2f}", f"{pixel_video[i, 1]:.2f}"]
            writer.writerow(row)
    print(f"Saved per-point log: {output_csv}")

    # Step 6: geodetic origin sidecar
    if origin_used is not None:
        sidecar = base.with_name(f"{base.name}_origin_f{frame_index}.json")
        with open(sidecar, "w") as f:
            json.dump({
                "origin_lat": origin_used[0],
                "origin_lon": origin_used[1],
                "frame": "local tangent plane (East, North), metres",
                "geotiff": str(geotiff_path) if geotiff_path else None,
                "n_reference_points": n_points,
            }, f, indent=2)
        print(f"Saved geodetic origin sidecar: {sidecar}")

    return rvec, tvec, R, camera_position


def validate_elevation_video(video_path, n_points, K, D, model="pinhole",
                              frame_index=0, geotiff_path=None,
                              coords_format="metric3d", coords=None,
                              origin_latlon=None, validation_tag=""):
    """
    Validate camera pose by reprojecting hold-out 3D points and measuring pixel error.

    Accepts the same coordinate input modes as calibrate_elevation_video().
    Load origin_latlon from the calibration sidecar JSON when using latlon or GeoTIFF mode.

    :param video_path     [str or Path]        Path to the video.
    :param n_points       [int]                Number of hold-out validation points.
    :param K              [np.ndarray (3,3)]   Camera intrinsic matrix from Stage 1.
    :param D              [np.ndarray]         Distortion coefficients from Stage 1.
    :param model          [str]                "pinhole" or "fisheye".
    :param frame_index    [int]                Frame index used during calibration.
    :param geotiff_path   [str or Path|None]   Georeferenced GeoTIFF for automatic XY.
    :param coords_format  [str]                "metric3d" or "latlon3d".
    :param coords         [list of tuples|None] Pre-supplied hold-out 3D coordinates.
    :param origin_latlon  [(lat,lon)|None]     Geodetic origin from the calibration sidecar.
    :param validation_tag [str]                Appended to output filename (e.g. "_holdout").

    :return  pixel_errors  [np.ndarray (n_points,)]  Per-point pixel reprojection error.
    """
    video_path = Path(video_path)
    base = video_path.with_suffix("")
    pose_path = base.with_name(f"{base.name}_pose_f{frame_index}.npz")
    frame_path = base.with_name(f"{base.name}_reference_f{frame_index}.png")

    if not pose_path.exists():
        raise FileNotFoundError(
            f"Pose file not found: {pose_path}\nRun calibrate_elevation_video() first.")
    if not frame_path.exists():
        raise FileNotFoundError(
            f"Reference frame not found: {frame_path}\n"
            "Make sure frame_index matches the calibration.")

    rvec, tvec, R, camera_position = load_camera_pose(pose_path)

    # Collect hold-out 3D points and video pixel positions
    # Re-use calibrate_elevation_video logic but without saving outputs
    if coords is not None:
        use_latlon = (geotiff_path is not None) or (coords_format == "latlon3d")
        if use_latlon:
            raw_latlon = [(c[0], c[1]) for c in coords]
            z_values = [float(c[2]) for c in coords]
            world_xy, _ = coords_to_world_meters(raw_latlon, "latlon", origin_latlon)
        else:
            xyz = np.asarray(coords, dtype=np.float64)
            world_xy = xyz[:, :2]
            z_values = list(xyz[:, 2])
        world_3d = np.column_stack([world_xy, z_values])
        print(f"\nClick the {n_points} validation points on the video frame:")
        pixel_video = pick_pixel_points(frame_path)
    elif geotiff_path is not None:
        geotiff_path = Path(geotiff_path)
        _, latlon_coords = pick_points_from_geotiff(geotiff_path, n_points)
        world_xy, _ = coords_to_world_meters(latlon_coords, "latlon", origin_latlon)
        print(f"\nClick the same {n_points} validation points on the video frame:")
        pixel_video = pick_pixel_points(frame_path)
        print("Type Z elevations for validation points:")
        z_values = _prompt_z_per_point(n_points, pixel_video, frame_path)
        world_3d = np.column_stack([world_xy, z_values])
    elif coords_format == "latlon3d":
        pixel_video = pick_pixel_points(frame_path)
        raw_latlonz = _prompt_latlonz_per_point(n_points, pixel_video, frame_path)
        raw_latlon = [(c[0], c[1]) for c in raw_latlonz]
        z_values = [c[2] for c in raw_latlonz]
        world_xy, _ = coords_to_world_meters(raw_latlon, "latlon", origin_latlon)
        world_3d = np.column_stack([world_xy, z_values])
    else:  # metric3d
        pixel_video = pick_pixel_points(frame_path)
        raw_xyz = _prompt_xyz_per_point(n_points, pixel_video, frame_path)
        world_3d = np.asarray(raw_xyz, dtype=np.float64)

    if len(pixel_video) != n_points:
        raise ValueError(f"Clicked {len(pixel_video)} points, expected {n_points}.")

    # Project 3D world points to pixels using the camera model
    obj_pts = world_3d.reshape(-1, 1, 3).astype(np.float64)
    if model == "fisheye":
        projected, _ = cv2.fisheye.projectPoints(obj_pts, rvec, tvec, K, D)
    else:
        projected, _ = cv2.projectPoints(obj_pts, rvec, tvec, K, D)
    projected = projected.reshape(-1, 2)

    pixel_errors = np.linalg.norm(projected - pixel_video, axis=1)

    print(f"\n--- Validation reprojection errors (pixels) ---")
    print(f"  Mean : {pixel_errors.mean():.2f} px")
    print(f"  Max  : {pixel_errors.max():.2f} px")
    print(f"  RMSE : {np.sqrt((pixel_errors ** 2).mean()):.2f} px")
    for i, e in enumerate(pixel_errors):
        print(f"  Point {i+1}: {e:.2f} px")

    val_path = base.with_name(
        f"{base.name}_validation3d_f{frame_index}{validation_tag}.npz")
    np.savez(str(val_path),
             pixel_points=pixel_video,
             world_points=world_3d,
             projected_pixels=projected,
             pixel_errors=pixel_errors)
    print(f"\nSaved validation report: {val_path}")

    return pixel_errors
