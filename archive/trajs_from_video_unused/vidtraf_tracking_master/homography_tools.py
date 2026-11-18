"""
Homography is the mathematical transformation that converts pixel coordinates to world coordinates on the ground plane.
It is a 3×3 matrix H that, returns any pixel position (u, v) into the corresponding world position (x, y) in meters.
A homography is a planar projective transformation. It maps one flat plane to another. The assumption is that:
- The ground (road, floor, sidewalk) is approximately flat in the area of interest
- The camera sees this plane from some angle
"""

from pathlib import Path
import csv
import json
import cv2
import numpy as np
import pyproj
import base64
import plotly.graph_objects as go
import rasterio
from tifffile import TiffFile
from rasterio.merge import merge as rasterio_merge
from rasterio.warp import calculate_default_transform, reproject, Resampling
from vidtraf_tracking_master.video_utils import get_frame


# Helpers --------------------------------------------------------------------------------------------------------------
ARROW = {2424832: (-1, 0), 2555904: (1, 0), 2490368: (0, -1), 2621440: (0, 1)}   # left, right, up, down


def load_image(image_path, label="image") -> np.ndarray:
    """Decode an image via cv2 (Unicode-path safe); raise FileNotFoundError if it can't be read."""
    img = cv2.imdecode(np.fromfile(str(image_path), dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        raise FileNotFoundError(f"Cannot load {label}: {image_path}")
    return img


def save_image(img, output_path):
    """Encode img by output_path's extension and write it (Unicode-path safe)."""
    ext = Path(output_path).suffix.lower()
    cv2.imencode(ext, img)[1].tofile(str(output_path))


def find_nearest_point(ix, iy, candidates, radius_px):
    """Nearest point to (ix, iy) among candidates (iterable of (x, y)) within radius_px, or None."""
    best, best_d2 = None, radius_px ** 2
    for vx, vy in candidates:
        d2 = (vx - ix) ** 2 + (vy - iy) ** 2
        if d2 <= best_d2:
            best, best_d2 = (vx, vy), d2
    return best


def confirm_pending_vertex(pending, pending_snapped, points):
    """Append pending as a new vertex to points (logging it), then clear the pending state. All updated in place."""
    if pending[0] is not None:
        points.append((pending[0], pending[1]))
        tag = " (snapped)" if pending_snapped[0] else ""
        print(f"  Vertex {len(points)}: ({pending[0]}, {pending[1]}){tag}")
        pending[0] = pending[1] = None
        pending_snapped[0] = False


def setup_click_window(img, max_display_size=None):
    """
    Scale an image to fit the screen (or a given cap) for an interactive cv2 window,
    and return pixel<->display coordinate converters.

    :param img [np.ndarray]                         Full-resolution source image (as loaded by cv2).
    :param max_display_size [(int, int), optional]  Display size cap in screen pixels; auto-detected if None.

    :return img_display [np.ndarray]        Resized image for on-screen display.
    :return win_w, win_h [int]              Display window dimensions in screen pixels.
    :return to_img [callable(dx, dy)]       Converts display coordinates -> source image pixel coordinates.
    :return to_disp [callable(ix, iy)]      Converts source image pixel coordinates -> display coordinates.
    """
    h, w = img.shape[:2]
    if max_display_size is None:
        try:
            import ctypes
            user32 = ctypes.windll.user32
            # noinspection PyUnresolvedReferences
            screen_w = user32.GetSystemMetrics(0)
            # noinspection PyUnresolvedReferences
            screen_h = user32.GetSystemMetrics(1)
        except Exception:
            screen_w, screen_h = 1920, 1080
        max_display_size = (int(screen_w * 0.9), int(screen_h * 0.9))

    scale = min(max_display_size[0] / w, max_display_size[1] / h, 1.0)
    win_w = max(400, int(w * scale))
    win_h = max(300, int(h * scale))
    img_display = cv2.resize(img, (win_w, win_h))
    sx, sy = win_w / w, win_h / h

    def to_img(dx, dy):
        return int(round(dx / sx)), int(round(dy / sy))

    def to_disp(ix, iy):
        return int(round(ix * sx)), int(round(iy * sy))

    return img_display, win_w, win_h, to_img, to_disp


def apply_pending(ix, iy, w, h, to_disp, pending, cursor_xy, snap_fn=None, snapped_flag=None):
    """
    Clamp a candidate pending point to image bounds, optionally snap it onto an existing point via snap_fn,
     and update pending + the display-space cursor position in place.

    :param ix, iy [int]                              Candidate pending point, in source image pixels.
    :param w, h [int]                                Source image dimensions, for clamping.
    :param to_disp [callable(ix, iy)]                Source pixel -> display pixel converter (from setup_click_window).
    :param pending [list[int|None, int|None]]        Mutable 2-element pending-point state, updated in place.
    :param cursor_xy [list[int, int]]                Mutable display-space cursor position, updated in place.
    :param snap_fn [callable(ix, iy) -> (x,y)|None, optional]  Returns a snap target near (ix, iy), or None.
    :param snapped_flag [list[bool], optional]       If given, snapped_flag[0] is set to whether snapping fired.
    """
    ix = max(0, min(w - 1, ix))
    iy = max(0, min(h - 1, iy))
    if snap_fn is not None:
        snapped = snap_fn(ix, iy)
        if snapped_flag is not None:
            snapped_flag[0] = snapped is not None
        if snapped is not None:
            ix, iy = snapped
    pending[0], pending[1] = ix, iy
    cursor_xy[0], cursor_xy[1] = to_disp(ix, iy)


def zoom_crop(img, cursor_xy, to_img, zoom_size, zoom_factor):
    """
    Extract and upscale a magnified square crop of img centered on the display-space cursor_xy.

    :param img [np.ndarray]                    Full-resolution source image.
    :param cursor_xy [(int, int)]              Cursor position in display-space pixels.
    :param to_img [callable(dx, dy)]           Display pixel -> source pixel converter.
    :param zoom_size [int]                     Side length (source px) of the cropped region.
    :param zoom_factor [int]                   Magnification factor.

    :return zoom_img [np.ndarray or None]        The magnified crop (None if the crop was empty).
    :return to_zoom [callable(px, py) or None]   Source pixel -> zoom_img pixel converter.
    :return in_view [callable(px, py) or None]   Whether a source pixel falls inside the cropped region.
    """
    h, w = img.shape[:2]
    half = zoom_size // 2
    zds = zoom_size * zoom_factor
    cx_img, cy_img = to_img(*cursor_xy)
    x0, y0 = max(0, cx_img - half), max(0, cy_img - half)
    x1, y1 = min(w, cx_img + half), min(h, cy_img + half)
    crop = img[y0:y1, x0:x1].copy()
    if crop.size == 0:
        return None, None, None

    pad_top = max(0, half - (cy_img - y0))
    pad_left = max(0, half - (cx_img - x0))
    pad_bottom = zoom_size - crop.shape[0] - pad_top
    pad_right = zoom_size - crop.shape[1] - pad_left
    if any((pad_top, pad_bottom, pad_left, pad_right)):
        crop = cv2.copyMakeBorder(crop, pad_top, pad_bottom, pad_left, pad_right,
                                  cv2.BORDER_CONSTANT, value=(64, 64, 64))
    zoom_img = cv2.resize(crop, (zds, zds), interpolation=cv2.INTER_NEAREST)

    def to_zoom(px, py):
        return int((px - (cx_img - half)) * zoom_factor), int((py - (cy_img - half)) * zoom_factor)

    def in_view(px, py):
        return (cx_img - half) <= px < (cx_img + half) and (cy_img - half) <= py < (cy_img + half)

    return zoom_img, to_zoom, in_view


def draw_crosshair(zoom_img, thickness, color):
    """Draw a centered crosshair on a zoom crop (from zoom_crop), colored to reflect pending-point state."""
    zds = zoom_img.shape[0]
    c = zds // 2
    cv2.line(zoom_img, (c, 0), (c, zds), color, thickness)
    cv2.line(zoom_img, (0, c), (zds, c), color, thickness)


def draw_zoom_box(display, cursor_xy, zoom_size, sx, sy, box_thick, color=(0, 255, 0)):
    """Draw the green rectangle on the main window marking the area the zoom window is magnifying."""
    cx, cy = cursor_xy
    hx = max(1, int(zoom_size * sx // 2))
    hy = max(1, int(zoom_size * sy // 2))
    cv2.rectangle(display, (cx - hx, cy - hy), (cx + hx, cy + hy), color, box_thick)


def make_mouse_handlers(to_img, set_pending, draw_main, draw_zoom, pending, cursor_xy, prev_xy,
                        zoom_state, zoom_factor):
    """
    Build the on_main_event/on_zoom_click mouse callbacks in interactive picking tools:
    left-click (in either window) sets a pending point, scrolling the main window zooms, and both
    windows repaint via the caller's own draw_main/draw_zoom after every event.

    :return on_main_event, on_zoom_click [callable]   cv2 mouse callbacks.
    """
    def on_main_event(event, x, y, flags, param):
        if pending[0] is None:
            cursor_xy[0], cursor_xy[1] = x, y
        if event == cv2.EVENT_LBUTTONDOWN:
            set_pending(*to_img(x, y))
        elif event == cv2.EVENT_MOUSEWHEEL:
            zoom_state[0] = max(10, zoom_state[0] - 10) if flags > 0 else min(500, zoom_state[0] + 10)
        draw_main()
        if abs(x - prev_xy[0]) + abs(y - prev_xy[1]) <= 3 or pending[0] is not None:
            draw_zoom()
        prev_xy[0], prev_xy[1] = x, y

    def on_zoom_click(event, x, y, flags, param):
        if event != cv2.EVENT_LBUTTONDOWN:
            return
        half = zoom_state[0] // 2
        cx_img, cy_img = to_img(*cursor_xy)
        set_pending(int(round((cx_img - half) + x / zoom_factor)),
                    int(round((cy_img - half) + y / zoom_factor)))
        draw_main()
        draw_zoom()

    return on_main_event, on_zoom_click


def flush_stdin():
    """
    Discard any stray buffered keystrokes before an input() prompt.
    """
    try:
        import msvcrt
        while msvcrt.kbhit():
            msvcrt.getch()
    except ImportError:
        pass


def save_json_list(items, output_path, noun):
    """Write items (a list of dicts) to output_path as indented JSON, creating parent dirs as needed."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(items, indent=2), encoding="utf-8")
    print(f"Saved {len(items)} {noun} -> {output_path}")

    return output_path


# Orthophoto preparation ------------------------------------------------------------------------------------------------

def merge_tif_tiles(tile_paths, output_tif):
    """
    Merge multiple GeoTIFF tiles into a single GeoTIFF.

    :param tile_paths [list of str or Path]   Paths to the input tiles. All must share the same CRS and band count.
    :param output_tif [str or Path]           Path for the merged output TIF.
    """
    tile_paths = [Path(p) for p in tile_paths]
    missing = [p for p in tile_paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Tile(s) not found: {missing}")
    output_tif = Path(output_tif)
    print(f"Merging {len(tile_paths)} TIF tiles...")
    datasets = [rasterio.open(p) for p in tile_paths]
    try:
        merged_arr, merged_transform = rasterio_merge(datasets)
        meta = datasets[0].meta.copy()
    finally:
        for ds in datasets:
            ds.close()
    meta.update({"height": merged_arr.shape[1], "width": merged_arr.shape[2], "transform": merged_transform})
    output_tif.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(output_tif, "w", **meta) as dst:
        dst.write(merged_arr)
    print(f"Saved merged TIF: {output_tif.name}")


def reproject_tif(src_tif, dst_tif, dst_crs="EPSG:4326"):
    """
    Reproject a GeoTIFF to a target CRS.

    :param src_tif [str or Path]   Input GeoTIFF.
    :param dst_tif [str or Path]   Output GeoTIFF path.
    :param dst_crs [str]           Target CRS as an EPSG string — defaults to WGS84 (EPSG 4326).

    :return [Path]  Path to the reprojected TIF, or src_tif if already in dst_crs.
    """
    src_tif = Path(src_tif)
    dst_tif = Path(dst_tif)

    if not src_tif.exists():
        raise FileNotFoundError(f"Source TIF not found: {src_tif}")

    with rasterio.open(src_tif) as src:
        src_epsg = src.crs.to_epsg()
        src_crs_str = f"EPSG:{src_epsg}" if src_epsg else str(src.crs)

        if src_crs_str.upper() == dst_crs.upper():
            print(f"Source already in {dst_crs}.")
            return src_tif

        print(f"Reprojecting {src_crs_str} -> {dst_crs} ...")
        dst_crs_obj = rasterio.CRS.from_string(dst_crs)
        dst_tif.parent.mkdir(parents=True, exist_ok=True)

        resolution = None
        if dst_crs_obj.is_geographic and not src.crs.is_geographic:
            src_gsd_x, src_gsd_y = abs(src.transform.a), abs(src.transform.e)
            cx_native, cy_native = src.transform * (src.width / 2, src.height / 2)
            transformer = pyproj.Transformer.from_crs(src.crs, dst_crs_obj, always_xy=True)
            lon0, lat0 = transformer.transform(cx_native, cy_native)
            geod = pyproj.Geod(ellps="WGS84")
            lon_x, _, _ = geod.fwd(lon0, lat0, 90, src_gsd_x)   # src_gsd_x meters due east
            _, lat_y, _ = geod.fwd(lon0, lat0, 0, src_gsd_y)    # src_gsd_y meters due north
            resolution = (abs(lon_x - lon0), abs(lat_y - lat0))
            print(f"Output resolution: {resolution[0]:.10f} x {resolution[1]:.10f} deg/px "
                  f"(preserves source's {src_gsd_x:.4f} x {src_gsd_y:.4f} m/px)")

        t, w, h = calculate_default_transform(src.crs, dst_crs_obj, src.width, src.height, *src.bounds,
                                              resolution=resolution)
        meta = src.meta.copy()
        meta.update({"crs": dst_crs_obj, "transform": t, "width": w, "height": h,
                     "tiled": True, "blockxsize": 256, "blockysize": 256})
        with rasterio.open(dst_tif, "w", **meta) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=t,
                    dst_crs=dst_crs_obj,
                    resampling=Resampling.bilinear,
                )

    print(f"Saved reprojected TIF: {dst_tif.name}")
    return dst_tif


def measure_ortho_gsd(ortho_tif_path, lonlat=None):
    """
    Measure the true ground sampling distance (GSD) of a georeferenced orthophoto, in meters per pixel.
      - Projected (units already in meters, e.g. SWEREF99, UTM): pixel size is read directly off the transform.
      - Geographic (units in degrees, e.g. WGS84): pixel size in degrees is converted to meters via geodesic distance,
      which requires a reference point.

    :param ortho_tif_path [str or Path]           Georeferenced orthophoto.
    :param lonlat [(lon, lat) tuple, optional]    Reference point for geographic CRSs (ignored for
                                                  projected CRSs). Defaults to the raster's own center.

    :return gsd_m_per_px [float]   Ground sampling distance in meters/pixel, averaged over x and y.
    """
    with rasterio.open(ortho_tif_path) as src:
        transform = src.transform
        is_geographic = src.crs.is_geographic

        if not is_geographic:
            gsd_x, gsd_y = abs(transform.a), abs(transform.e)
        else:
            col, row = (src.width / 2, src.height / 2) if lonlat is None else ~transform * lonlat
            lon0, lat0 = transform * (col, row)
            lon_x, lat_x = transform * (col + 1, row)
            lon_y, lat_y = transform * (col, row + 1)
            geod = pyproj.Geod(ellps="WGS84")
            _, _, gsd_x = geod.inv(lon0, lat0, lon_x, lat_x)
            _, _, gsd_y = geod.inv(lon0, lat0, lon_y, lat_y)

    if max(gsd_x, gsd_y) and abs(gsd_x - gsd_y) / max(gsd_x, gsd_y) > 0.05:
        print(f"Warning: anisotropic pixel scale — x={gsd_x:.4f} m/px, y={gsd_y:.4f} m/px")

    gsd = (gsd_x + gsd_y) / 2
    print(f"Measured ortho GSD: {gsd:.4f} m/px (x={gsd_x:.4f}, y={gsd_y:.4f})")

    return gsd


def read_ortho_params(tif_path, output_txt=None):
    """
    Read ortho parameters from a full GeoTIFF's tags,

    :param tif_path [str or Path]              Georeferenced orthophoto (EPSG:4326).
    :param output_txt [str or Path, optional]  Save as ortho_parameters.txt (geo-trax format).

    :return ortho_params [np.ndarray, shape (4,)]   (lng_0, lat_0, lng_scale, -lat_scale):
                                               top-left corner of pixel (0,0) and degrees/pixel.
    """
    with TiffFile(str(tif_path)) as tif:
        tif_tags = {tag.name: tag.value for tag in tif.pages[0].tags.values()}

    lng_0, lat_0 = tif_tags['ModelTiepointTag'][3:5]
    lng_scale, lat_scale = tif_tags['ModelPixelScaleTag'][:2]
    ortho_params = np.array([lng_0, lat_0, lng_scale, -lat_scale])

    if output_txt is not None:
        np.savetxt(str(output_txt), ortho_params)
        print(f"Saved ortho parameters: {Path(output_txt).name}")

    return ortho_params


def save_geotiff_preview(tif_path, out_path, max_size=2000):
    """
    Downsampled RGB preview PNG of a (possibly huge) GeoTIFF.

    :param tif_path [str or Path]   Source GeoTIFF (any size).
    :param out_path [str or Path]  Output PNG path.
    :param max_size [int]          Longest output side, in pixels.
    """
    tif_path, out_path = Path(tif_path), Path(out_path)
    with rasterio.open(tif_path) as src:
        scale = max_size / max(src.width, src.height)
        out_shape = (src.count, max(1, round(src.height * scale)), max(1, round(src.width * scale)))
        data = src.read(out_shape=out_shape, resampling=Resampling.average)

    rgb = np.moveaxis(data[:3], 0, -1)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ok, buf = cv2.imencode(".png", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
    if not ok:
        raise RuntimeError(f"Failed to encode preview PNG for {tif_path}")
    out_path.write_bytes(buf.tobytes())   # cv2.imwrite silently fails on Windows paths with non-ASCII characters
    print(f"Preview saved -> {out_path}")


# Control points with real-worl coodinates -----------------------------------------------------------------------------

def pick_real_points(image_path, coords=None, annotated_output_path=None):
    """
    Interactively pick reference points and their real-world X,Y coordinates.

    Without coords: click a point, press Enter, type its X,Y in meters — repeat until done, then press q/ESC
                     (minimum 4 points required).
    With coords:    lists the known world positions in order and click the corresponding pixels in that same order.

    :param image_path [str or Path]                       Image to click on.
    :param coords [list of (x, y) tuples, optional]       Pre-supplied world coordinates in meters.
    :param annotated_output_path [str or Path, optional]  If set, saves a copy of the image with markers.

    :return pixel_points [(N, 2) np.float32]              Clicked positions in image pixels.
    :return world_points [(N, 2) np.float32]              Real-world X,Y coordinates in meters.
    """
    image_path = Path(image_path)
    if not image_path.exists():
        raise FileNotFoundError(f"Image not found: {image_path}")

    if coords is not None:
        n = len(coords)
        if n < 4:
            raise ValueError(f"Need at least 4 coordinate pairs, got {n}")
        print("=" * 70)
        print(f"Click {n} points in the following order:")
        for i, (x, y) in enumerate(coords):
            print(f"  {i+1}: X={x:.3f}, Y={y:.3f} m")
        print("=" * 70)
        pixel_points = pick_pixel_points(image_path)
        if len(pixel_points) != n:
            raise ValueError(f"Expected {n} clicks, got {len(pixel_points)}.")
        world_points = np.asarray(coords, dtype=np.float32)
        if annotated_output_path is not None:
            save_annotated_image(image_path, pixel_points, annotated_output_path)
        return pixel_points.astype(np.float32), world_points

    # Interactive: click a point -> Enter to confirm -> type X,Y -> next point
    img = load_image(image_path)
    window_name = "Pick reference points (Enter=confirm, u=undo, q/ESC=finish)"
    zoom_window_name = "Zoom"
    h, w = img.shape[:2]
    zoom_factor = 8
    zoom_state = [200]
    img_display, win_w, win_h, to_img, to_disp = setup_click_window(img)
    sx, sy = win_w / w, win_h / h
    box_thick = max(1, win_w // 800)
    crosshair_thick = max(2, zoom_factor // 2)
    confirmed_pixels = []
    confirmed_world = []
    pending = [None, None]
    cursor_xy = [win_w // 2, win_h // 2]
    prev_xy = [win_w // 2, win_h // 2]

    def set_pending(ix, iy):
        apply_pending(ix, iy, w, h, to_disp, pending, cursor_xy)

    def draw_main():
        sz = zoom_state[0]
        display = img_display.copy()
        for i, (ix, iy) in enumerate(confirmed_pixels):
            dx, dy = to_disp(ix, iy)
            cv2.circle(display, (dx, dy), 6, (0, 0, 255), -1)
            cv2.putText(display, str(i + 1), (dx + 8, dy - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
        if pending[0] is not None:
            # noinspection PyTypeChecker
            dx, dy = to_disp(pending[0], pending[1])
            cv2.circle(display, (dx, dy), 8, (0, 255, 255), 2)
            cv2.putText(display, f">{len(confirmed_pixels) + 1}", (dx + 8, dy - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
        draw_zoom_box(display, cursor_xy, sz, sx, sy, box_thick)
        cv2.imshow(window_name, display)

    def draw_zoom():
        # noinspection PyTypeChecker
        zoom_img, to_zoom, in_view = zoom_crop(img, cursor_xy, to_img, zoom_state[0], zoom_factor)
        if zoom_img is None:
            return
        for i, (px, py) in enumerate(confirmed_pixels):
            if in_view(px, py):
                lx, ly = to_zoom(px, py)
                cv2.circle(zoom_img, (lx, ly), 6, (0, 0, 255), -1)
                cv2.putText(zoom_img, str(i + 1), (lx + 10, ly - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 255), 2)
        col = (0, 255, 255) if pending[0] is not None else (0, 255, 0)
        draw_crosshair(zoom_img, crosshair_thick, col)
        cv2.imshow(zoom_window_name, zoom_img)

    on_main_event, on_zoom_click = make_mouse_handlers(
        to_img, set_pending, draw_main, draw_zoom, pending, cursor_xy, prev_xy, zoom_state, zoom_factor)

    cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
    cv2.namedWindow(zoom_window_name, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(window_name, win_w, win_h)
    cv2.resizeWindow(zoom_window_name, 500, 500)
    cv2.setMouseCallback(window_name, on_main_event)
    cv2.setMouseCallback(zoom_window_name, on_zoom_click)
    draw_main()
    draw_zoom()

    print("Click a point -> Enter to confirm -> type X,Y in meters. Repeat. q/ESC when done (>=4 points).")
    print("Controls: left-click/zoom-click = place | arrows = nudge | Enter = confirm+type | u = undo | scroll = zoom")

    while True:
        key = cv2.waitKeyEx(20)
        if key in (ord('q'), 27):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                draw_main()
                draw_zoom()
            elif len(confirmed_pixels) >= 4:
                break
            else:
                print(f"  Need at least 4 points (have {len(confirmed_pixels)}). Keep clicking.")
        elif key in (13, 10):  # Enter: confirm pending, then ask for coord
            if pending[0] is not None:
                px, py = pending[0], pending[1]
                pending[0] = pending[1] = None
                print(f"\n  Point {len(confirmed_pixels) + 1} at pixel ({px}, {py})")
                accepted = False
                while not accepted:
                    try:
                        s = input("  X,Y in meters (or 'b' to cancel): ").strip()
                    except EOFError:
                        raise RuntimeError("Input stream closed")
                    if s.lower() in ("b", "back"):
                        print("  Cancelled - click again.")
                        break
                    for sep in (";", "\t"):
                        s = s.replace(sep, ",")
                    parts = s.replace(",", " ").split()
                    if len(parts) != 2:
                        print("    Expected X Y or X,Y. Try again.")
                        continue
                    try:
                        wx, wy = float(parts[0]), float(parts[1])
                    except ValueError:
                        print("    Could not parse numbers. Try again.")
                        continue
                    confirmed_pixels.append((px, py))
                    confirmed_world.append((wx, wy))
                    print(f"  Saved - {len(confirmed_pixels)} point(s) total")
                    accepted = True
                draw_main()
                draw_zoom()
        elif key == ord('u'):
            if pending[0] is not None:
                pending[0] = pending[1] = None
            elif confirmed_pixels:
                confirmed_pixels.pop()
                confirmed_world.pop()
                print(f"  Removed last point ({len(confirmed_pixels)} remaining)")
            draw_main()
            draw_zoom()
        elif key in ARROW and pending[0] is not None:
            ddx, ddy = ARROW[key]
            # noinspection PyTypeChecker
            set_pending(pending[0] + ddx, pending[1] + ddy)
            draw_main()
            draw_zoom()

    cv2.destroyWindow(window_name)
    cv2.destroyWindow(zoom_window_name)

    pixel_arr = np.array(confirmed_pixels, dtype=np.float32)
    world_arr = np.array(confirmed_world, dtype=np.float32)

    if annotated_output_path is not None:
        save_annotated_image(image_path, pixel_arr, annotated_output_path)
        print(f"Saved annotated image: {annotated_output_path}")

    return pixel_arr, world_arr


# Control points with ortophotos ---------------------------------------------------------------------------------------

def pick_pixel_points(image_path, zoom_size=200, zoom_factor=8,
                      max_display_size=None, zoom_window_size=(500, 500), point_radius=6):
    """
    Interactive tool to click reference points on an image, with a zoom window. Two windows are shown:
        - Main window: full image with all clicked points marked.
        - Zoom window: magnified view around the cursor with a crosshair.
    Hover over the main window to scout. Click in either window to add a point.
    Controls:
        Left-click in EITHER window  -> add a point
        u key                        -> undo last point
        q key or ESC                 -> finish


    :param image_path [str or Path]     Path to the reference image.
    :param zoom_size [int]              Side length (in source pixels) of the region around the cursor shown
                                        in the zoom view. Smaller = more magnification of a smaller area.
    :param zoom_factor [int]            Magnification factor of the zoomed region.
    :param max_display_size, zoom_window_size [tuple (width, height)]    Display size of the windows in screen pixels.
    :param point_radius [int]           Radius, in pixels, of the confirmed-point markers drawn in the main and
                                        zoom windows. Smaller = easier to see exactly where the point sits.

    :returns: np.ndarray of shape (N, 2) with float32 pixel coordinates.
    """
    img = load_image(image_path)
    window_name = "Click reference points"
    zoom_window_name = "Zoom"
    h, w = img.shape[:2]
    img_display, win_w, win_h, to_img, to_disp = setup_click_window(img, max_display_size)
    sx, sy = win_w / w, win_h / h
    box_thick = max(1, win_w // 800)
    crosshair_thick = max(2, zoom_factor // 2)
    points = []
    cursor_xy  = [win_w // 2, win_h // 2]
    prev_xy    = [win_w // 2, win_h // 2]
    zoom_state = [zoom_size]
    pending: list[int | None] = [None, None]

    def set_pending(ix, iy):
        apply_pending(ix, iy, w, h, to_disp, pending, cursor_xy)

    def draw_main():
        sz = zoom_state[0]
        display = img_display.copy()
        for i, (ix, iy) in enumerate(points):
            dx, dy = to_disp(ix, iy)
            cv2.circle(display, (dx, dy), point_radius, (0, 0, 255), -1)
            cv2.putText(display, str(i + 1), (dx + 8, dy - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
        if pending[0] is not None:
            # noinspection PyTypeChecker
            dx, dy = to_disp(pending[0], pending[1])
            cv2.circle(display, (dx, dy), point_radius + 2, (0, 255, 255), 2)
            cv2.putText(display, f">{len(points) + 1}", (dx + 8, dy - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
        draw_zoom_box(display, cursor_xy, sz, sx, sy, box_thick)
        cv2.imshow(window_name, display)

    def draw_zoom():
        # noinspection PyTypeChecker
        zoom_img, to_zoom, in_view = zoom_crop(img, cursor_xy, to_img, zoom_state[0], zoom_factor)
        if zoom_img is None:
            return
        for i, (px, py) in enumerate(points):
            if in_view(px, py):
                lx, ly = to_zoom(px, py)
                cv2.circle(zoom_img, (lx, ly), point_radius, (0, 0, 255), -1)
                cv2.putText(zoom_img, str(i + 1), (lx + 10, ly - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 255), 2)
        color = (0, 255, 255) if pending[0] is not None else (0, 255, 0)
        draw_crosshair(zoom_img, crosshair_thick, color)
        cv2.imshow(zoom_window_name, zoom_img)

    def confirm_pending():
        if pending[0] is not None:
            x, y = pending[0], pending[1]
            if 0 <= x < w and 0 <= y < h:
                points.append((x, y))
                print(f"  Point {len(points)}: ({x}, {y})")
            pending[0] = pending[1] = None

    on_main_event, on_zoom_click = make_mouse_handlers(
        to_img, set_pending, draw_main, draw_zoom, pending, cursor_xy, prev_xy, zoom_state, zoom_factor)

    cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
    cv2.namedWindow(zoom_window_name, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(window_name, win_w, win_h)
    cv2.resizeWindow(zoom_window_name, zoom_window_size[0], zoom_window_size[1])
    cv2.setMouseCallback(window_name, on_main_event)
    cv2.setMouseCallback(zoom_window_name, on_zoom_click)

    draw_main()
    draw_zoom()

    print("Hover over the main window to scout, left-click to set a pending point (yellow).")
    print("Controls: left-click = place pending | arrows = nudge 1px | Enter = confirm | u = undo | scroll = zoom | q/ESC = finish")

    while True:
        key = cv2.waitKeyEx(20)
        if key in (ord('q'), 27):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                draw_main()
                draw_zoom()
            else:
                break
        elif key in (13, 10):
            confirm_pending()
            draw_main()
            draw_zoom()
        elif key == ord('u'):
            if pending[0] is not None:
                pending[0] = pending[1] = None
            elif points:
                removed = points.pop()
                print(f"  Removed point: {removed}")
            draw_main()
            draw_zoom()
        elif key in ARROW and pending[0] is not None:
            dx, dy = ARROW[key]
            set_pending(pending[0] + dx, pending[1] + dy)
            draw_main()
            draw_zoom()

    cv2.destroyWindow(window_name)
    cv2.destroyWindow(zoom_window_name)

    if not points:
        return np.empty((0, 2), dtype=np.float32)
    return np.array(points, dtype=np.float32)


# Homography -----------------------------------------------------------------------------------------------------------

def pixel_to_world(H, pixel_points):
    """
    Apply homography to convert pixel coordinates to world coordinates.

    :param H [np.ndarray, shape (3, 3)]                          Homography matrix from compute_homography().
    :param pixel_points [array-like, shape (N, 2) or (2,)]       Pixel coordinates to transform.

    :return world_points [np.ndarray, shape (N, 2) or (2,)]      Corresponding world coordinates.
    """
    pts = np.atleast_2d(np.asarray(pixel_points, dtype=np.float64))
    homog = np.hstack([pts, np.ones((len(pts), 1))])
    transformed = (H @ homog.T).T
    world = transformed[:, :2] / transformed[:, 2:3]

    return world[0] if np.asarray(pixel_points).ndim == 1 else world


def crop_offset(center_txt=None, crop_size=None):
    """
    Convert ortho-pixel coordinates to (lon, lat)..
    Pixel offset of a crop's top-left corner within the source GeoTIFF; (0, 0) if no crop (full tile).

    :param center_txt, crop_size    See pixel_to_lonlat().

    :return off_x, off_y [float]
    """
    if center_txt is None:
        return 0.0, 0.0
    p_x, p_y = np.loadtxt(str(center_txt))
    return p_x - crop_size // 2, p_y - crop_size // 2


def pixel_to_lonlat(pixel_xy, ortho_params_txt, center_txt=None, crop_size=None):
    """
    Convert ortho-pixel coordinates to (lon, lat)..

    :param pixel_xy [array-like, shape (N, 2) or (2,)]  Ortho-pixel coordinates.
    :param ortho_params_txt [str or Path]   Path to ortho_parameters.txt — the source GeoTIFF's own
                                            origin (lng_0, lat_0) and degrees/pixel (lng_scale, -lat_scale).
    :param center_txt [str or Path, optional]         Path to <location_id>_center.txt — the crop's center point,
                                            as an integer pixel offset (p_x, p_y) within the source GeoTIFF.
                                            None: pixels are on the full GeoTIFF.
    :param crop_size [int, optional]        --crop-size used when the crop was made (subset_crop_size).
                                            Required with center_txt.

    :return lonlat [np.ndarray, shape (N, 2) or (2,)]   (lon, lat) per input point.
    """
    lng_0, lat_0, lng_scale, neg_lat_scale = np.loadtxt(str(ortho_params_txt))
    lat_scale = -neg_lat_scale
    off_x, off_y = crop_offset(center_txt, crop_size)

    pts = np.atleast_2d(np.asarray(pixel_xy, dtype=np.float64))
    src_col = off_x + pts[:, 0]
    src_row = off_y + pts[:, 1]
    lon = lng_0 + src_col * lng_scale
    lat = lat_0 - src_row * lat_scale
    out = np.column_stack([lon, lat])

    return out[0] if np.asarray(pixel_xy).ndim == 1 else out


def lonlat_to_pixel(lonlat, ortho_params_txt, center_txt=None, crop_size=None):
    """
    Convert (lon, lat) to ortho-pixel coordinates — the exact inverse of pixel_to_lonlat().

    :param lonlat [array-like, shape (N, 2) or (2,)]  (lon, lat) points.
    :param ortho_params_txt, center_txt, crop_size    See pixel_to_lonlat().

    :return pixel_xy [np.ndarray, shape (N, 2) or (2,)]   Ortho-pixel coordinates per input point.
    """
    lng_0, lat_0, lng_scale, neg_lat_scale = np.loadtxt(str(ortho_params_txt))
    lat_scale = -neg_lat_scale
    off_x, off_y = crop_offset(center_txt, crop_size)

    pts = np.atleast_2d(np.asarray(lonlat, dtype=np.float64))
    src_col = (pts[:, 0] - lng_0) / lng_scale
    src_row = (lat_0 - pts[:, 1]) / lat_scale
    x = src_col - off_x
    y = src_row - off_y
    out = np.column_stack([x, y])

    return out[0] if np.asarray(lonlat).ndim == 1 else out


def measure_distances(image_path, ortho_params_txt, center_txt, crop_size, H=None,
                      annotated_output_path=None, results_json_path=None, zoom_size=200, zoom_factor=8,
                      max_display_size=None, zoom_window_size=(500, 500), point_radius=6):
    """
    Real-world (geodesic) distance between clicked points — spot-checking the geo-referencing.

    :param image_path [str or Path]        Image to click on: the ortho PNG, or a drone frame (pass H).
    :param ortho_params_txt, center_txt, crop_size   See pixel_to_lonlat().
    :param H [np.ndarray, shape (3,3), optional]   Homography (drone-pixel -> ortho-pixel). Required if
                                            image_path is a drone frame; leave None for the ortho PNG.
    :param annotated_output_path [str or Path, optional]  Save the clicked image with distances.
    :param results_json_path [str or Path, optional]  Save points/lonlat/distances as JSON.
    :param zoom_size, zoom_factor, max_display_size, zoom_window_size, point_radius  See pick_pixel_points().

    :return points, lonlat, distances_m [np.ndarray]   Clicked points, their (lon, lat), and the distance (m).
    """
    points = pick_pixel_points(image_path, zoom_size=zoom_size, zoom_factor=zoom_factor,
                               max_display_size=max_display_size, zoom_window_size=zoom_window_size,
                               point_radius=point_radius)
    if len(points) < 2:
        print("Fewer than 2 points clicked — nothing to measure.")
        return points, np.empty((0, 2)), np.empty(0)

    ortho_pts = points if H is None else pixel_to_world(np.asarray(H, dtype=np.float64), points)
    lonlat = np.atleast_2d(pixel_to_lonlat(ortho_pts, ortho_params_txt, center_txt, crop_size))

    geod = pyproj.Geod(ellps="WGS84")
    distances_m = np.array([
        geod.inv(lonlat[i, 0], lonlat[i, 1], lonlat[i + 1, 0], lonlat[i + 1, 1])[2]
        for i in range(len(lonlat) - 1)
    ])

    print("Point-to-point distances:")
    for i, d in enumerate(distances_m):
        print(f"  {i + 1} -> {i + 2}: {d:.3f} m")

    if annotated_output_path is not None:
        img = load_image(image_path)
        for i, (x, y) in enumerate(points):
            cv2.circle(img, (int(x), int(y)), point_radius, (0, 0, 255), -1)
            cv2.putText(img, str(i + 1), (int(x) + 8, int(y) - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2, cv2.LINE_AA)
        for i, d in enumerate(distances_m):
            (x1, y1), (x2, y2) = points[i], points[i + 1]
            cv2.line(img, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2, cv2.LINE_AA)
            mx, my = int((x1 + x2) / 2), int((y1 + y2) / 2)
            cv2.putText(img, f"{d:.2f} m", (mx + 6, my - 6),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2, cv2.LINE_AA)
        save_image(img, annotated_output_path)
        print(f"Annotated image saved -> {annotated_output_path}")

    if results_json_path is not None:
        results_json_path = Path(results_json_path)
        results_json_path.parent.mkdir(parents=True, exist_ok=True)
        results_json_path.write_text(json.dumps({
            "image": str(image_path),
            "points": points.tolist(),
            "lonlat": lonlat.tolist(),
            "distances_m": distances_m.tolist(),
        }, indent=2), encoding="utf-8")
        print(f"Results saved -> {results_json_path}")

    return points, lonlat, distances_m


def local_ground_scale(H, pixel_points, eps=1.0):
    """
    Estimate the local scale of a homography at each point, via finite differences: how far in
    world_points' space (e.g. ortho pixels) a 1-pixel step moves, along each pixel axis independently.

    :param H [np.ndarray, shape (3, 3)]                 Homography matrix, e.g. from compute_homography().
    :param pixel_points [array-like, shape (N, 2)]      Points (in H's pixel space) to evaluate the scale at.
    :param eps [float]                                  Finite-difference step, in pixels.

    :return scale_u, scale_v [np.ndarray, shape (N,)]   World-space distance covered by a 1px step along
                                                        the u (x) and v (y) pixel axes, at each point.
    """
    pixel_points = np.atleast_2d(np.asarray(pixel_points, dtype=np.float64))
    base = pixel_to_world(H, pixel_points)
    proj_u = pixel_to_world(H, pixel_points + np.array([eps, 0.0]))
    proj_v = pixel_to_world(H, pixel_points + np.array([0.0, eps]))
    scale_u = np.linalg.norm(proj_u - base, axis=1) / eps
    scale_v = np.linalg.norm(proj_v - base, axis=1) / eps

    return scale_u, scale_v


def report_ground_scale(H, image_path, gsd_m_per_px, output_html=None, grid_step=100,
                        color_max_m_per_px=2.0):
    """
    Report a drone-frame homography's local ground scale (real-world meters per pixel) across the
    frame, as a heatmap over the frame image.

    :param H [np.ndarray, shape (3,3)]       Homography (drone-pixel -> ortho-pixel).
    :param image_path [str or Path]          The drone frame H maps from (e.g. the master frame) — used
                                              only as the background image and for its pixel size.
    :param gsd_m_per_px [float]              Ortho ground sampling distance, m/px (from measure_ortho_gsd)
                                              — converts H's local ortho-pixel scale into real-world meters.
    :param output_html [str or Path, optional]   Save an interactive heatmap (Plotly HTML).
    :param grid_step [int]                   Sampling grid spacing, in source pixels.
    :param color_max_m_per_px [float]        Fixed top of the color scale (0 at the bottom).

    :return grid_xy, scale_m_per_px [np.ndarray]   Sampled frame points and the local scale (m/px) there
                                              (averaged over the two pixel axes at each point).
    """
    img = load_image(image_path, "video frame")
    h, w = img.shape[:2]
    H = np.asarray(H, dtype=np.float64)

    xs = np.arange(grid_step / 2, w, grid_step)
    ys = np.arange(grid_step / 2, h, grid_step)
    grid_x, grid_y = np.meshgrid(xs, ys)
    grid_xy = np.column_stack([grid_x.ravel(), grid_y.ravel()])

    scale_u, scale_v = local_ground_scale(H, grid_xy)
    scale_m_per_px = (scale_u + scale_v) / 2 * gsd_m_per_px

    ratio = scale_m_per_px.max() / scale_m_per_px.min()
    n_over = int((scale_m_per_px > color_max_m_per_px).sum())
    print(f"Local ground scale across frame: {scale_m_per_px.min():.4f}-{scale_m_per_px.max():.4f} m/px "
          f"({ratio:.1f}x more ground per pixel at the coarsest point than the finest) — "
          f"a fixed N-pixel homography error there means up to {ratio:.1f}x more real-world error.")
    if n_over:
        print(f"⚠ {n_over}/{len(scale_m_per_px)} sampled point(s) exceed {color_max_m_per_px:.1f} m/px "
              f"— shown hard red on the heatmap.")

    if output_html is not None:
        ok, buf = cv2.imencode(".png", img)
        img_source = f"data:image/png;base64,{base64.b64encode(buf).decode('ascii')}"
        fig = go.Figure()
        fig.add_layout_image(dict(
            source=img_source, xref="x", yref="y", x=0, y=0, sizex=w, sizey=h,
            sizing="stretch", xanchor="left", yanchor="top", layer="below",
        ))
        fig.add_trace(go.Contour(
            x=xs, y=ys, z=scale_m_per_px.reshape(len(ys), len(xs)),
            opacity=0.55, colorscale="Turbo", contours_coloring="heatmap",
            zmin=0, zmax=color_max_m_per_px,
            colorbar=dict(title="m/px"), hovertemplate="x=%{x}<br>y=%{y}<br>%{z:.4f} m/px<extra></extra>",
        ))
        fig.update_xaxes(range=[0, w], showgrid=False, zeroline=False, visible=False)
        fig.update_yaxes(range=[h, 0], showgrid=False, zeroline=False, visible=False, scaleanchor="x", scaleratio=1)
        fig.update_layout(
            title=f"Local ground scale (m/px) — {ratio:.1f}x range across frame ",
            width=min(1600, w), height=min(1600, w) * h / w,
            margin=dict(l=10, r=10, t=40, b=10),
        )
        output_html = Path(output_html)
        output_html.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(str(output_html))
        print(f"Ground-scale heatmap saved -> {output_html}")

    return grid_xy, scale_m_per_px


def compute_homography(pixel_points, world_points, method=cv2.RANSAC, ransac_threshold=3.0,
                       gsd_m_per_px=None):
    """
    Compute the homography matrix mapping pixel coordinates to world coordinates.

    :param pixel_points [array-like, shape (N, 2)]     Reference points in image (pixel) coordinates.
    :param world_points [array-like, shape (N, 2)]     Corresponding points in world coordinates (meters).
    :param method [int]                                OpenCV homography estimation method.
                                                       cv2.RANSAC is robust to outliers.
                                                       Use 0 for least-squares (no outlier rejection).
    :param ransac_threshold [float]                    Maximum reprojection error (in pixels) for an inlier.
    :param gsd_m_per_px [float, optional]              Ground sampling distance of world_points' units, in meters/pixel.

    :return H [np.ndarray, shape (3, 3)]               Homography matrix.
    :return inlier_mask [np.ndarray, shape (N,)]       True for inliers, False for outliers (only meaningful for RANSAC).
    :return errors [np.ndarray, shape (N,)]            Per-point reprojection error, in world_points' units
                                                       (pixels, unless world_points are metric coordinates).
    :return scale_u, scale_v [np.ndarray, shape (N,)]  Per-point local scale from local_ground_scale() —
                                                       how much world-space distance a 1px step along each
                                                       pixel axis covers, at that point.
    """
    pixel_points = np.asarray(pixel_points, dtype=np.float32)
    world_points = np.asarray(world_points, dtype=np.float32)

    if pixel_points.shape != world_points.shape:
        raise ValueError(
            f"Shape mismatch: pixel {pixel_points.shape} vs world {world_points.shape}")
    if len(pixel_points) < 4:
        raise ValueError(f"Need at least 4 point pairs, got {len(pixel_points)}")

    H, mask = cv2.findHomography(pixel_points, world_points, method, ransac_threshold)
    if H is None:
        raise RuntimeError("Homography computation failed. Check point correspondences.")
    print("\nHomography matrix:")
    print(H)

    inlier_mask = mask.ravel().astype(bool) if mask is not None else np.ones(len(pixel_points), dtype=bool)
    projected = pixel_to_world(H, pixel_points)
    errors = np.linalg.norm(projected - world_points, axis=1)
    scale_u, scale_v = local_ground_scale(H, pixel_points)
    n_inliers = int(inlier_mask.sum())
    threshold_txt = f"{ransac_threshold:.2f}px"
    if gsd_m_per_px:
        threshold_txt += f" = {ransac_threshold * gsd_m_per_px:.2f}m"
    print(f"Inliers: {n_inliers}/{len(pixel_points)}  (RANSAC threshold: {threshold_txt})")
    print("Per-point reprojection error (worst first):")
    for idx in np.argsort(-errors):
        tag = "inlier" if inlier_mask[idx] else "OUTLIER"
        expected_m = 1.5 * (scale_u[idx] + scale_v[idx]) / 2 * (gsd_m_per_px or 1.0)
        line = f"  point {idx + 1}: {errors[idx]:7.2f}px"
        if gsd_m_per_px:
            line += f" = {errors[idx] * gsd_m_per_px:6.2f}m"
        line += (f"   local scale: {scale_u[idx] * (gsd_m_per_px or 1.0):5.2f}(u) x "
                f"{scale_v[idx] * (gsd_m_per_px or 1.0):5.2f}(v) {'m' if gsd_m_per_px else 'px'}/px")
        if gsd_m_per_px:
            line += f"   expected~{expected_m:.2f}m @1.5px click"
        print(f"{line}  [{tag}]")

    return H, inlier_mask, errors, scale_u, scale_v


def review_control_points(image_a_path, image_b_path, pts_a, pts_b, H,
                          inlier_mask=None, only_outliers=True, gsd_m_per_px=None,
                          zoom_size=200, zoom_factor=8, zoom_window_size=(500, 500)):
    """
    Side-by-side review/adjustment of control point pairs across two images, with live model feedback:
     each point is shown next to where H currently predicts it should be.

    Generic over any homography between two images (an orthophoto + a drone frame, two camera views of the same plane).

    Controls:
        Left-click in EITHER zoom window  -> move that image's point to the clicked position
        Arrow keys                        -> nudge the point last clicked, by 1px
        n / p                             -> next / previous point in the review list
        r                                 -> reset the current point pair to its original position
        d                                 -> delete this point pair entirely (requires >4 points left).
                                             An outlier already has zero influence on the current H.
        q / ESC                           -> finish, keeping all edits made so far

    :param image_a_path [str or Path]           Image the pts_a points were picked on.
    :param image_b_path [str or Path]           Image the pts_b points were picked on.
    :param pts_a, pts_b [(N, 2) array-like]     Corresponding control points (index i = same physical point).
                                                H maps pts_b's pixel space -> pts_a's (H_inv the reverse).
    :param H [np.ndarray, shape (3, 3)]              Homography, e.g. from compute_homography(). Used to compute
                                                     each point's live reprojection error/local scale, and to
                                                     show where the model currently predicts each point should be.
    :param inlier_mask [(N,) array-like, optional]  True=inlier, False=outlier. Used to pick the default
                                                     review list, and to color-code each point's crosshair.
    :param only_outliers [bool]                     If True (default) and inlier_mask is given, only review
                                                     outlier points. If False, review all points (worst first).
    :param gsd_m_per_px [float, optional]           Ground sampling distance of pts_a's units, in meters/pixel.
                                                     If given, side A's error is also shown in meters.
    :param zoom_size [int]                          Side length (source px) of the region shown, per image.
    :param zoom_factor [int]                        Magnification factor.
    :param zoom_window_size [(int, int)]           Display size of each zoom window, in screen pixels.

    :return pts_a, pts_b [(N, 2) np.float32]  Updated point arrays, with any adjustments applied.
    """
    img_a = load_image(image_a_path, "image A")
    img_b = load_image(image_b_path, "image B")

    pts_a = np.array(pts_a, dtype=np.float64)
    pts_b = np.array(pts_b, dtype=np.float64)
    n = len(pts_a)
    if len(pts_b) != n:
        raise ValueError(f"Point count mismatch: {n} in image A vs {len(pts_b)} in image B")
    orig_a, orig_b = pts_a.copy(), pts_b.copy()
    H = np.asarray(H, dtype=np.float64)
    H_inv = np.linalg.inv(H)

    def point_diag(i):
        """Live diagnostics for point i, recomputed from the CURRENT pts_a/pts_b positions."""
        pred_a = pixel_to_world(H, pts_b[i:i + 1])[0]
        pred_b = pixel_to_world(H_inv, pts_a[i:i + 1])[0]
        err_a = float(np.linalg.norm(pred_a - pts_a[i]))
        err_b = float(np.linalg.norm(pred_b - pts_b[i]))
        su, sv = local_ground_scale(H, pts_b[i:i + 1])
        return pred_a, pred_b, err_a, err_b, float(su[0]), float(sv[0])

    all_err_a = np.linalg.norm(pixel_to_world(H, pts_b) - pts_a, axis=1)

    if inlier_mask is not None:
        inlier_mask = np.asarray(inlier_mask).astype(bool)
        review_idx = np.where(~inlier_mask)[0] if only_outliers else np.arange(n)
    else:
        review_idx = np.arange(n)
    review_idx = list(review_idx[np.argsort(-all_err_a[review_idx])])   # worst first
    if not review_idx:
        print("No points to review.")
        return pts_a.astype(np.float32), pts_b.astype(np.float32)

    print(f"Reviewing {len(review_idx)} point(s):")
    for review_i in review_idx:
        _, _, err_a, err_b, su, sv = point_diag(review_i)
        m_txt = f"={err_a * gsd_m_per_px:.2f}m" if gsd_m_per_px else ""
        print(f"  point {int(review_i) + 1}: a-resid={err_a:.2f}px{m_txt}, b-resid={err_b:.2f}px, "
              f"local scale={su:.2f}(u) x {sv:.2f}(v) px/px")

    win_a, win_b = "A (review)", "B (review)"
    cv2.namedWindow(win_a, cv2.WINDOW_NORMAL)
    cv2.namedWindow(win_b, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(win_a, *zoom_window_size)
    cv2.resizeWindow(win_b, *zoom_window_size)

    cursor = [0]                 # index into review_idx
    active_side = ["a"]          # which point the arrow keys currently nudge
    identity_to_img = lambda dx, dy: (dx, dy)
    crosshair_thick = max(2, zoom_factor // 2)

    def status_line(i, err_a, err_b, su, sv):
        tag = ""
        if inlier_mask is not None:
            tag = " OUTLIER" if not inlier_mask[i] else " inlier"
        err_txt = f", a-resid={err_a:.2f}px"
        if gsd_m_per_px:
            err_txt += f"={err_a * gsd_m_per_px:.2f}m"
        err_txt += f", b-resid={err_b:.2f}px"
        scale_txt = f", local scale={su:.2f}(u) x {sv:.2f}(v) px/px"
        return f"point {i + 1} ({cursor[0] + 1}/{len(review_idx)}){tag}{err_txt}{scale_txt}"

    def draw():
        i = review_idx[cursor[0]]
        pred_a, pred_b, err_a, err_b, su, sv = point_diag(i)
        line = status_line(i, err_a, err_b, su, sv)
        print(f"  {line}")
        preds = {"a": pred_a, "b": pred_b}
        for img, win, pt, side in ((img_a, win_a, pts_a[i], "a"),
                                   (img_b, win_b, pts_b[i], "b")):
            cx, cy = int(round(pt[0])), int(round(pt[1]))
            zoom_img, to_zoom, in_view = zoom_crop(img, (cx, cy), identity_to_img, zoom_size, zoom_factor)
            if zoom_img is None:
                continue
            color = (0, 255, 0)
            if inlier_mask is not None:
                color = (0, 255, 0) if inlier_mask[i] else (0, 0, 255)
            thick = crosshair_thick + (2 if active_side[0] == side else 0)
            draw_crosshair(zoom_img, thick, color)
            # Model-predicted position (magenta)
            pred = preds[side]
            if in_view(*pred):
                px, py = to_zoom(*pred)
                ax, ay = to_zoom(cx, cy)
                cv2.arrowedLine(zoom_img, (ax, ay), (px, py), (255, 0, 255), 1, tipLength=0.3)
                cv2.drawMarker(zoom_img, (px, py), (255, 0, 255), cv2.MARKER_TILTED_CROSS,
                              markerSize=max(10, crosshair_thick * 5), thickness=2)
            cv2.setWindowTitle(win, f"{side.upper()} — {line}")
            cv2.imshow(win, zoom_img)

    def click_to_source(center_xy, click_xy):
        half = zoom_size // 2
        cx, cy = center_xy
        zx, zy = click_xy
        return (cx - half) + zx / zoom_factor, (cy - half) + zy / zoom_factor

    def make_click_handler(side):
        def handler(event, x, y, flags, param):
            if event != cv2.EVENT_LBUTTONDOWN:
                return
            i = review_idx[cursor[0]]
            pts = pts_a if side == "a" else pts_b
            cx, cy = int(round(pts[i, 0])), int(round(pts[i, 1]))
            pts[i] = click_to_source((cx, cy), (x, y))
            active_side[0] = side
            draw()
        return handler

    cv2.setMouseCallback(win_a, make_click_handler("a"))
    cv2.setMouseCallback(win_b, make_click_handler("b"))

    print("Controls: click = move point | arrows = nudge | n/p = next/prev | r = reset | d = delete | q/ESC = finish")
    draw()

    while True:
        key = cv2.waitKeyEx(20)
        if key == -1:
            continue
        i = review_idx[cursor[0]]
        if key in (ord('q'), 27):
            break
        elif key == ord('n'):
            cursor[0] = (cursor[0] + 1) % len(review_idx)
            draw()
        elif key == ord('p'):
            cursor[0] = (cursor[0] - 1) % len(review_idx)
            draw()
        elif key == ord('r'):
            pts_a[i] = orig_a[i]
            pts_b[i] = orig_b[i]
            draw()
        elif key == ord('d'):
            if n <= 4:
                print(f"  Cannot delete point {i + 1}: only {n} point(s) left (homography needs at least 4).")
            else:
                print(f"  Deleting point {i + 1} — it's an outlier, so this doesn't change the current fit; "
                      f"it just stops it from being re-reviewed.")
                pts_a = np.delete(pts_a, i, axis=0)
                pts_b = np.delete(pts_b, i, axis=0)
                orig_a = np.delete(orig_a, i, axis=0)
                orig_b = np.delete(orig_b, i, axis=0)
                if inlier_mask is not None:
                    inlier_mask = np.delete(inlier_mask, i)
                review_idx = [(j - 1 if j > i else j) for j in review_idx if j != i]
                n -= 1
                if not review_idx:
                    print("  No more points to review.")
                    break
                cursor[0] = min(cursor[0], len(review_idx) - 1)
                draw()
        elif key in ARROW:
            ddx, ddy = ARROW[key]
            pts = pts_a if active_side[0] == "a" else pts_b
            pts[i, 0] += ddx
            pts[i, 1] += ddy
            draw()

    cv2.destroyWindow(win_a)
    cv2.destroyWindow(win_b)
    return pts_a.astype(np.float32), pts_b.astype(np.float32)


def save_homography(H, output_path):
    """Save homography matrix to a text file."""
    np.savetxt(str(output_path), H)
    print(f"Saved homography to {output_path}")


def save_annotated_image(image_path, pixel_points, output_path,
                         point_labels=None, color=(0, 0, 255), text_color=(0, 255, 255)):
    """
    Save a copy of an image with numbered point markers drawn on it.

    :param image_path [str or Path]               Source image.
    :param pixel_points [(N, 2) array-like]       Point positions in image pixels.
    :param output_path [str or Path]              Where to save the annotated copy.
    :param point_labels [list of str, optional]   Per-point labels. If None, uses "1", "2", ...
    :param color, text_color : BGR tuples         Marker and label colors.
    """
    img = load_image(image_path)
    h, w = img.shape[:2]
    radius = max(3, int(max(w, h)) // 800)
    thickness = max(1, int(max(w, h)) // 1600)
    font_scale = max(0.4, int(max(w, h)) / 3000)
    for i, (x, y) in enumerate(pixel_points):
        x, y = int(round(x)), int(round(y))
        cv2.circle(img, (x, y), radius, color, -1)
        label = str(i + 1) if point_labels is None else str(point_labels[i])
        cv2.putText(img, label, (x + radius + 2, y - radius - 2),
                    cv2.FONT_HERSHEY_SIMPLEX, font_scale, text_color,
                    thickness, cv2.LINE_AA)
    save_image(img, output_path)

    return output_path


def calibrate_video(video_path, frame_index=0, ref_image_path=None, coords=None):
    """
    Compute and save the homography for a fixed camera.

    Click reference points on the video frame (or a provided reference image) and type their
    real-world X,Y positions in meters in a local coordinate system.

    :param video_path      [str or Path]        Path to the input video.
    :param frame_index     [int]                Video frame to use as the reference image.
    :param ref_image_path  [str or Path|None]   Reference image to pick points on instead of the video frame.
                                                If provided, the same points must then be clicked on the video frame too.
    :param coords          [list of (x,y)|None] Pre-supplied X,Y coordinates in meters (skips interactive entry).

    :return H [np.ndarray (3, 3)]  Homography matrix mapping video pixels → world metres.
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")
    if ref_image_path is not None:
        ref_image_path = Path(ref_image_path)
        if not ref_image_path.exists():
            raise FileNotFoundError(f"Reference image not found: {ref_image_path}")

    base = video_path.with_suffix("")
    output_homography = base.with_name(f"{base.name}_homography_f{frame_index}.txt")
    output_video_frame = base.with_name(f"{base.name}_reference_f{frame_index}.png")
    output_csv = base.with_name(f"{base.name}_refpoints_f{frame_index}.csv")

    get_frame(video_path, output_video_frame, frame_index)

    pick_image = ref_image_path if ref_image_path is not None else output_video_frame
    annotated_out = base.with_name(f"{base.name}_reference_annotated_f{frame_index}.png")
    pixel_ref, world_points = pick_real_points(
        pick_image, coords=coords, annotated_output_path=annotated_out)
    n = len(pixel_ref)

    if ref_image_path is not None:
        print()
        print("=" * 70)
        print(f"VIDEO FRAME PICKING — click the same {n} points")
        print("=" * 70)
        print(f"Click the same {n} points on the video frame, in the SAME ORDER.")
        print()
        pixel_video = pick_pixel_points(output_video_frame)
        if len(pixel_video) != n:
            raise ValueError(f"Clicked {len(pixel_video)} points on video, expected {n}.")
        save_annotated_image(output_video_frame, pixel_video, output_video_frame)
    else:
        pixel_video = pixel_ref

    H, inlier_mask, errors, _, _ = compute_homography(pixel_video, world_points)
    save_homography(H, output_homography)

    span_x = world_points[:, 0].max() - world_points[:, 0].min()
    span_y = world_points[:, 1].max() - world_points[:, 1].min()
    print(f"\nReference points span: {span_x:.1f} x {span_y:.1f} m")

    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["point_id", "x_world_m", "y_world_m",
                         "x_pixel_video", "y_pixel_video", "homography_inlier", "reprojection_error"])
        for i in range(n):
            writer.writerow([i + 1,
                             f"{world_points[i, 0]:.4f}", f"{world_points[i, 1]:.4f}",
                             f"{pixel_video[i, 0]:.2f}", f"{pixel_video[i, 1]:.2f}",
                             int(inlier_mask[i]), f"{errors[i]:.3f}"])
    print(f"Saved per-point log: {output_csv}")

    return H


def validate_video(video_path, frame_index=0, ref_image_path=None,
                   coords=None, validation_tag=""):
    """
    Validate an existing homography using hold-out reference points.

    :param video_path      [str or Path]        Path to the video.
    :param frame_index     [int]                Frame index used during calibration.
    :param ref_image_path  [str or Path|None]   Reference image to pick points on instead of the video frame.
    :param coords          [list of (x,y)|None] Pre-supplied X,Y coordinates in meters.
    :param validation_tag  [str]                Appended to the output filename (e.g. "_holdout").

    :return errors [np.ndarray (N,)]            Per-point reprojection error in metres.
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")

    base = video_path.with_suffix("")
    homography_path = base.with_name(f"{base.name}_homography_f{frame_index}.txt")
    frame_path = base.with_name(f"{base.name}_reference_f{frame_index}.png")
    if not homography_path.exists():
        raise FileNotFoundError(
            f"Homography file not found: {homography_path}\nRun calibrate_video() first.")
    if not frame_path.exists():
        raise FileNotFoundError(
            f"Reference frame not found: {frame_path}\nMake sure frame_index matches calibration.")

    H = np.loadtxt(str(homography_path))
    if H.shape != (3, 3):
        raise ValueError(f"Expected 3x3 matrix, got {H.shape}")

    if ref_image_path is not None:
        ref_image_path = Path(ref_image_path)
        _, world_points = pick_real_points(ref_image_path, coords=coords)
        n = len(world_points)
        print(f"\nNow click the same {n} validation points on the video frame:")
        pixel_video = pick_pixel_points(frame_path)
        if len(pixel_video) != n:
            raise ValueError(f"Clicked {len(pixel_video)} points on video, expected {n}.")
    else:
        pixel_video, world_points = pick_real_points(frame_path, coords=coords)

    projected = pixel_to_world(H, pixel_video)
    errors = np.linalg.norm(projected - world_points, axis=1)
    print(f"\n--- Validation reprojection errors ---")
    print(f"  Mean : {errors.mean():.3f} m")
    print(f"  Max  : {errors.max():.3f} m")
    print(f"  RMSE : {np.sqrt((errors ** 2).mean()):.3f} m")
    for i, e in enumerate(errors):
        print(f"  point {i+1}: {e:.3f} m")

    validation_path = base.with_name(
        f"{base.name}_validation_f{frame_index}{validation_tag}.npz")
    np.savez(str(validation_path),
             pixel_points=pixel_video,
             world_points=world_points,
             errors=errors)
    print(f"\nSaved validation report: {validation_path}")

    return errors

# Object marking (points/lines/polygons for lanes, manholes, stop lines, poles, etc.) --------------------------------
# All objects (lanes included) are arbitrary-vertex points/lines/polygons in the ortho-pixel coordinate space
LANE_COLORS = [
    (255, 80, 80), (80, 220, 80), (80, 160, 255), (0, 210, 255),
    (255, 80, 220), (80, 255, 220), (180, 100, 255), (255, 180, 0),
]
GEOMETRY_MIN_VERTICES = {"point": 1, "line": 2, "polygon": 3}


def object_color(obj_type, type_colors):
    """BGR color for an object, by its type. type_colors: {obj_type: (b,g,r), ...}; unlisted types are black."""
    return type_colors.get(obj_type, (0, 0, 0))


def object_style(section, obj_type, type_colors, section_alphas):
    """
    BGR color (by object type) + alpha (by section).
    type_colors: {obj_type: (b,g,r), ...} matched against `obj_type`; unlisted types are black.
    section_alphas: {label: alpha, ...} — matched against `section` first, then as a substring
    of `obj_type`; use key "default" for the fallback alpha (0.45 if not given).
    """
    color = object_color(obj_type, type_colors)
    if section in section_alphas:
        return color, section_alphas[section]
    for label, alpha in section_alphas.items():
        if label in obj_type:
            return color, alpha
    return color, section_alphas.get("default", 0.45)


def mark_objects(image_path, objects_json_path, H=None, max_display_size=None, zoom_size=200, zoom_factor=8,
                 zoom_window_size=(500, 500), point_radius=4, snap_radius_px=10):
    """
    Interactive tool to mark point/line/polygon objects (lanes, manholes, stop lines, poles, etc.) on an image.
        Loads existing objects from objects_json_path if it exists (to add to / delete from), or
        starts from scratch otherwise.

    Controls:
        Left-click in EITHER window  -> set a pending vertex (yellow, or magenta if snapped)
        Arrow keys                   -> nudge the pending vertex by 1px
        Enter                        -> confirm the pending vertex, adding it to the current object
        u                            -> undo: pending vertex, else last confirmed vertex, else last saved object
        p                            -> save current object as a POINT (needs 1 vertex)
        l                            -> save current object as a LINE (needs >=2 vertices)
        a                            -> save current object as an AREA/polygon (needs >=3 vertices)
        d                            -> pick an existing object (by number) to delete
        ESC                          -> finish (any unsaved in-progress object is discarded)

    Saving an object (p/l/a) prompts for a free-text Section (e.g. travel direction; blank allowed) and
    Type (e.g. "lane", "tree", "signal"); the id is auto-assigned as 1, 2, 3, ... per type, in the order
    objects of that type were defined.

    :param image_path [str or Path]                Image to click on (e.g. the ortho PNG, or a drone's frame).
    :param objects_json_path [str or Path]         Existing objects JSON to load (if present) and,
                                                    once edited, overwrite with the result.
    :param H [np.ndarray, shape (3, 3), optional]  Homography matrix. If None, image_path's own pixel
                                                    space is used as-is (e.g. when marking directly on
                                                    the ortho image, which IS the reference/world space).
    :param max_display_size [(int, int), optional] Display size cap in screen pixels; auto-detected if None.
    :param zoom_size [int]                         Side length (source px) of the zoomed region.
    :param zoom_factor [int]                       Magnification factor.
    :param zoom_window_size [(int, int)]           Display size of the zoom window, in screen pixels.
    :param point_radius [int]                      Radius, in pixels, of vertex markers.
    :param snap_radius_px [int]                    Source-image-pixel radius for snapping onto an
                                                    existing object's vertex (in image_path's pixel space).

    :return objects [list of dict]   Each: {"section": str, "type": str, "id": str,
                                    "geometry": "point"|"line"|"polygon", "points": [[x, y], ...]}
                                    in the coordinate space H maps to (or image_path's own if H is None).
    """
    img = load_image(image_path)
    H = np.asarray(H, dtype=np.float64) if H is not None else None
    H_inv = np.linalg.inv(H) if H is not None else None
    window_name = "Mark objects (p=point, l=line, a=area, d=delete, u=undo, ESC=finish)"
    zoom_window_name = "Zoom"
    h, w = img.shape[:2]
    img_display, win_w, win_h, to_img, to_disp = setup_click_window(img, max_display_size)
    sx, sy = win_w / w, win_h / h
    box_thick = max(1, win_w // 800)
    crosshair_thick = max(2, zoom_factor // 2)
    objects_json_path = Path(objects_json_path)
    objects = json.loads(objects_json_path.read_text(encoding="utf-8")) if objects_json_path.exists() else []
    current_points = []
    cursor_xy = [win_w // 2, win_h // 2]
    prev_xy = [win_w // 2, win_h // 2]
    zoom_state = [zoom_size]
    pending: list[int | None] = [None, None]
    pending_snapped = [False]

    def obj_label(obj):
        return "/".join(p for p in (obj["section"], obj["type"], obj["id"]) if p)

    def frame_points(obj):
        pts = np.array(obj["points"], dtype=np.float64)
        return np.atleast_2d(pixel_to_world(H_inv, pts) if H_inv is not None else pts)

    def existing_vertices():
        pts = [tuple(p) for obj in objects for p in frame_points(obj)]
        pts += [tuple(p) for p in current_points]
        return pts

    def find_snap(ix, iy):
        return find_nearest_point(ix, iy, existing_vertices(), snap_radius_px)

    def set_pending(ix, iy):
        apply_pending(ix, iy, w, h, to_disp, pending, cursor_xy,
                      snap_fn=find_snap, snapped_flag=pending_snapped)

    def draw_main():
        sz = zoom_state[0]
        display = img_display.copy()
        overlay = display.copy()

        def draw_sane_runs(pts_disp, sane, color, thickness):
            """Draw only the contiguous run(s) of sane vertices, as open polylines."""
            run = []
            for vj, vj_sane in enumerate(sane):
                if vj_sane:
                    run.append(pts_disp[vj])
                    continue
                if len(run) >= 2:
                    cv2.polylines(display, [np.array(run, dtype=np.int32)], isClosed=False,
                                 color=color, thickness=thickness, lineType=cv2.LINE_AA)
                run = []
            if len(run) >= 2:
                cv2.polylines(display, [np.array(run, dtype=np.int32)], isClosed=False,
                             color=color, thickness=thickness, lineType=cv2.LINE_AA)

        for i, obj in enumerate(objects):
            color = LANE_COLORS[i % len(LANE_COLORS)]
            pts_frame = frame_points(obj)
            # A vertex near/past the homography's vanishing line projects to a huge or non-finite
            # pixel position (garbage once cast to int32). Draw only the contiguous run(s) of
            # vertices that project sanely, instead of drawing whatever comes out for all of them.
            sane = [np.isfinite(p).all() and abs(p[0]) < 10 * w and abs(p[1]) < 10 * h for p in pts_frame]
            if not any(sane):
                continue
            pts_disp = [to_disp(*p) for p in pts_frame]
            all_sane = all(sane)
            geometry = obj["geometry"]
            if geometry == "point":
                if sane[0]:
                    cv2.circle(display, tuple(np.array(pts_disp[0], dtype=np.int32)), point_radius + 2, color, -1)
            elif geometry == "line":
                if all_sane:
                    pts = np.array(pts_disp, dtype=np.int32)
                    cv2.polylines(display, [pts], isClosed=False, color=color, thickness=2, lineType=cv2.LINE_AA)
                else:
                    draw_sane_runs(pts_disp, sane, color, 2)
            else:
                if all_sane:
                    pts = np.array(pts_disp, dtype=np.int32)
                    cv2.fillPoly(overlay, [pts], color)
                    cv2.polylines(display, [pts], isClosed=True, color=color, thickness=2, lineType=cv2.LINE_AA)
                else:
                    draw_sane_runs(pts_disp, sane, color, 2)
            sane_pts_disp = [p for p, s in zip(pts_disp, sane) if s]
            lx, ly = np.mean(sane_pts_disp, axis=0).astype(int)
            cv2.putText(display, obj_label(obj), (lx, ly),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2, cv2.LINE_AA)
        cv2.addWeighted(overlay, 0.25, display, 0.75, 0, dst=display)

        disp_pts = [to_disp(*p) for p in current_points]
        for i, (dx, dy) in enumerate(disp_pts):
            cv2.circle(display, (dx, dy), point_radius, (0, 255, 255), -1)
            cv2.putText(display, str(i + 1), (dx + 6, dy - 6),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 255), 1, cv2.LINE_AA)
        if disp_pts:
            cv2.polylines(display, [np.array(disp_pts, dtype=np.int32)], isClosed=False,
                          color=(0, 255, 255), thickness=2, lineType=cv2.LINE_AA)

        if pending[0] is not None:
            # noinspection PyTypeChecker
            dx, dy = to_disp(pending[0], pending[1])
            color = (255, 0, 255) if pending_snapped[0] else (0, 255, 255)
            cv2.circle(display, (dx, dy), point_radius + 2, color, 2)
            cv2.putText(display, f">{len(current_points) + 1}", (dx + 8, dy - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)
            if disp_pts:
                cv2.line(display, disp_pts[-1], (dx, dy), color, 1, cv2.LINE_AA)
            if len(current_points) + 1 >= GEOMETRY_MIN_VERTICES["polygon"]:
                cv2.line(display, (dx, dy), disp_pts[0] if disp_pts else (dx, dy),
                        (0, 150, 150), 1, cv2.LINE_AA)
        elif disp_pts:
            # Rubber-band preview to the live cursor, and a faint closing line back to the first vertex
            cv2.line(display, disp_pts[-1], tuple(cursor_xy), (0, 255, 255), 1, cv2.LINE_AA)
            if len(disp_pts) >= GEOMETRY_MIN_VERTICES["polygon"]:
                cv2.line(display, disp_pts[-1], disp_pts[0], (0, 150, 150), 1, cv2.LINE_AA)

        draw_zoom_box(display, cursor_xy, sz, sx, sy, box_thick)
        cv2.imshow(window_name, display)

    def draw_zoom():
        # noinspection PyTypeChecker
        zoom_img, to_zoom, in_view = zoom_crop(img, cursor_xy, to_img, zoom_state[0], zoom_factor)
        if zoom_img is None:
            return
        for obj in objects:
            for px, py in frame_points(obj):
                if in_view(px, py):
                    lx, ly = to_zoom(px, py)
                    cv2.circle(zoom_img, (lx, ly), 6, (255, 255, 255), -1)
        for i, (px, py) in enumerate(current_points):
            if in_view(px, py):
                lx, ly = to_zoom(px, py)
                cv2.circle(zoom_img, (lx, ly), 6, (0, 255, 255), -1)
                cv2.putText(zoom_img, str(i + 1), (lx + 10, ly - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 255), 2)
        if pending[0] is not None:
            lx, ly = to_zoom(*pending)
            color = (255, 0, 255) if pending_snapped[0] else (0, 255, 255)
            cv2.circle(zoom_img, (lx, ly), 10, color, 2)

        col = (255, 0, 255) if pending[0] is not None and pending_snapped[0] else \
              ((0, 255, 255) if pending[0] is not None else (0, 255, 0))
        draw_crosshair(zoom_img, crosshair_thick, col)
        cv2.imshow(zoom_window_name, zoom_img)

    on_main_event, on_zoom_click = make_mouse_handlers(
        to_img, set_pending, draw_main, draw_zoom, pending, cursor_xy, prev_xy, zoom_state, zoom_factor)

    def confirm_pending():
        confirm_pending_vertex(pending, pending_snapped, current_points)

    def finish_object(geometry, min_vertices, label):
        if len(current_points) < min_vertices:
            print(f"  Need at least {min_vertices} vertex/vertices for a {label} (have {len(current_points)}).")
            return
        flush_stdin()
        section = input(f"  Object {len(objects) + 1} - Section: ").strip()
        flush_stdin()
        obj_type = input(f"  Object {len(objects) + 1} - Type: ").strip()
        obj_id = str(sum(1 for o in objects if o["type"] == obj_type and o["section"] == section) + 1)
        pts = np.array(current_points, dtype=np.float64)
        mapped_pts = np.atleast_2d(pixel_to_world(H, pts) if H is not None else pts)
        obj = {"section": section, "type": obj_type, "id": obj_id, "geometry": geometry,
              "points": [[float(x), float(y)] for x, y in mapped_pts]}
        objects.append(obj)
        print(f"  Added {label} '{obj_label(obj)}' with {len(current_points)} vertex/vertices.")
        current_points.clear()

    cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
    cv2.namedWindow(zoom_window_name, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(window_name, win_w, win_h)
    cv2.resizeWindow(zoom_window_name, zoom_window_size[0], zoom_window_size[1])
    cv2.setMouseCallback(window_name, on_main_event)
    cv2.setMouseCallback(zoom_window_name, on_zoom_click)
    draw_main()
    draw_zoom()

    print("Click to place a vertex, Enter to confirm. p/l/a = save as point/line/area, "
          "d = delete an object, ESC finishes.")
    print("Controls: left-click/zoom-click = place | arrows = nudge | u = undo | scroll = zoom")

    while True:
        key = cv2.waitKeyEx(20)
        if key == 27:
            if pending[0] is not None:
                pending[0] = pending[1] = None
                pending_snapped[0] = False
                draw_main()
                draw_zoom()
            else:
                if current_points:
                    print(f"  Discarding unsaved object ({len(current_points)} vertices).")
                break
        elif key == ord('p'):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                pending_snapped[0] = False
            else:
                finish_object("point", GEOMETRY_MIN_VERTICES["point"], "point")
            draw_main()
            draw_zoom()
        elif key == ord('l'):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                pending_snapped[0] = False
            else:
                finish_object("line", GEOMETRY_MIN_VERTICES["line"], "line")
            draw_main()
            draw_zoom()
        elif key == ord('a'):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                pending_snapped[0] = False
            else:
                finish_object("polygon", GEOMETRY_MIN_VERTICES["polygon"], "polygon/area")
            draw_main()
            draw_zoom()
        elif key == ord('d'):
            if not objects:
                print("  No objects to delete.")
            else:
                for i, obj in enumerate(objects):
                    print(f"    {i + 1}. {obj_label(obj)} ({obj['geometry']})")
                flush_stdin()
                sel = input(f"  Delete which number (1-{len(objects)}, blank to cancel): ").strip()
                if sel.isdigit() and 1 <= int(sel) <= len(objects):
                    removed = objects.pop(int(sel) - 1)
                    print(f"  Deleted: {obj_label(removed)}")
                else:
                    print("  Cancelled.")
            draw_main()
            draw_zoom()
        elif key in (13, 10):
            confirm_pending()
            draw_main()
            draw_zoom()
        elif key == ord('u'):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                pending_snapped[0] = False
            elif current_points:
                removed = current_points.pop()
                print(f"  Removed vertex: {removed}")
            elif objects:
                removed = objects.pop()
                print(f"  Removed object: {obj_label(removed)}")
            draw_main()
            draw_zoom()
        elif key in ARROW and pending[0] is not None:
            ddx, ddy = ARROW[key]
            # noinspection PyTypeChecker
            set_pending(pending[0] + ddx, pending[1] + ddy)
            draw_main()
            draw_zoom()

    cv2.destroyWindow(window_name)
    cv2.destroyWindow(zoom_window_name)
    print(f"{len(objects)} object(s) defined.")
    save_json_list(objects, objects_json_path, "object(s)")

    return objects


def refine_objects(image_path, H, objects_json_path, type_colors=None, section_alphas=None,
                   max_display_size=None, zoom_size=200, zoom_factor=8, zoom_window_size=(500, 500),
                   point_radius=6, snap_tol=1.0, snap_radius_px=2, gsd_m_per_px=None,
                   warn_threshold_m=0.3, edge_ratio_warn=5.0):
    """
    Edit marked objects (points/lines/polygons) based on a video frame.

    Controls:
        Left-click + hold + drag on a vertex of the CURRENT   -> select it and move it (and any
        object                                                   object(s) sharing it). A plain click
                                                                  (even with the small jitter real clicks
                                                                  have) only selects — nothing moves.
        Arrow keys                                            -> nudge the selected vertex by 1px
        d                                                     -> delete the selected vertex; if the
                                                                  object is already at its geometry's
                                                                  minimum (1 for point, 2 for line, 3
                                                                  for polygon), deletes the whole object
        e / E                                                -> toggle extend mode (line/polygon only):
                                                                  click places a PENDING vertex — snapped
                                                                  onto a nearby existing vertex if one is
                                                                  close — appending (e) or prepending (E)
                                                                  to the CURRENT object. Arrows nudge the
                                                                  pending point; Enter confirms it (stays
                                                                  in extend mode for the next one); u/Esc
                                                                  cancels it. Clicking again re-aims it.
        r                                                     -> attempt an automatic re-splice of the
                                                                  active object's isolated bad chain.
        u                                                     -> cancel a pending point if one is staged,
                                                                  else revert the last 'r' repair, else undo
                                                                  the last CONFIRMED extend vertex
                                                                  (repeatable); cleared on n/p or a
                                                                  whole-object delete
        n / p                                                -> next / previous object (only objects
                                                                  with >=1 vertex visible in this frame)
        q / ESC                                              -> cancel a pending point if one is staged,
                                                                  else finish, keeping all edits made so far

    A vertex nudged more than warn_threshold_m from where it was marked on the ortho gets a red ring
    on-screen, a console warning, and a line in the closing summary.

    Active-object vertex ring colors: red = over warn_threshold_m (above), orange = selected, green =
    linked (within snap_tol of another object's vertex), white = none of the above.
    Other objects' vertices also show as cyan dots in the zoom window, for snapping onto them at zoom precision.

    :param image_path [str or Path]            Image to refine against (e.g. the master frame, or the
                                                ortho PNG itself).
    :param H [np.ndarray, shape (3, 3), optional]  Homography matrix (drone-pixel -> ortho-pixel). None
                                                when image_path's own pixel space IS the ortho/world
                                                space (e.g. refining directly on the ortho PNG).
    :param objects_json_path [str or Path]     Objects JSON to load (as saved by mark_objects) and,
                                                once refined, overwrite with the result.
    :param type_colors, section_alphas         See object_style(). Defaults to {} (all objects black).
    :param max_display_size [(int, int), optional]  Display size cap in screen pixels; auto-detected if None.
    :param zoom_size, zoom_factor [int]        Magnifier crop size (source px) / magnification factor,
                                                shown alongside the full frame for drag precision.
    :param zoom_window_size [(int, int)]      Display size of the magnifier window, in screen pixels.
    :param point_radius [int]                  Radius, in pixels, of vertex markers / drag hit-radius.
    :param snap_tol [float]                    Ortho-pixel distance below which two vertices are treated
                                                as the same shared point (so moving one moves both).
    :param snap_radius_px [int]                 Display-pixel click radius for snapping a new (extend-
                                                mode) point onto an existing vertex — see find_snap_target().
    :param gsd_m_per_px [float, optional]      Ortho ground sampling distance (m/px). If given, the
                                                diff-from-ortho warning is reported in meters; otherwise
                                                in ortho pixels (and warn_threshold_m is then read as px).
    :param warn_threshold_m [float]            Distance a vertex can be nudged from its ortho-marked
                                                position before it's flagged (see above).
    :param edge_ratio_warn [float]              How many times an object's median edge length a vertex's
                                                connecting edge can be before it's flagged (see above).

    :return objects [list of dict]   The same list, with any adjusted vertex positions.
    """
    type_colors = type_colors or {}
    section_alphas = section_alphas or {}
    img = load_image(image_path, "video frame")
    h, w = img.shape[:2]
    H = np.asarray(H, dtype=np.float64) if H is not None else None
    H_inv = np.linalg.inv(H) if H is not None else None
    objects_json_path = Path(objects_json_path)
    objects = json.loads(objects_json_path.read_text(encoding="utf-8"))
    objects_before = json.loads(json.dumps(objects))
    orig_idx_of = list(range(len(objects)))   # current objects index -> its index in objects_before
    # orig_pts[orig_idx_of[oi]][vi] is vertex vi's as-marked ortho position; kept in lockstep with
    # objects[oi]["points"] across vertex deletions so index vi always refers to the same vertex.
    orig_pts = [[list(p) for p in obj["points"]] for obj in objects_before]
    warn_unit = "m" if gsd_m_per_px is not None else "px"

    def diff_from_ortho(oi, vi):
        """(distance, over_threshold) of vertex vi's current position vs. its as-marked ortho position."""
        diff_px = float(np.linalg.norm(
            np.array(objects[oi]["points"][vi], dtype=np.float64) -
            np.array(orig_pts[orig_idx_of[oi]][vi], dtype=np.float64)))
        diff = diff_px * gsd_m_per_px if gsd_m_per_px is not None else diff_px
        return diff, diff > warn_threshold_m

    def edge_ratio(oi, vi):
        pts = objects[oi]["points"]
        n = len(pts)
        pairs = [(i, i + 1) for i in range(n - 1)]
        if objects[oi]["geometry"] == "polygon":
            pairs.append((n - 1, 0))
        lengths = [np.linalg.norm(np.array(pts[a], dtype=np.float64) - np.array(pts[b], dtype=np.float64))
                  for a, b in pairs]
        touching = [length for (a, b), length in zip(pairs, lengths) if vi in (a, b)]
        other = [length for (a, b), length in zip(pairs, lengths) if vi not in (a, b)]
        if not touching or len(other) < 3:
            return None
        med_other = float(np.median(other))
        return (max(touching) / med_other) if med_other > 0 else None

    def detect_bad_chain(oi):
        obj = objects[oi]
        pts = obj["points"]
        n = len(pts)
        if obj["geometry"] != "polygon" or n < 6:
            return None
        arr = np.array(pts, dtype=np.float64)
        edges = np.linalg.norm(np.diff(arr, axis=0), axis=1)   # edges[i]: pts[i] -> pts[i+1]
        closing = np.linalg.norm(arr[0] - arr[-1])               # pts[n-1] -> pts[0]
        all_edges = np.append(edges, closing)                    # all_edges[i]: pts[i] -> pts[(i+1) % n]
        med = np.median(all_edges)
        if med <= 0:
            return None
        bad = np.where(all_edges > med * edge_ratio_warn)[0]
        if len(bad) != 2:
            return None
        i, j = sorted(bad.tolist())
        return i + 1, j + 1   # isolated chain is pts[i+1 .. j] inclusive == pts[i+1:j+1]

    def resplice_object(oi):
        """Auto-repair objects"""
        chain = detect_bad_chain(oi)
        if chain is None:
            return False, "No isolated bad chain detected on this object (nothing obvious to auto-repair)."
        cs, ce = chain
        pts = objects[oi]["points"]
        n = len(pts)
        rest_idx = list(range(0, cs)) + list(range(ce, n))
        chain_idx = list(range(cs, ce))
        if len(rest_idx) < 3:
            return False, "Not enough of the object left outside the bad chain to re-splice against."
        rest = np.array([pts[i] for i in rest_idx], dtype=np.float64)
        chain_pts = np.array([pts[i] for i in chain_idx], dtype=np.float64)

        d = np.linalg.norm(rest[:, None, :] - chain_pts[None, :, :], axis=2).min(axis=1)
        nearest = int(np.argmin(d))

        def junction_score(insert_after, order):
            seq = np.vstack([rest[:insert_after + 1], chain_pts[order], rest[insert_after + 1:]])
            seq_closed = np.vstack([seq, seq[:1]])
            v = np.diff(seq_closed, axis=0)
            norms = np.linalg.norm(v, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            v = v / norms
            cosang = np.sum(v[:-1] * v[1:], axis=1).clip(-1, 1)
            angles = np.degrees(np.arccos(cosang))
            j1 = insert_after
            j2 = min(insert_after + len(order), len(angles) - 1)
            return max(angles[j1], angles[j2])

        order_as_is = list(range(len(chain_idx)))
        order_reversed = order_as_is[::-1]
        candidates = []
        for insert_after in {max(0, nearest - 1), nearest}:
            for order in (order_as_is, order_reversed):
                candidates.append((junction_score(insert_after, order), insert_after, order))
        candidates.sort(key=lambda c: c[0])
        best_score, best_insert_after, best_order = candidates[0]
        reversed_chosen = best_order is order_reversed
        runner_up_score = candidates[1][0]   # 4 candidates always (2 insert positions x 2 orientations)

        new_order = (rest_idx[:best_insert_after + 1] + [chain_idx[k] for k in best_order]
                    + rest_idx[best_insert_after + 1:])

        old_points = objects[oi]["points"]
        old_lonlat = objects[oi].get("lonlat")
        objects[oi]["points"] = [old_points[k] for k in new_order]
        if old_lonlat is not None and len(old_lonlat) == n:
            objects[oi]["lonlat"] = [old_lonlat[k] for k in new_order]
        op = orig_pts[orig_idx_of[oi]]
        if len(op) == n:
            orig_pts[orig_idx_of[oi]] = [op[k] for k in new_order]

        msg = (f"Re-spliced {len(chain_idx)} vertices (orig. {cs + 1}-{ce}) after original vertex "
              f"{rest_idx[best_insert_after] + 1}, {'reversed' if reversed_chosen else 'as-is'} order "
              f"(junction angle {best_score:.1f}° vs {runner_up_score:.1f}° next-best — look at the shape).")
        return True, msg

    def frame_pt(oi, vi):
        pt = np.array(objects[oi]["points"][vi], dtype=np.float64)
        return pixel_to_world(H_inv, pt) if H_inv is not None else pt

    def frame_of(ortho_xy):
        """Ortho-pixel point (e.g. `pending`) -> image_path's own pixel space, same as frame_pt()."""
        pt = np.array(ortho_xy, dtype=np.float64)
        return pixel_to_world(H_inv, pt) if H_inv is not None else pt

    def to_ortho(pixel_xy):
        """image_path's own pixel -> ortho-pixel space (identity when H is None, e.g. image_path IS the ortho)."""
        return pixel_to_world(H, np.asarray(pixel_xy, dtype=np.float64)) if H is not None else np.asarray(pixel_xy, dtype=np.float64)

    def label(oi):
        obj = objects[oi]
        return "/".join(p for p in (obj["section"], obj["type"], obj["id"]) if p)

    # Objects with >=1 vertex visible in this frame
    obj_order = []
    for idx, obj in enumerate(objects):
        if any(0 <= (p := frame_pt(idx, vi))[0] < w and 0 <= p[1] < h for vi in range(len(obj["points"]))):
            obj_order.append(idx)
    if not obj_order:
        print("No objects visible in this frame.")
        return objects
    print(f"Refining {len(obj_order)} object(s) visible in this frame.")

    img_display, win_w, win_h, to_img, to_disp = setup_click_window(img, max_display_size)
    sx, sy = win_w / w, win_h / h
    box_thick = max(1, win_w // 800)
    win, zoom_win = "Object refinement (video frame)", "Zoom"
    cv2.namedWindow(win, cv2.WINDOW_NORMAL)
    cv2.namedWindow(zoom_win, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(win, win_w, win_h)
    cv2.resizeWindow(zoom_win, *zoom_window_size)

    cursor = [0]                    # index into obj_order
    cursor_xy = [win_w // 2, win_h // 2]
    # None, or [oi, vi, [(oj, vj), ...], (down_disp_x, down_disp_y), moved]. `moved` starts False and
    # only flips True once the cursor has moved past DRAG_THRESHOLD_PX from the mousedown point — a
    # plain click (even with the few px of jitter real clicks always have) then never nudges anything.
    dragging: list[list | None] = [None]
    DRAG_THRESHOLD_PX = 3
    selected: list[int | None] = [None]   # vertex index (in the active object) that arrows/d act on
    extend: list[str | None] = [None]     # None, "end", or "start" — extend-mode target end of the active object
    extend_undo: list[tuple[int, str]] = []   # stack of (oi, "end"|"start") per confirmed extend vertex, in order
    pending: list[float | None] = [None, None]   # ortho-pixel coords of an extend-mode point awaiting Enter
    pending_snapped: list[bool] = [False]
    repair_undo: list[tuple[int, list, list | None, list] | None] = [None]   # one-level snapshot for 'r'
    crosshair_thick = max(2, zoom_factor // 2)

    def find_linked(oi, vi):
        p = np.array(objects[oi]["points"][vi], dtype=np.float64)
        linked = []
        for oj, obj in enumerate(objects):
            for vj, pt in enumerate(obj["points"]):
                if (oj, vj) == (oi, vi):
                    continue
                if np.linalg.norm(np.array(pt, dtype=np.float64) - p) <= snap_tol:
                    linked.append((oj, vj))
        return linked

    def find_snap_target(disp_x, disp_y):
        """Nearest existing vertex (any object) within snap_radius_px of the click, in display-pixel space"""
        best, best_d2 = None, snap_radius_px ** 2
        for oj, obj in enumerate(objects):
            for vj, pt in enumerate(obj["points"]):
                fx, fy = frame_pt(oj, vj)
                if not np.isfinite([fx, fy]).all():
                    continue
                dx, dy = to_disp(fx, fy)
                d2 = (dx - disp_x) ** 2 + (dy - disp_y) ** 2
                if d2 <= best_d2:
                    best, best_d2 = pt, d2
        return best

    def set_ortho_pt(oi, vi, ox, oy):
        objects[oi]["points"][vi] = [float(ox), float(oy)]

    def linked_mask(oi):
        """Vectorized: for each vertex of objects[oi], is it within snap_tol of any OTHER object's vertex?"""
        active_pts = np.array(objects[oi]["points"], dtype=np.float64)
        other_pts = np.array([pt for oj2, obj2 in enumerate(objects) if oj2 != oi for pt in obj2["points"]],
                             dtype=np.float64)
        if len(other_pts) == 0:
            return np.zeros(len(active_pts), dtype=bool)
        dists = np.linalg.norm(active_pts[:, None, :] - other_pts[None, :, :], axis=2)
        return (dists <= snap_tol).any(axis=1)

    def status_line():
        oi = obj_order[cursor[0]]
        obj = objects[oi]
        line = f"{label(oi)} ({obj['geometry']}) ({cursor[0] + 1}/{len(obj_order)}) — {len(obj['points'])} vertex/vertices"
        if extend[0] is not None:
            line += f" | EXTEND:{extend[0]}"
            if pending[0] is not None:
                line += " | PENDING" + (" [snapped]" if pending_snapped[0] else "") + \
                        " — Enter=confirm, arrows=nudge, u/Esc=cancel"
            else:
                line += " (click to place)"
        if selected[0] is not None:
            diff, over = diff_from_ortho(oi, selected[0])
            line += f" | vertex {selected[0] + 1} Δortho={diff:.2f}{warn_unit}"
            if over:
                line += " ⚠ nudged far from ortho"
        return line

    def draw_main():
        oi = obj_order[cursor[0]]
        is_linked = linked_mask(oi)
        display = img_display.copy()
        overlay = display.copy()
        active_alpha = 0.3
        for oj, obj in enumerate(objects):
            pts_frame = [frame_pt(oj, vj) for vj in range(len(obj["points"]))]
            # A vertex near/past the homography's vanishing line projects to a huge or non-finite pixel position.
            # Draw only the contiguous run(s) of vertices that project sanely — so the visible portion still shows up.
            sane = [np.isfinite(p).all() and abs(p[0]) < 10 * w and abs(p[1]) < 10 * h for p in pts_frame]
            if not any(sane):
                continue
            pts_disp = [to_disp(*p) for p in pts_frame]
            color, alpha = object_style(obj["section"], obj["type"], type_colors, section_alphas)
            active = oj == oi
            geometry = obj["geometry"]
            all_sane = all(sane)
            if geometry == "point":
                if sane[0] and not active:
                    cv2.circle(display, tuple(np.array(pts_disp[0], dtype=np.int32)), point_radius, color, -1)
            else:
                is_closed = geometry == "polygon"
                if all_sane:
                    pts_arr = np.array(pts_disp, dtype=np.int32)
                    if active and is_closed:
                        active_alpha = alpha
                        cv2.fillPoly(overlay, [pts_arr], color)
                    cv2.polylines(display, [pts_arr], isClosed=is_closed, color=color,
                                 thickness=3 if active else 1, lineType=cv2.LINE_AA)
                else:
                    run = []
                    for vj, vj_sane in enumerate(sane):
                        if vj_sane:
                            run.append(pts_disp[vj])
                            continue
                        if len(run) >= 2:
                            cv2.polylines(display, [np.array(run, dtype=np.int32)], isClosed=False,
                                         color=color, thickness=3 if active else 1, lineType=cv2.LINE_AA)
                        run = []
                    if len(run) >= 2:
                        cv2.polylines(display, [np.array(run, dtype=np.int32)], isClosed=False,
                                     color=color, thickness=3 if active else 1, lineType=cv2.LINE_AA)
            if active:
                for vj, (vj_sane, (dx, dy)) in enumerate(zip(sane, pts_disp)):
                    if not vj_sane:
                        continue
                    cv2.circle(display, (dx, dy), point_radius, color, -1)
                    _, over = diff_from_ortho(oj, vj)
                    if over:
                        ring_color = (0, 0, 255)
                    elif vj == selected[0]:
                        ring_color = (0, 165, 255)
                    elif is_linked[vj]:
                        ring_color = (0, 255, 0)
                    else:
                        ring_color = (255, 255, 255)
                    cv2.circle(display, (dx, dy), point_radius + (3 if vj == selected[0] else 0), ring_color, 2)
                    cv2.putText(display, str(vj + 1), (dx + 8, dy - 8),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2, cv2.LINE_AA)
        cv2.addWeighted(overlay, active_alpha, display, 1 - active_alpha, 0, dst=display)
        if pending[0] is not None:
            fx, fy = frame_of(pending)
            if np.isfinite([fx, fy]).all():
                dx, dy = to_disp(fx, fy)
                pcolor = (255, 0, 255) if pending_snapped[0] else (0, 255, 255)
                cv2.circle(display, (dx, dy), point_radius + 2, pcolor, 2)
        draw_zoom_box(display, cursor_xy, zoom_size, sx, sy, box_thick)
        line = status_line()
        cv2.setWindowTitle(win, line)
        cv2.imshow(win, display)

    def draw_zoom():
        assert img is not None   # narrowing doesn't cross into nested closures; re-assert here
        zoom_img, to_zoom, in_view = zoom_crop(img, tuple(cursor_xy), to_img, zoom_size, zoom_factor)
        if zoom_img is None:
            return
        oi = obj_order[cursor[0]]
        # Other objects' vertices, muted — so a nearby point to drag onto/snap to is visible even
        # when zoomed in, not just in the (often crowded) main window.
        for oj, obj in enumerate(objects):
            if oj == oi:
                continue
            for vj in range(len(obj["points"])):
                px, py = frame_pt(oj, vj)
                if in_view(px, py):
                    lx, ly = to_zoom(px, py)
                    cv2.circle(zoom_img, (lx, ly), 7, (255, 255, 0), -1)
                    cv2.circle(zoom_img, (lx, ly), 7, (0, 0, 0), 1)
        color = object_color(objects[oi]["type"], type_colors)
        is_linked = linked_mask(oi)
        for vj in range(len(objects[oi]["points"])):
            px, py = frame_pt(oi, vj)
            if in_view(px, py):
                lx, ly = to_zoom(px, py)
                cv2.circle(zoom_img, (lx, ly), 6, color, -1)
                zoom_display_scale = zoom_window_size[0] / (zoom_size * zoom_factor)
                ring_r = max(6, round(9 / zoom_display_scale))
                ring_t = max(1, round(2 / zoom_display_scale))
                cv2.circle(zoom_img, (lx, ly), ring_r, (0, 255, 0) if is_linked[vj] else (255, 255, 255), ring_t)
        if pending[0] is not None:
            fx, fy = frame_of(pending)
            if in_view(fx, fy):
                lx, ly = to_zoom(fx, fy)
                zoom_display_scale = zoom_window_size[0] / (zoom_size * zoom_factor)
                p_r = max(8, round(11 / zoom_display_scale))
                p_t = max(1, round(2 / zoom_display_scale))
                pcolor = (255, 0, 255) if pending_snapped[0] else (0, 255, 255)
                cv2.circle(zoom_img, (lx, ly), p_r, pcolor, p_t)
        draw_crosshair(zoom_img, crosshair_thick, (0, 255, 0) if dragging[0] is None else (0, 165, 255))
        cv2.imshow(zoom_win, zoom_img)

    def to_img_from_zoom(zx, zy):
        cx_img, cy_img = to_img(*cursor_xy)
        half = zoom_size // 2
        px = (cx_img - half) + zx / zoom_factor
        py = (cy_img - half) + zy / zoom_factor
        return px, py

    def handle_click(event, ix, iy, disp_x, disp_y):
        oi = obj_order[cursor[0]]
        if event == cv2.EVENT_LBUTTONDOWN:
            if extend[0] is not None:
                # Stage a pending point (like mark_objects) — nothing is inserted until Enter.
                # Clicking again just re-aims it.
                ox, oy = to_ortho([ix, iy])
                snapped = find_snap_target(disp_x, disp_y)
                pending_snapped[0] = snapped is not None
                if snapped is not None:
                    ox, oy = snapped
                pending[0], pending[1] = ox, oy
            else:
                best, best_d2 = None, (point_radius * 2) ** 2
                for vi in range(len(objects[oi]["points"])):
                    dx, dy = to_disp(*frame_pt(oi, vi))
                    d2 = (dx - disp_x) ** 2 + (dy - disp_y) ** 2
                    if d2 <= best_d2:
                        best, best_d2 = vi, d2
                if best is not None:
                    selected[0] = best
                    dragging[0] = [oi, best, find_linked(oi, best), (disp_x, disp_y), False]
        elif event == cv2.EVENT_MOUSEMOVE:
            drag = dragging[0]
            if drag is not None:
                oi_d, vi_d, drag_links, down_xy, moved = drag
                if not moved:
                    ddist2 = (disp_x - down_xy[0]) ** 2 + (disp_y - down_xy[1]) ** 2
                    if ddist2 >= DRAG_THRESHOLD_PX ** 2:
                        drag[4] = True
                        moved = True
                if moved:
                    ox, oy = to_ortho([ix, iy])
                    set_ortho_pt(oi_d, vi_d, ox, oy)
                    for link_oi, link_vi in drag_links:
                        set_ortho_pt(link_oi, link_vi, ox, oy)
        elif event == cv2.EVENT_LBUTTONUP:
            drag = dragging[0]
            if drag is not None:
                oi_d, vi_d, _, _, moved = drag
                if moved:
                    diff, over = diff_from_ortho(oi_d, vi_d)
                    if over:
                        print(f"  ⚠ {label(oi_d)} vertex {vi_d + 1} is now {diff:.2f}{warn_unit} "
                              f"from its ortho-marked position (threshold {warn_threshold_m}{warn_unit}).")
                    ratio = edge_ratio(oi_d, vi_d)
                    if ratio is not None and ratio > edge_ratio_warn:
                        print(f"  ⚠ {label(oi_d)} vertex {vi_d + 1}'s edge is now {ratio:.1f}x the "
                              f"object's median edge length — check this didn't drag it somewhere wrong "
                              f"(r attempts an automatic re-splice if it's an isolated bad chain).")
            dragging[0] = None
        draw_main()
        draw_zoom()

    def on_main_event(event, x, y, _flags, _param):
        # Recenter the zoom crop only on a click, or while actively dragging (to follow the drag) —
        # NOT on idle hover, so crossing the main window to reach the zoom window (or vice versa)
        # doesn't drag the crop away from wherever you actually clicked/are dragging.
        if event == cv2.EVENT_LBUTTONDOWN or (event == cv2.EVENT_MOUSEMOVE and dragging[0] is not None):
            cursor_xy[0], cursor_xy[1] = x, y
        ix, iy = to_img(x, y)
        handle_click(event, ix, iy, x, y)

    def on_zoom_event(event, x, y, _flags, _param):
        # Resolve against the CURRENT crop first, then (on click-down only, like mark_objects'
        # apply_pending) recenter the crop on that point — lets you "walk" the zoom window across
        # the image by clicking, same as mark_objects, without the view panning mid-drag.
        ix, iy = to_img_from_zoom(x, y)
        if event == cv2.EVENT_LBUTTONDOWN:
            cursor_xy[0], cursor_xy[1] = to_disp(ix, iy)
        handle_click(event, ix, iy, *to_disp(ix, iy))

    cv2.setMouseCallback(win, on_main_event)
    cv2.setMouseCallback(zoom_win, on_zoom_event)

    print("Controls: click+drag = move vertex | arrows = nudge selected | d = delete selected | "
          "e/E = toggle extend at end/start | click = place pending, Enter = confirm | "
          "r = auto re-splice a bad chain | u = cancel pending / undo repair / undo last extend | "
          "n/p = next/prev object | q/ESC = finish")
    print(f"  {status_line()}")
    draw_main()
    draw_zoom()

    while True:
        key = cv2.waitKeyEx(20)
        if key == -1:
            continue
        oi = obj_order[cursor[0]]
        if key in (ord('q'), 27):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                pending_snapped[0] = False
                print("  Cancelled pending point.")
                draw_main()
                draw_zoom()
            else:
                break
        elif key == ord('n'):
            cursor[0] = (cursor[0] + 1) % len(obj_order)
            selected[0] = None
            extend[0] = None
            extend_undo.clear()
            pending[0] = pending[1] = None
            pending_snapped[0] = False
            repair_undo[0] = None
            print(f"  {status_line()}")
            draw_main()
            draw_zoom()
        elif key == ord('p'):
            cursor[0] = (cursor[0] - 1) % len(obj_order)
            selected[0] = None
            extend[0] = None
            extend_undo.clear()
            pending[0] = pending[1] = None
            pending_snapped[0] = False
            repair_undo[0] = None
            print(f"  {status_line()}")
            draw_main()
            draw_zoom()
        elif key in (ord('e'), ord('E')):
            mode = "end" if key == ord('e') else "start"
            if objects[oi]["geometry"] == "point":
                print("  Can't extend a point object (single-vertex geometry).")
            elif extend[0] == mode:
                extend[0] = None
                pending[0] = pending[1] = None
                pending_snapped[0] = False
                print("  Extend mode off.")
            else:
                extend[0] = mode
                print(f"  Extend mode: click to place a pending vertex at the {mode} of {label(oi)} "
                      f"(snaps onto a nearby existing vertex) — arrows to nudge it, Enter to confirm, "
                      f"u/Esc to cancel it. Press {'e' if mode == 'end' else 'E'} again to turn extend off.")
            draw_main()
            draw_zoom()
        elif key == ord('r'):
            snapshot = (oi, json.loads(json.dumps(objects[oi]["points"])),
                       json.loads(json.dumps(objects[oi]["lonlat"])) if "lonlat" in objects[oi] else None,
                       json.loads(json.dumps(orig_pts[orig_idx_of[oi]])))
            ok, msg = resplice_object(oi)
            if ok:
                repair_undo[0] = snapshot
                print(f"  {msg}")
                print("  Press u to revert this repair if the shape looks wrong.")
            else:
                print(f"  {msg}")
            draw_main()
            draw_zoom()
        elif key in (13, 10) and pending[0] is not None:
            new_vi = len(objects[oi]["points"]) if extend[0] == "end" else 0
            objects[oi]["points"].insert(new_vi, [float(pending[0]), float(pending[1])])
            orig_pts[orig_idx_of[oi]].insert(new_vi, [float(pending[0]), float(pending[1])])
            extend_undo.append((oi, extend[0]))
            print(f"  Confirmed vertex on {label(oi)} ({len(objects[oi]['points'])} total).")
            ratio = edge_ratio(oi, new_vi)
            if ratio is not None and ratio > edge_ratio_warn:
                print(f"  ⚠ That edge is {ratio:.1f}x {label(oi)}'s median edge length — looks spliced "
                      f"at the wrong end (e.g. 'end' when the list's end is actually the near cap). "
                      f"Press u to undo this vertex, or r to try an automatic re-splice.")
            pending[0] = pending[1] = None
            pending_snapped[0] = False
            selected[0] = new_vi
            draw_main()
            draw_zoom()
        elif key == ord('u'):
            if pending[0] is not None:
                pending[0] = pending[1] = None
                pending_snapped[0] = False
                print("  Cancelled pending point.")
            elif repair_undo[0] is not None:
                undo_oi, prev_points, prev_lonlat, prev_orig_pts = repair_undo[0]
                objects[undo_oi]["points"] = prev_points
                if prev_lonlat is not None:
                    objects[undo_oi]["lonlat"] = prev_lonlat
                orig_pts[orig_idx_of[undo_oi]] = prev_orig_pts
                repair_undo[0] = None
                selected[0] = None
                print(f"  Reverted last auto-repair on {label(undo_oi)}.")
            elif extend_undo:
                undo_oi, undo_mode = extend_undo.pop()
                if undo_oi < len(objects) and objects[undo_oi]["points"]:
                    undo_vi = len(objects[undo_oi]["points"]) - 1 if undo_mode == "end" else 0
                    del objects[undo_oi]["points"][undo_vi]
                    del orig_pts[orig_idx_of[undo_oi]][undo_vi]
                    selected[0] = None
                    print(f"  Undid extend: removed vertex from {label(undo_oi)}.")
                else:
                    print("  Nothing to undo (that object changed since).")
            else:
                print("  Nothing to undo.")
            draw_main()
            draw_zoom()
        elif key in ARROW and pending[0] is not None:
            ddx, ddy = ARROW[key]
            fx, fy = frame_of(pending)
            ox, oy = to_ortho([fx + ddx, fy + ddy])
            pending[0], pending[1] = ox, oy
            pending_snapped[0] = False
            draw_main()
            draw_zoom()
        elif key in ARROW and selected[0] is not None:
            ddx, ddy = ARROW[key]
            linked = find_linked(oi, selected[0])
            dx, dy = frame_pt(oi, selected[0])
            ox, oy = to_ortho([dx + ddx, dy + ddy])
            set_ortho_pt(oi, selected[0], ox, oy)
            for oj, vj in linked:
                set_ortho_pt(oj, vj, ox, oy)
            diff, over = diff_from_ortho(oi, selected[0])
            if over:
                print(f"  ⚠ {label(oi)} vertex {selected[0] + 1} is now {diff:.2f}{warn_unit} "
                      f"from its ortho-marked position (threshold {warn_threshold_m}{warn_unit}).")
            draw_main()
            draw_zoom()
        elif key == ord('d') and selected[0] is not None:
            min_vertices = GEOMETRY_MIN_VERTICES[objects[oi]["geometry"]]
            if len(objects[oi]["points"]) > min_vertices:
                print(f"  Deleting vertex {selected[0] + 1} from {label(oi)}.")
                del objects[oi]["points"][selected[0]]
                del orig_pts[orig_idx_of[oi]][selected[0]]
                selected[0] = None
                draw_main()
                draw_zoom()
            else:
                print(f"  Only {min_vertices} vertex/vertices left — deleting the entire object "
                      f"{label(oi)} instead.")
                del objects[oi]
                del orig_idx_of[oi]
                extend_undo.clear()
                repair_undo[0] = None
                obj_order[:] = [oj if oj < oi else oj - 1 for oj in obj_order if oj != oi]
                selected[0] = None
                dragging[0] = None
                extend[0] = None
                pending[0] = pending[1] = None
                pending_snapped[0] = False
                if not obj_order:
                    print("  No objects left to refine.")
                    break
                cursor[0] = min(cursor[0], len(obj_order) - 1)
                print(f"  {status_line()}")
                draw_main()
                draw_zoom()

    cv2.destroyWindow(win)
    cv2.destroyWindow(zoom_win)
    n_eliminated = len(objects_before) - len(objects)
    n_modified = sum(1 for i, orig_i in enumerate(orig_idx_of) if objects[i]["points"] != objects_before[orig_i]["points"])
    print(f"{n_modified} object(s) modified, {n_eliminated} eliminated.")
    over_lines = [
        f"    {label(oi)} vertex {vi + 1}: {diff:.2f}{warn_unit}"
        for oi, obj in enumerate(objects)
        for vi in range(len(obj["points"]))
        for diff, over in [diff_from_ortho(oi, vi)] if over
    ]
    if over_lines:
        print(f"⚠ {len(over_lines)} vertex/vertices ended up > {warn_threshold_m}{warn_unit} "
              f"from their ortho-marked position — re-check these against the ortho before trusting them:")
        for line in over_lines:
            print(line)
    jump_lines = [
        f"    {label(oi)} vertex {vi + 1}: {ratio:.1f}x median edge length"
        for oi, obj in enumerate(objects)
        for vi in range(len(obj["points"]))
        for ratio in [edge_ratio(oi, vi)] if ratio is not None and ratio > edge_ratio_warn
    ]
    if jump_lines:
        print(f"⚠ {len(jump_lines)} vertex/vertices have an edge > {edge_ratio_warn}x their object's "
              f"median edge length — likely spliced at the wrong position (self-crossing polygon risk):")
        for line in jump_lines:
            print(line)
    save_json_list(objects, objects_json_path, "object(s)")

    return objects


def plot_objects(image_path, objects_json_path, output_html, type_colors=None, section_alphas=None, H=None):
    """
    Render saved objects (points/lines/polygons) over a background image as a Plotly HTML plot.

    By default, objects are plotted directly in image_path's own pixel space.
    Pass H to instead reproject them onto a *different* image_path.

    :param image_path [str or Path]        Background image (e.g. the ortho PNG, or a drone's frame if H is given).
    :param objects_json_path [str or Path] Objects JSON.
    :param output_html [str or Path]       Where to save the HTML plot.
    :param type_colors, section_alphas     See object_style(). Defaults to {} (all objects black).
    :param H [np.ndarray, shape (3,3), optional]  Homography matrix

    :returns: output_html
    """
    type_colors = type_colors or {}
    section_alphas = section_alphas or {}
    image_path = Path(image_path)
    objects_json_path = Path(objects_json_path)
    if not image_path.exists():
        raise FileNotFoundError(f"Image not found: {image_path}")
    if not objects_json_path.exists():
        raise FileNotFoundError(f"Objects JSON not found: {objects_json_path}")

    img = load_image(image_path)
    h, w = img.shape[:2]
    ok, buf = cv2.imencode(".png", img)
    img_source = f"data:image/png;base64,{base64.b64encode(buf).decode('ascii')}"
    objects = json.loads(objects_json_path.read_text(encoding="utf-8"))
    H_inv = np.linalg.inv(H) if H is not None else None
    geometry_order = {"polygon": 0, "line": 1, "point": 2}
    objects = sorted(objects, key=lambda o: (geometry_order[o["geometry"]], o["type"], o["section"]))

    fig = go.Figure()
    fig.add_layout_image(dict(
        source=img_source, xref="x", yref="y", x=0, y=0, sizex=w, sizey=h,
        sizing="stretch", xanchor="left", yanchor="top", layer="below",
    ))

    for obj in objects:
        (b, g, r), alpha = object_style(obj["section"], obj["type"], type_colors, section_alphas)
        color = f"rgb({r},{g},{b})"
        if H_inv is None:
            pts = obj["points"]
        else:
            pts = pixel_to_world(H_inv, np.array(obj["points"], dtype=np.float64)).tolist()
        # A vertex near/past the homography's vanishing line reprojects to a huge or non-finite position
        # Skip such objects entirely if nothing survives, and break the trace at each degenerate vertex (via a None gap)
        # rather than drawing a spurious line/fill across it.
        sane = [np.isfinite(p).all() and abs(p[0]) < 10 * w and abs(p[1]) < 10 * h for p in pts]
        if not any(sane):
            continue
        all_sane = all(sane)
        is_polygon = obj["geometry"] == "polygon" and all_sane
        if all_sane:
            xs = [p[0] for p in pts] + ([pts[0][0]] if is_polygon else [])
            ys = [p[1] for p in pts] + ([pts[0][1]] if is_polygon else [])
            labels = [str(j) for j in range(len(pts))] + ([""] if is_polygon else [])
        else:
            xs, ys, labels = [], [], []
            prev_sane = False
            for j, (p, s) in enumerate(zip(pts, sane)):
                if not s:
                    prev_sane = False
                    continue
                if not prev_sane and xs:
                    xs.append(None); ys.append(None); labels.append("")
                xs.append(p[0]); ys.append(p[1]); labels.append(str(j))
                prev_sane = True
        name = "/".join(p for p in (obj["section"], obj["type"], obj["id"]) if p)
        mode = "markers+text" if obj["geometry"] == "point" else "lines+markers+text"
        fig.add_trace(go.Scatter(
            x=xs, y=ys, mode=mode, name=name, fill="toself" if is_polygon else "none",
            fillcolor=f"rgba({r},{g},{b},{alpha})" if is_polygon else None,
            text=labels, textposition="top center", textfont=dict(size=10, color=color),
            line=dict(color=color, width=2), marker=dict(color=color, size=6),
            hovertemplate=f"{name}<br>point %{{text}}<br>x=%{{x}}, y=%{{y}}<extra></extra>",
        ))

    fig.update_xaxes(range=[0, w], showgrid=False, zeroline=False, visible=False)
    fig.update_yaxes(range=[h, 0], showgrid=False, zeroline=False, visible=False, scaleanchor="x", scaleratio=1)
    fig.update_layout(
        title=objects_json_path.stem,
        width=min(1600, w), height=min(1600, w) * h / w,
        margin=dict(l=10, r=10, t=40, b=120),
        legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
    )

    output_html = Path(output_html)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_html))

    return output_html