import sys
import time
import numpy as np
import matplotlib


"""
Interactively select N corresponding points (default 5) on:
    1) A georeferenced orthophoto (GeoTIFF or any rasterio-readable raster)
    2) A plain PNG image of the same scene, taken from a different angle

Output:
    - Orthophoto points  -> real-world coordinates in WGS84 (lon, lat)
    - PNG points         -> pixel coordinates (x, y)

Results are written to a tab-separated text file, one row per point pair, so point i on the orthophoto corresponds to point i on the PNG.

Instructions when running:
    - A window opens showing the orthophoto. Click the points in the exact order you want them recorded. The window closes automatically after the last click.
    - A second window opens showing the PNG. Click the corresponding points in the SAME order (point 1 on the PNG must be the same real-world feature as point 1 on the orthophoto, etc.).
"""

TERMINAL_SHORT_NAME = "hudd"
PART = "1"
ORTHOPHOTO_PATH = "../data/orthophotos/huddinge/657_66_0075_2023_clipped.tif"
FRAME_PATH = f"../output/trajs_from_video/{TERMINAL_SHORT_NAME}_frame_0_part{PART}.png"
OUTPUT_TXT_PATH = f"../output/{TERMINAL_SHORT_NAME}_ref_points_part{PART}.txt"
NB_REF_POINTS = 5

# ginput() needs an interactive GUI backend; the default (e.g. "Agg") can't open a clickable window. Try common interactive backends until one loads.
_INTERACTIVE_BACKENDS = ["QtAgg", "Qt5Agg", "TkAgg", "MacOSX"]
_backend_ok = False
for _backend in _INTERACTIVE_BACKENDS:
    try:
        matplotlib.use(_backend, force=True)
        _backend_ok = True
        break
    except Exception:
        continue
 
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
 
if not _backend_ok:
    print(
        "ERROR: No interactive matplotlib backend is available (tried: "
        + ", ".join(_INTERACTIVE_BACKENDS)
        + ").\n"
        "Install one, e.g.:\n"
        "    pip install PyQt5\n"
        "or, if running in Jupyter, add this in a cell before running:\n"
        "    %matplotlib widget    (requires: pip install ipympl)\n"
        "or:\n"
        "    %matplotlib qt        (requires: pip install PyQt5)"
    )
    sys.exit(1)

try:
    import rasterio
    from rasterio.transform import xy as rasterio_xy
except ImportError:
    print("This script requires rasterio. Install with: pip install rasterio")
    sys.exit(1)

try:
    from pyproj import Transformer, CRS
except ImportError:
    print("This script requires pyproj. Install with: pip install pyproj")
    sys.exit(1)


def select_points_on_image(image_array, title, n_points):
    """Show an image and let the user click n_points points on it.
 
    Unlike plt.ginput(), this ignores clicks made while the toolbar's
    zoom or pan tool is active, and ignores non-left-clicks, so you can
    freely zoom/pan to line up a precise click without it being recorded
    as a point.
 
    Returns a list of (x, y) tuples in pixel/data coordinates, in click order.
    """
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.imshow(image_array)
    ax.set_title(
        f"{title}\nLeft-click {n_points} points in order.\n"
        "Zoom/pan freely with the toolbar; those clicks are ignored."
    )
    plt.axis("on")
 
    points = []
    markers = []
 
    def on_click(event):
        # Ignore clicks made while the zoom or pan tool is active.
        toolbar = fig.canvas.toolbar
        if toolbar is not None and toolbar.mode != "":
            return
        # Ignore clicks outside the image axes or that aren't a left-click.
        if event.inaxes != ax or event.button != 1:
            return
 
        points.append((event.xdata, event.ydata))
        marker, = ax.plot(event.xdata, event.ydata, "r+", markersize=12, markeredgewidth=2)
        markers.append(marker)
        ax.set_title(f"{title}\nPoint {len(points)}/{n_points} selected.")
        fig.canvas.draw_idle()
 
    cid = fig.canvas.mpl_connect("button_press_event", on_click)
 
    fig.canvas.draw()
    plt.show(block=False)
 
    # Manually pump the GUI event queue instead of using
    # fig.canvas.start_event_loop(). Jupyter/IPython installs its own GUI
    # event-loop integration for interactive backends, which can make
    # start_event_loop() (and plt.ginput(), which uses it internally) return
    # immediately instead of actually waiting. Polling with flush_events()
    # sidesteps that and works the same in a notebook, a plain script, or
    # an IDE console.
    try:
        while len(points) < n_points and plt.fignum_exists(fig.number):
            fig.canvas.flush_events()
            time.sleep(0.02)
    finally:
        fig.canvas.mpl_disconnect(cid)
        if plt.fignum_exists(fig.number):
            plt.close(fig)
 
    if len(points) < n_points:
        print(f"Warning: only {len(points)} of {n_points} points were selected on {title}.")
    return points
 
 
def load_orthophoto_for_display(ortho_path):
    """Read an orthophoto with rasterio and return an array matplotlib can display."""
    with rasterio.open(ortho_path) as src:
        arr = src.read()  # shape: (bands, rows, cols)
        if arr.shape[0] >= 3:
            img = np.transpose(arr[:3], (1, 2, 0))
        else:
            img = arr[0]
 
        if img.dtype != np.uint8:
            img = img.astype(np.float32)
            img -= img.min()
            if img.max() > 0:
                img = img / img.max()
        return img
 
 
def ortho_pixels_to_wgs84(ortho_path, pixel_points):
    """Convert (col, row) pixel points on the orthophoto to (lon, lat) in WGS84."""
    with rasterio.open(ortho_path) as src:
        transform = src.transform
        src_crs = src.crs
        if src_crs is None:
            raise ValueError(
                "Orthophoto has no CRS information; cannot convert to WGS84."
            )
 
        native_coords = []
        for (col, row) in pixel_points:
            x, y = rasterio_xy(transform, row, col, offset="center")
            native_coords.append((x, y))
 
        target_crs = CRS.from_epsg(4326)  # WGS84
        transformer = Transformer.from_crs(src_crs, target_crs, always_xy=True)
        wgs84_coords = [transformer.transform(x, y) for (x, y) in native_coords]
        return wgs84_coords  # list of (lon, lat)


print("Step 1: click on the ORTHOPHOTO.")
ortho_img = load_orthophoto_for_display(ORTHOPHOTO_PATH)
ortho_pixel_points = select_points_on_image(ortho_img, "Orthophoto", NB_REF_POINTS)

print("Step 2: click on the PNG image (different angle), in the same order.")
png_img = Image.open(FRAME_PATH)
png_array = np.array(png_img)
png_pixel_points = select_points_on_image(png_array, "PNG Photo", NB_REF_POINTS)

if len(ortho_pixel_points) < NB_REF_POINTS or len(png_pixel_points) < NB_REF_POINTS:
    print("Not enough points selected on one or both images. Aborting without writing output.")
    sys.exit(1)

print("Converting orthophoto pixel coordinates to WGS84...")
wgs84_points = ortho_pixels_to_wgs84(ORTHOPHOTO_PATH, ortho_pixel_points)

with open(OUTPUT_TXT_PATH, "w") as f:
    f.write("Point\tOrtho_Lon(WGS84)\tOrtho_Lat(WGS84)\tPNG_Pixel_X\tPNG_Pixel_Y\n")
    for i in range(NB_REF_POINTS):
        lon, lat = wgs84_points[i]
        px, py = png_pixel_points[i]
        f.write(f"{i + 1}\t{lon:.8f}\t{lat:.8f}\t{px:.2f}\t{py:.2f}\n")

print(f"Done. Results written to {OUTPUT_TXT_PATH}")
