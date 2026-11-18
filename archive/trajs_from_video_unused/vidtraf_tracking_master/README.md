# Traffic Video Analysis
Detection and tracking of road users (pedestrians, cyclists, cars, buses, trucks, motorcycles)
from video recordings (fixed cameras or drones). Detection uses the [Ultralytics YOLO](https://docs.ultralytics.com/) family of models with
ByteTrack multi-object tracking. 

---

## Table of Contents

1. [Software requirements](#1-software-requirements)
2. [Pipeline overview](#2-pipeline-overview)
3. [Pre-processing](#3-pre-processing)
   - 3.1 [Undistort](#31-undistort)
   - 3.2 [Video stabilization](#32-video-stabilization)
   - 3.3 [Detection masking](#33-detection-masking)
   - 3.4 [Face anonymization](#34-face-anonymization)
4. [Detection and tracking](#4-detection-and-tracking)
5. [Post-processing](#5-post-processing)
   - 5.1 [Merging cyclists](#51-merging-cyclists)
   - 5.2 [Smoothing](#52-smoothing)
   - 5.3 [Visualizing](#53-visualizing)
   - 5.4 [Geometric calibration](#54-geometric-calibration)
     - 5.4.1 [Flat Terrain](#541-flat-terrain)
     - 5.4.2 [Non-flat Terrain](#542-non-flat-terrain)
6. [Parquet schema: trajectory results](#6-parquet-schema-trajectory-results)

---

## 1. Software requirements

### Installing this library

```powershell
pip install -e "path/to/vidtraf_tracking"                       # editable install (recommended for development)
pip install -e "path/to/vidtraf_tracking[tracking]"             # include ultralytics (YOLO + ByteTrack)
pip install -e "path/to/vidtraf_tracking[anonymize]"            # include deface (face anonymization)
pip install -e "path/to/vidtraf_tracking[elevation]"            # include rasterio for non-flat terrain projection
pip install -e "path/to/vidtraf_tracking[plots]"                # include matplotlib for validation plots
pip install -e "path/to/vidtraf_tracking[dev]"                  # include IDE type stubs for cv2
pip install -e "path/to/vidtraf_tracking[tracking,elevation]"   # combine extras
```

Core dependencies installed automatically: `opencv-python`, `numpy`, `scipy`, `pandas`, `scikit-learn`, `shapely`.

YOLO model weights are downloaded automatically on first use. To pre-download:

```python
from ultralytics import YOLO
YOLO('yolo12s.pt')
```

Available sizes within a family (`yolo12` shown): `n` (nano) → `s` → `m` → `l` → `x` (extra-large).
Larger models are slower but generally more accurate.

---

## 2. Pipeline Overview

```
Raw video
   │
   ▼ [Stage 1 — Video pre-processing]
   undistort, stabilize, mask, anonymize
   │
   ▼ [Stage 2 — Detection and Tracking]
   YOLO detection + ByteTrack multi-object tracking            
   │
   ▼ [Stage 3 — Trajectory post-processing]
   merge, smooth, visualize, geometric calibration
   │
   ▼ [Stage 4 — Traffic analysis]
```

---

## 3. Pre-processing

**Module**: `video_utils.py`.

| Function                                               | Description                                             |
|--------------------------------------------------------|---------------------------------------------------------|
| `open_video(path)`                                     | Open a video; returns `cap, fps, w, h, n_frames`        |
| `extract_frame(video, output, frame_index)`            | Save a single frame as PNG                              |
| `extract_frames(video, output_dir, fps)`               | Extract frames at a regular interval                    |
| `trim_video(video, output, duration, start)`           | Cut a video timewise                                    |
| `crop_video(video, output, x1, y1, x2, y2)`            | Crop every frame to a rectangle                         |
| `undistort_image(img, K, D, model, balance)`           | Undistort a single image                                |
| `undistort_video(video, output, K, D, model, balance)` | Undistort a full video                                  |
| `stabilize_video(video, output, ...)`                  | Register all frames to a reference frame                |
| `measure_residual_drift(video, ...)`                   | Quantify remaining motion after stabilization           |
| `apply_mask_to_video(video, output, mask, ...)`        | Blank masked regions before detection                   |
| `visualize_mask(frame, mask, output, ...)`             | Preview mask overlay on a reference frame               |
| `anonymize_video(video, output, ...)`                  | Blur/pixelate faces in ground-level video (uses deface) |

---

### 3.1 Undistort

**When to use:** When barrel or pincushion distortion is visible in the video, or when metric
accuracy requires a geometrically correct image.

Requires camera intrinsics (K, D) obtained from lens calibration.

```python
import video_utils as vu
import numpy as np

calib = np.load("camera_calib.npz")

# Undistort full video
vu.undistort_video("video.mp4", "video_undistorted.mp4",
                   K=calib["K"], D=calib["D"],
                   model="pinhole")  # or "fisheye" for wide-angle lenses

# Undistort a single image
import cv2
img = cv2.imread("frame.png")
corrected = vu.undistort_image(img, calib["K"], calib["D"], model="pinhole")
```

| Parameter | Default     | Description                                                                                                |
|-----------|-------------|------------------------------------------------------------------------------------------------------------|
| `K`       | —           | 3×3 camera intrinsic matrix                                                                                |
| `D`       | —           | Distortion coefficients                                                                                    |
| `model`   | `"pinhole"` | `"pinhole"` (≤90° FOV) or `"fisheye"` (>120° FOV)                                                          |
| `balance` | `0.0`       | Fisheye only: `0` = crop to valid pixels (no black borders), `1` = keep full field of view (black borders) |

**Output:** `<video>_undistorted.mp4`

---

### 3.2 Video stabilization

**When to use:** Drone footage with camera movement or jitter.

Registers every frame to a reference frame using ORB feature matching and RANSAC, producing a stationary video. 
Optionally applies a moving-average filter to the registration transforms to further reduce high-frequency jitter.

```python
import video_utils as vu

diagnostics = vu.stabilize_video(
    "video.mp4", "video_stab.mp4",
    reference_frame_index=300,
    static_mask_path="static_mask.png",
    smooth_radius=5,
)

# Verify stabilization quality
vu.measure_residual_drift("video_stab.mp4", reference_frame_index=300)
```

| Parameter               | Default   | Description                                                                                                                                  |
|-------------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `reference_frame_index` | `0`       | Index of the frame used as the registration target.                                                                                          |
| `static_mask_path`      | `None`    | Binary image (white = static region, black = exclude). <br/>Prevents moving objects from corrupting the registration.                        |
| `smooth_radius`         | `0`       | Half-width of the moving-average filter applied to registration transform parameters<br/>(0 = no smoothing; 5–15 for high-frequency jitter). |
| `write_diagnostics`     | `True`    | If True, save a `_stab_log.npz` alongside the output.                                                                                        |

**Output:** `<video>_stab.mp4`, `<video>_stab_log.npz`

Use `measure_residual_drift()` to quantify remaining motion after stabilization.

| Max translation   | Max rotation   | Quality                                            |
|-------------------|----------------|----------------------------------------------------|
| < 1.0 px          | < 0.02°        | Excellent                                          |
| < 2.0 px          | < 0.05°        | Good — usable                                      |
| < 5.0 px          | < 0.10°        | Marginal — try a different reference frame or mask |
| ≥ 5.0 px          | ≥ 0.10°        | Poor — not usable for homography or tracking       |

Common causes of poor stabilization: dense traffic obstructing static features, too few ORB features
in the reference frame, severe camera rotation between frames, static mask too restrictive.

---

### 3.3 Detection masking

**When to use:** When parts of the scene consistently produce false positives (cluttered edges, reflective surfaces, areas outside the analysis zone).

Draw a binary mask in any image editor (white = detect, black = suppress), preview the overlay on the reference frame, then apply it to the full video. Alternatively, pass `mask_path` directly to `trajectory.tracker()` to apply the mask per frame without pre-processing the video.

```python
import video_utils as vu

# Extract reference frame for drawing the mask
vu.extract_frame("video.mp4", "reference_frame.png", frame_index=0)

# After drawing the mask in an image editor, preview it
vu.visualize_mask("reference_frame.png", "detection_mask.png", "mask_preview.png")

# Apply the mask to the full video
vu.apply_mask_to_video("video.mp4", "video_masked.mp4", "detection_mask.png")
```

| Parameter    | Default       | Description                                                                                     |
|--------------|---------------|-------------------------------------------------------------------------------------------------|
| `fill_color` | `(0, 0, 0)`   | `apply_mask_to_video`: colour used to fill masked-out regions (B, G, R).                        |
| `invert`     | `False`       | `apply_mask_to_video`: if True, white is masked out and black is kept.                          |
| `alpha`      | `0.5`         | `visualize_mask`: tint opacity of masked-out regions in the preview (0 = invisible, 1 = solid). |
| `mask_color` | `(0, 0, 255)` | `visualize_mask`: colour used to tint masked-out regions in the preview (B, G, R). Default red. |

**Output:** `<video>_masked.mp4`, `<video>_detection_mask_preview.png`

---

### 3.4 Face anonymization

**When to use:** Ground-level footage where faces are visible and must be obscured before storing or sharing the video
(e.g. GDPR compliance).

Requires deface: `pip install "vidtraf_tracking[anonymize]"`

```python
import video_utils as vu

vu.anonymize_video("video.mp4")

# Stronger detection (catches more faces, more false positives)
vu.anonymize_video("video.mp4", threshold=0.1)

# Pixelate instead of blur
vu.anonymize_video("video.mp4", method="pixelate")
```

| Parameter      | Default    | Description                                                                          |
|----------------|------------|--------------------------------------------------------------------------------------|
| `threshold`    | `0.2`      | Detection confidence threshold (0–1). Lower = more detections, more false positives. |
| `method`       | `"blur"`   | `"blur"` (Gaussian), `"pixelate"`, or `"solid"` (black rectangle).                   |

**Output:** `<video>_anon.mp4`

---

## 4. Detection and tracking

**Module:** `trajectory.py`

Uses YOLO for per-frame detection and ByteTrack for multi-object tracking. 
Supports any Ultralytics-compatible YOLO model, including custom models fine-tuned on domain-specific data.

```python
import trajectory as tj

results = tj.tracker(
    video_path="video.mp4",
    output_path="traj.parquet",
    model_name="yolo11s.pt",  # or a fine-tuned model, e.g. "yolo12s_visdrone.pt"
    track_config="bytetrack.yaml",
    conf=0.3,
    imgsz=640,
)
```

| Parameter          | Default   | Description                                                                                                               |
|--------------------|-----------|---------------------------------------------------------------------------------------------------------------------------|
| `video_path`       | —         | Input video file path                                                                                                     |
| `model_name`       | —         | YOLO model file. Auto-downloaded on first use (e.g. `"yolo11s.pt"`).                                                      |
| `track_config`     | —         | Tracker YAML filename or path: `"bytetrack.yaml"` or `"botsort.yaml"`                                                     |
| `output_path`      | *(auto)*  | Output Parquet file. Defaults to `<video_stem>_traj.parquet` alongside the video.                                         |
| `conf`             | 0.25      | Detection confidence threshold. Range: 0–1.                                                                               |
| `iou`              | 0.7       | IoU threshold for NMS. <br/>Lower values reduce duplicate detections; raise for dense scenes with overlapping objects.    |
| `imgsz`            | 640       | YOLO input image size in pixels. Increase for drone footage.                                                              |
| `first_frame`      | 0         | First frame to process.                                                                                                   |
| `last_frame`       | *(end)*   | Last frame to process. `None` = end of video.                                                                             |
| `mask_path`        | `None`    | Binary mask image (white = detect, black = suppress). Applied per frame before detection.                                 |
| `classes`          | `None`    | Model class IDs to track. `None` = all classes.                                                                           |
| `min_track_frames` | `15`      | Fragmentation threshold for the tracking summary.<br/>Tracks shorter than this count toward the fragmentation percentage. |

The `track_config` argument points to `bytetrack.yaml`, which controls how ByteTrack associates detections across frames.

| Parameter           | Default  | Description                                                                                                                       |
|---------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------|
| `track_high_thresh` | `0.25`   | First-stage match threshold. Detections above this are matched to existing tracks first. <br/>Lower → more recall, more noise.    |
| `track_low_thresh`  | `0.1`    | Second-stage threshold. Detections between low and high thresh get a second chance to match unmatched tracks.                     |
| `new_track_thresh`  | `0.25`   | Minimum confidence to start a new track.<br/> Higher → fewer false tracks but more missed objects.                                |
| `track_buffer`      | `36`     | Frames a track survives without a matching detection.<br/> At 30 fps: 36 frames = 1.2 s. Increase if trajectories are fragmented. |
| `match_thresh`      | `0.8`    | IoU threshold for detection-to-track association. <br/>Lower to 0.6–0.7 for drone footage.                                        |
| `fuse_score`        | `true`   | Fuse detection confidence into the association cost.                                                                              |

**Output:** `<video>_traj.parquet` trajectory in pixel coordinates, one row per object per frame; `<video>_summary.txt` tracking summary saved automatically.

---

## 5. Post-processing

### 5.1 Merging cyclists

#### `trajectory.merge_cyclists(traj, model_names=None, iou_overlap=0.15, prop_match=0.3, prop_tw=0.2)`

Merges person + bicycle/motorcycle track pairs into single cyclist tracks. Two scenarios:

- **Scenario A** (two tracks): IoU-based Hungarian matching. The two-wheeler absorbs any
  pedestrian-only frames; the pedestrian track is deleted.
- **Scenario B** (one track, bicycle box was unstable): A pedestrian track detected as a two-wheeler for
  more than `prop_tw` of its frames is reclassified as bicycle without a structural merge.

| Parameter     | Default   | Description                                                                                                                             |
|---------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `traj`        | —         | Parquet trajectory file (modified in place).                                                                                            |
| `model_names` | `None`    | `class_id → label` dict. When `None`, labels are read from the `class_name` column.                                                     |
| `iou_overlap` | 0.15      | Per-frame IoU threshold for a person/bike frame to count as a match (Scenario A).                                                       |
| `prop_match`  | 0.3       | Minimum fraction of two-wheeler detected frames that must IoU-match a pedestrian to merge them (Scenario A).                            |
| `prop_tw`     | 0.2       | Reclassify a pedestrian as bicycle if more than this fraction of its detected frames carried a two-wheeler `class_id_raw` (Scenario B). |

Two audit columns are added to the Parquet:

| Column          |  Type            | Description                                                                                |
|-----------------|------------------|--------------------------------------------------------------------------------------------|
| `merged`        | bool             | `True` for every row of a track that was merged (Scenario A) or reclassified (Scenario B). |
| `absorbed_from` | Int32 (nullable) | The original `object_id` of the pedestrian track absorbed into this two-wheeler track.     |

**Output:** `<video>_merged_log.txt` recording every change with object IDs and class names. Returns `{"n_changes": int, "log_path": Path}`, or `{"n_changes": 0, "log_path": None}` if no cyclist classes were found.

---

### 5.2 Smoothing

#### `trajectory.smooth_trajectories(traj, window=5, cols=("x", "y"), out_cols=None)`

Symmetric moving average per object track. Applicable to pixel coordinates, world coordinates,
or any numeric positional column.

| Parameter  | Default      | Description                                                                                        |
|------------|--------------|----------------------------------------------------------------------------------------------------|
| `traj`     | —            | Parquet file (modified in place).                                                                  |
| `window`   | `5`          | Number of frames averaged.                                                                         |
| `cols`     | `("x", "y")` | Columns to smooth.                                                                                 |
| `out_cols` | *(auto)*     | Output column names.<br/>Defaults to input names with `"s"` appended (`"x"` → `"xs"`, `"y"` → `"ys"`). |

**Output:** adds smoothed columns to the Parquet file.

---

### 5.3 Visualizing

#### `trajectory.annotated_video(input_video, traj, output_video, trail_length=60, show_ids=True, label_scale=0.45, class_names=None, class_colors=None, min_track_length=None, first_frame=None, last_frame=None, xy_cols=("x", "y"), show_bbox=False)`

Renders tracked trajectories overlaid on the video as an annotated MP4.
Consecutive detected frames are joined by a **solid line**; interpolated gap segments by a **dotted line**.

```python
import trajectory as tj

# Explicit colours + class name in label
tj.annotated_video(
    input_video="video.mp4",
    traj="traj.parquet",
    output_video="results_annotated.mp4",
    class_names={0: "person", 1: "bicycle", 2: "car",
                 3: "motorcycle", 5: "bus", 7: "truck"},
    class_colors={
        0: (  0, 200,   0),  # person      → green
        1: (  0, 165, 255),  # bicycle     → orange
        2: (255,   0,   0),  # car         → blue
        3: (255,   0, 255),  # motorcycle  → magenta
        5: (255, 255,   0),  # bus         → cyan
        7: (128,   0, 128),  # truck       → purple
    },
)
```
| Parameter          | Default      | Description                                                                                           |
|--------------------|--------------|-------------------------------------------------------------------------------------------------------|
| `input_video`      | —            | Input video file.                                                                                     |
| `traj`             | —            | Parquet trajectory file.                                                                              |
| `output_video`     | —            | Output annotated video file (.mp4).                                                                   |
| `trail_length`     | `60`         | Past frames drawn as a trailing line. Pass `"all"` for the full trajectory from first detection.      |
| `show_ids`         | `True`       | Draw object ID (and class name when `class_names` is given) as a label.                               |
| `label_scale`      | `0.45`       | Font scale for the object label.                                                                      |
| `class_names`      | `None`       | `{class_id: name}` dict. When given, the class name is appended to the ID label.                      |
| `class_colors`     | `None`       | `{class_id: (B, G, R)}` dict. When `None`, colours are derived deterministically from class ID.       |
| `min_track_length` | `None`       | Skip objects with fewer detected frames than this. Hides short spurious tracks.                       |
| `first_frame`      | `None`       | First frame to annotate (inclusive). Defaults to 0.                                                   |
| `last_frame`       | `None`       | Last frame to annotate (inclusive). Defaults to end of video.                                         |
| `xy_cols`          | `("x", "y")` | Parquet columns used for (x, y) pixel coordinates.                                                    |
| `show_bbox`        | `False`      | Draw the detection bounding box on detected frames. Bounding box is not drawn on interpolated frames. |

**Output:** `<video>_annotated.mp4`

---

### 5.4 Geometric calibration

Converts pixel-coordinate trajectories to real-world coordinates and corrects projection errors,
enabling distance and speed measurements.

**When to use:** Whenever real-world distances, speeds, or positions are needed.
Without calibration, tracking results remain in pixel coordinates only.

| Terrain | Module |
|---------|--------|
| Flat (most cases) | `homography_tools.py` |
| Non-flat (hills, ramps) | `elevation_projection.py` |

---

#### 5.4.1 Flat Terrain

Maps pixel coordinates (u, v) to world coordinates (X, Y) in metres. Assumes the ground plane
is approximately flat within the analysis area.

##### Coordinate input modes

| Mode | How to use | Best for |
|------|-----------|----------|
| **Metric** | Type (X, Y) in metres per point | Tape measure or theodolite field measurements |
| **Lat/Lon** | Type (lat, lon) per point; pass a plain orthophoto or Google Maps screenshot | When coordinates come from a GIS or online map |
| **GeoTIFF** | Pass a `geotiff_path`; click on the file — coordinates auto-extracted from the embedded CRS | Lantmäteriet georeferenced orthophotos (recommended) |

For GeoTIFF mode: download an orthophoto from Lantmäteriet (any projection — SWEREF99 TM, WGS84,
local Swedish zones are all supported). Requires `pip install rasterio`.

##### Workflow
1. Provide a reference frame index (same as used for stabilization, if applied)
2. Click ≥ 4 reference points on the orthophoto or video frame
3. Provide world coordinates (typed or auto-extracted from GeoTIFF)
4. If using an orthophoto: click the same points on the video frame
5. Compute and validate the homography with hold-out points

##### Outputs
| File | Description |
|------|-------------|
| `<video>_homography_f<idx>.txt` | 3×3 homography matrix |
| `<video>_reference_f<idx>.png` | Annotated reference frame |
| `<video>_orthophoto_f<idx>.png` | Annotated orthophoto / GeoTIFF (if used) |
| `<video>_refpoints_f<idx>.csv` | Per-point coordinates and inlier flags |
| `<video>_origin_f<idx>.json` | WGS84 geodetic origin (if lat/lon or GeoTIFF mode) |

---

#### 5.4.2 Non-flat Terrain

For scenes with significant elevation changes (hills, ramps, multi-level bridges). Uses camera
intrinsics from Stage 1 + estimated camera extrinsics + DEM ray-casting. Requires ≥ 4 reference
points with known 3D (X, Y, Z) world coordinates.

##### Coordinate input modes

| Mode | Parameter | Z source | Best for |
|------|-----------|----------|----------|
| **metric3d** | `coords_format="metric3d"` | Type (X, Y, Z) | Theodolite / total-station field data |
| **latlon3d** | `coords_format="latlon3d"` | Type (lat, lon, Z) | GPS position + level instrument |
| **GeoTIFF + DEM** | `geotiff_path=..., dem=DEM.from_geotiff(...)` | Auto from DEM | Lantmäteriet orthophoto + elevation model |
| **GeoTIFF + typed Z** | `geotiff_path=..., dem=None` | Typed per point | Lantmäteriet orthophoto + on-site Z measurements |

##### Workflow
1. Load camera intrinsics from Stage 1 (`cameralens_calibration.py`)
2. Load or prepare the DEM (`DEM.from_geotiff()` for Lantmäteriet products)
3. Click reference points on the GeoTIFF or video frame
4. Provide 3D world coordinates (typed or auto-extracted)
5. Camera pose is estimated with solvePnP
6. Project pixel trajectories to 3D world using DEM ray-casting

##### Outputs
| File | Description |
|------|-------------|
| `<video>_pose_f<idx>.npz` | Camera extrinsics (rvec, tvec, R, camera position) |
| `<video>_reference_f<idx>.png` | Annotated reference frame |
| `<video>_refpoints3d_f<idx>.csv` | Per-point 3D coordinate log |
| `<video>_origin_f<idx>.json` | WGS84 geodetic origin (if lat/lon or GeoTIFF mode) |

##### Quality — pixel reprojection error

Use `validate_elevation_video()` to measure hold-out reprojection error in pixels.

| RMSE | Quality |
|------|---------|
| < 2 px | Excellent |
| 2 – 5 px | Good — usable |
| 5 – 10 px | Marginal — check reference point quality and DEM resolution |
| > 10 px | Poor — review intrinsics and reference points |

---

## 6. Parquet schema: trajectory results

All trajectory data lives in a single flat file — one row per tracked object per frame.

| Column                 | Type    | Description                                         |
|------------------------|---------|-----------------------------------------------------|
| `object_id`            | int32   | Unique track identifier assigned by ByteTrack       |
| `class_id`             | int8    | Majority-voted model class ID (one value per track) |
| `class_name`           | str     | Class label                                         |
| `frame`                | int32   | Video frame index                                   |
| `x`                    | float32 | Bounding box centre X in pixels                     |
| `y`                    | float32 | Bounding box centre Y in pixels                     |
| `width`                | float32 | Bounding box width in pixels                        |
| `height`               | float32 | Bounding box height in pixels                       |
| `confidence`           | float32 | YOLO detection confidence (0–1)                     |
| `detected`             | bool    | `True` if the object was detected in this frame     |
| `class_id_raw`         | Int8    | Raw per-frame YOLO class for this detection.        |
| `xs`, `ys`             | float32 | *(optional)* Smoothed positions                     |
| `x_px`, `y_px`         | float32 | *(optional)* Original pixel positions (backup)      |
| `x_center`, `y_center` | float32 | *(optional)* Pre-correction positions (backup)      |