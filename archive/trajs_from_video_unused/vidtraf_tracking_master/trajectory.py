"""
Traffic analysis — detection and tracking of road users from video.
"""

import cv2
import numpy as np
import pandas as pd
from pathlib import Path
from collections import Counter, defaultdict
from typing import Any, cast
import bisect
from ultralytics import YOLO
from scipy.optimize import linear_sum_assignment
import video_utils as vu

# Parquet column schema.
parquet_dtype: dict[str, Any] = {
    "object_id":     "int32",
    "class_id":      "int8",
    "class_name":    "object",
    "frame":         "int32",
    "x":             "float32",
    "y":             "float32",
    "width":         "float32",
    "height":        "float32",
    "confidence":    "float32",
    "class_id_raw":  "Int8",
}


def tracker(video_path, model_name, track_config, output_path=None,
            conf=0.25, iou=0.7, imgsz=640,
            first_frame=0, last_frame=None, mask_path=None, classes=None,
            min_track_frames=15, verbose=True):
    """YOLO + ByteTrack tracking saved to a Parquet file.

    Positions are stored in raw pixel coordinates.
    Parquet schema:
        object_id | class_id | class_name | frame | x | y | width | height | confidence | class_id_raw

    :param video_path        [str or Path]         Input video file.
    :param model_name        [str]                 YOLO model name (e.g. "yolo12s.pt").
    :param track_config      [str]                 Tracker YAML name (e.g. "bytetrack.yaml").
    :param output_path       [str or Path | None]  Output trajectory file. Defaults to <video_stem>_traj.parquet.
    :param conf              [float]               Detection confidence threshold.
    :param iou               [float]               IoU threshold for NMS.
    :param imgsz             [int]                 YOLO input image size in pixels.
    :param first_frame       [int]                 First frame to process.
    :param last_frame        [int | None]          Last frame to process (None = end of video).
    :param mask_path         [str | None]          Binary mask image (white = detect, black = suppress).
    :param classes           [list | None]         Model class IDs to track (None = all classes).
    :param min_track_frames  [int]                 Fragmentation threshold for the tracking summary.
    :param verbose           [bool]                Print progress.

    :return  [dict]  {parquet_path, summary_path}.
    """

    # YOLO model — downloaded automatically on first use if model_name is a model family name
    model = YOLO(model_name)

    # Video setup
    cap, _, _, _, n_total_frames = vu.open_video(video_path)
    if last_frame is None:
        last_frame = n_total_frames - 1

    # Detection mask — pixels in suppressed regions are set to zero before YOLO sees the frame
    mask = None
    if mask_path is not None:
        mask = cv2.imread(str(mask_path), cv2.IMREAD_GRAYSCALE)
        if mask is None:
            raise FileNotFoundError(f"Cannot load mask: {mask_path}")

    # Detection and tracking
    if first_frame > 0:
        cap.set(cv2.CAP_PROP_POS_FRAMES, first_frame)
    tracks = {}   # track_id → {"frames": [(frame_num, cx, cy, w, h, conf), ...], "class_ids": [...]}
    frame_num = first_frame
    n_frames = last_frame - first_frame + 1
    reported_pcts = set()

    while cap.isOpened() and frame_num <= last_frame:
        ret, frame = cap.read()
        if not ret:
            break

        # Apply mask
        if mask is not None:
            m = (mask if mask.shape[:2] == frame.shape[:2]
                 else cv2.resize(mask, (frame.shape[1], frame.shape[0]),
                                 interpolation=cv2.INTER_NEAREST))
            frame = cv2.bitwise_and(frame, frame, mask=m)

        # YOLO detection + ByteTrack
        results = model.track(
            source=frame,
            tracker=track_config,
            classes=classes,
            conf=conf,
            iou=iou,
            imgsz=imgsz,
            persist=True,
            verbose=False,
        )

        # Each box in results[0].boxes is one detection matched to a track
        for box in results[0].boxes:
            if box.id is None:
                continue
            tid          = int(box.id.item())
            cls          = int(box.cls.item())
            box_conf     = float(box.conf.item())
            cx, cy, w, h = (float(v) for v in box.xywh[0])
            if tid not in tracks:
                tracks[tid] = {"frames": [], "class_ids": []}
            tracks[tid]["frames"].append((frame_num, cx, cy, w, h, box_conf, cls))
            tracks[tid]["class_ids"].append(cls)

        if verbose:
            pct = (frame_num - first_frame + 1) * 100 // n_frames
            milestone = pct // 25 * 25
            if milestone > 0 and milestone not in reported_pcts:
                reported_pcts.add(milestone)
                print(f"  {milestone}%  ({frame_num - first_frame + 1}/{n_frames} frames"
                      f"  active tracks: {len(tracks)})")
        frame_num += 1

    cap.release()

    # Majority-vote class ID — assign a single class_id per track from the most frequent detected class
    for d in tracks.values():
        d["class_id"] = Counter(d["class_ids"]).most_common(1)[0][0]

    # Assign class name from the model
    for d in tracks.values():
        d["class_name"] = model.names.get(d["class_id"], str(d["class_id"]))

    # Save to Parquet — one row per object per frame, with gap rows interpolated
    track_dfs: list[pd.DataFrame] = []
    for tid, d in tracks.items():
        tdf = pd.DataFrame(d["frames"], columns=["frame", "x", "y", "width", "height",
                                                 "confidence", "class_id_raw"])
        tdf = tdf.set_index("frame").reindex(range(int(tdf["frame"].min()), int(tdf["frame"].max()) + 1))
        tdf["detected"] = tdf["x"].notna()
        tdf["x"] = tdf["x"].interpolate(method="index")
        tdf["y"] = tdf["y"].interpolate(method="index")
        tdf["object_id"] = tid
        tdf["class_id"]  = d["class_id"]
        tdf["class_name"] = d["class_name"]
        tdf.index.name = "frame"
        track_dfs.append(tdf.reset_index())
    df = pd.concat(track_dfs, ignore_index=True)
    for col, dtype in parquet_dtype.items():
        df[col] = df[col].astype(dtype)
    df["detected"] = df["detected"].astype(bool)
    video_path   = Path(video_path)
    parquet_path = Path(output_path) if output_path is not None \
                   else video_path.with_name(video_path.stem + "_traj.parquet")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)

    # Summary
    print(f"  Saved {len(tracks)} tracks → {parquet_path.name}")
    summary_path = parquet_path.with_name(parquet_path.stem.replace("_traj", "") + "_summary.txt")
    tracking_summary(parquet_path, min_track_frames=min_track_frames)

    return {"parquet_path": parquet_path, "summary_path": summary_path}


def tracking_summary(traj, min_track_frames=15):
    """Print a per-class summary table and write it alongside the Parquet file.

    Track lengths are counted over detected frames only (gap rows excluded).
    Fragmentation is the percentage of tracks shorter than min_track_frames.

    :param traj             [str or Path]  Parquet trajectory file.
    :param min_track_frames [int]          Track length threshold for the fragmentation count.

    :return  None
    """
    traj = Path(traj)
    if not traj.exists():
        print(f"  [summary] file not found: {traj.name}")
        return
    output_path = traj.with_name(traj.stem.replace("_traj", "") + "_summary.txt")

    df = pd.read_parquet(traj)
    if "detected" in df.columns:
        df = df[df["detected"]]
    df = df[["object_id", "class_id", "class_name", "frame"]]
    uniq = df.drop_duplicates("object_id").set_index("object_id")
    obj_class_map = cast(dict[int, int], uniq["class_id"].to_dict())
    obj_name_map  = cast(dict[int, str], uniq["class_name"].to_dict())
    track_lengths = cast(dict[int, int], df.groupby("object_id")["frame"].count().to_dict())
    all_lengths = list(track_lengths.values())
    n_total     = len(all_lengths)
    lines = [f"\n── {traj.name} ──────────────────────────────"]

    if n_total == 0:
        lines.append("  No tracks found.")
    else:
        all_class_ids = sorted(set(obj_class_map.values()))

        class_lengths = {cid: [] for cid in all_class_ids}
        for oid, n in track_lengths.items():
            cid = obj_class_map.get(oid, -1)
            if cid in class_lengths:
                class_lengths[cid].append(n)

        class_label = {}
        for oid, cid in obj_class_map.items():
            if cid not in class_label:
                class_label[cid] = f"{cid} – {obj_name_map.get(oid, '?')}"

        hdr = f"  {'Class':<20} {'Tracks':>6}  {'Mean':>6}  {'Median':>6}  {'Min':>5}  {'Max':>5}  {'Frag%':>6}"
        sep = "  " + "─" * (len(hdr) - 2)
        lines += [hdr, sep]

        for cid in all_class_ids:
            label   = class_label.get(cid, str(cid))
            lengths = class_lengths[cid]
            n = len(lengths)
            if n == 0:
                lines.append(f"  {label:<20} {0:>6}  {'─':>6}  {'─':>6}  {'─':>5}  {'─':>5}  {'─':>6}")
            else:
                arr     = np.array(lengths)
                n_short = int((arr < min_track_frames).sum())
                lines.append(f"  {label:<20} {n:>6}  {arr.mean():>6.1f}  "
                              f"{float(np.median(arr)):>6.0f}  {int(arr.min()):>5}  "
                              f"{int(arr.max()):>5}  {n_short/n*100:>5.1f}%")

        lens    = np.array(all_lengths)
        n_short = int((lens < min_track_frames).sum())
        lines += [sep,
                  f"  {'TOTAL':<20} {n_total:>6}  {lens.mean():>6.1f}  "
                  f"{float(np.median(lens)):>6.0f}  {int(lens.min()):>5}  "
                  f"{int(lens.max()):>5}  {n_short/n_total*100:>5.1f}%"]

    for line in lines:
        print(line)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def merge_cyclists(traj, model_names=None, iou_overlap=0.15, prop_match=0.3, prop_tw=0.2):
    """Merge person+bicycle/motorcycle track pairs.

    YOLO often detects a cyclist as two separate tracks: a 'person' track for the rider
    and a 'bicycle' track for the frame. Two scenarios are handled:

      Scenario A — two tracks exist (person + bicycle):
        The two-wheeler track absorbs any pedestrian-only detected frames (filling position gaps);
         The pedestrian track is removed.
         IoU-based matching via the Hungarian algorithm.

      Scenario B — only a person track exists (bicycle box was unstable or occluded):
        A pedestrian track where more than prop_tw of its frames were detected as a two-wheeler class is reclassified
        as bicycle, without any structural merge;
         Uses raw per-frame YOLO class distribution.

    The input Parquet file is modified in place. A companion *_merged_log.txt is written recording every change made.

    :param traj           [str or Path]   Parquet trajectory file (modified in place).
    :param model_names    [dict | None]   class_id labels. When None, labels are read from class_name column.
    :param iou_overlap    [float]         Per-frame IoU threshold for a person/bike frame to count as a match.
    :param prop_match     [float]         Minimum fraction of two-wheeler detected frames that has a IoU-match.
    :param prop_tw        [float]         Minimum fraction of pedestrian frames with a two-wheeler detection
                                           (Scenario B fallback).

    :return  [dict]  {n_changes, log_path} or {n_changes: 0, log_path: None} when no cyclist classes were found.
    """
    traj = Path(traj)
    log_path = traj.with_name(traj.stem + "_merged_log.txt")
    df = pd.read_parquet(traj)
    has_detected = "detected" in df.columns

    # Build class-name lookup
    name_lookup: dict[int, str]
    if model_names is not None:
        name_lookup = model_names
    else:
        name_lookup = cast(dict[int, str], df[["class_id", "class_name"]].drop_duplicates("class_id")
                           .set_index("class_id")["class_name"].to_dict())
    ped_classes = {cid for cid, name in name_lookup.items()
                   if any(k in name.lower() for k in ('person', 'pedestrian', 'people'))}
    tw_classes  = {cid for cid, name in name_lookup.items()
                   if any(k in name.lower() for k in ('bicycle', 'bike', 'motorcycle', 'motorbike', 'motor'))}

    # End if no person or two-wheeler classes are found
    if not ped_classes or not tw_classes:
        print("No class names for pedestrian or two-wheelers in Parquet file")
        return {"n_changes": 0, "log_path": None}

    obj_class = cast(dict[int, int], df.drop_duplicates("object_id").set_index("object_id")["class_id"].to_dict())
    tw_ids    = [oid for oid, cid in obj_class.items() if cid in tw_classes]
    ped_ids   = [oid for oid, cid in obj_class.items() if cid in ped_classes]

    # End if no person or two-wheeler tracks are found
    if not tw_ids or not ped_ids:
        print("No tracks for pedestrians or two-wheelers exist in Parquet file")
        return {"n_changes": 0, "log_path": None}

    det = df[df["detected"]] if has_detected else df

    def _bboxes(oid):
        rows = det[det["object_id"] == oid]
        result = {}
        for fn, cx, cy, w, h in zip(rows["frame"], rows["x"], rows["y"], rows["width"], rows["height"]):
            if pd.notna(w) and pd.notna(h) and float(w) > 0 and float(h) > 0:
                fw, fh = float(w), float(h)
                result[int(fn)] = (float(cx) - fw/2, float(cy) - fh/2, float(cx) + fw/2, float(cy) + fh/2)
        return result

    def _iou(a, b):
        ix1 = max(a[0], b[0]); iy1 = max(a[1], b[1])
        ix2 = min(a[2], b[2]); iy2 = min(a[3], b[3])
        inter = max(0.0, ix2 - ix1) * max(0.0, iy2 - iy1)
        if inter == 0.0:
            return 0.0
        return inter / ((a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - inter)

    tw_boxes  = {oid: _bboxes(oid) for oid in tw_ids}
    ped_boxes = {oid: _bboxes(oid) for oid in ped_ids}

    # Cost matrix [n_tw × n_ped]: fraction of two-wheeler frames IoU-matching the ped
    n_tw, n_ped = len(tw_ids), len(ped_ids)
    costs = np.zeros((n_tw, n_ped))
    for i, tw_oid in enumerate(tw_ids):
        boxes = tw_boxes[tw_oid]
        n = len(boxes)
        if n == 0:
            continue
        for j, ped_oid in enumerate(ped_ids):
            ped_b = ped_boxes[ped_oid]
            nmatches = sum(1 for t, b in boxes.items() if t in ped_b and _iou(b, ped_b[t]) > iou_overlap)
            costs[i, j] = nmatches / n

    # Scenario B
    reclassify = {}  # ped_oid → (new_class_id, new_class_name)
    has_per_frame_cls = "class_id_raw" in df.columns
    for j, ped_oid in enumerate(ped_ids):
        if (costs[:, j] >= prop_match).any():
            continue  # will be handled by Hungarian matching below
        if not has_per_frame_cls:
            continue
        ped_det = det[det["object_id"] == ped_oid]
        tw_det_count = ped_det["class_id_raw"].isin(tw_classes).sum()
        total_det    = len(ped_det)
        if total_det > 0 and tw_det_count > prop_tw * total_det:
            tw_dets  = ped_det.loc[ped_det["class_id_raw"].isin(tw_classes), "class_id_raw"]
            best_cls = int(tw_dets.value_counts().index[0])
            reclassify[ped_oid] = (best_cls, name_lookup.get(best_cls, str(best_cls)))

    # Hungarian matching — one-to-one, maximize total overlap score
    row_inds, col_inds = linear_sum_assignment(-costs)
    peds_to_delete  = []
    ped_to_tw       = {}  # ped_oid → tw_oid (for log)
    rebuilt_tracks  = {}  # tw_oid → rebuilt DataFrame (only populated when ped adds new frames)

    for i, j in zip(row_inds, col_inds):
        if costs[i, j] < prop_match:
            continue
        tw_oid  = tw_ids[int(i)]
        ped_oid = ped_ids[int(j)]
        ped_to_tw[ped_oid] = tw_oid

        extra_frames = set(ped_boxes[ped_oid]) - set(tw_boxes[tw_oid])
        if extra_frames:
            # Pedestrian has detections in frames where the bike was absent — absorb them
            ped_extra = det[(det["object_id"] == ped_oid) &
                            (det["frame"].isin(extra_frames))].copy()
            ped_extra["object_id"]  = tw_oid
            ped_extra["class_id"]   = obj_class[tw_oid]
            ped_extra["class_name"] = name_lookup.get(obj_class[tw_oid], str(obj_class[tw_oid]))

            tw_det = det[det["object_id"] == tw_oid].copy()
            combined = (pd.concat([tw_det, ped_extra], ignore_index=True)
                          .sort_values("frame"))

            # Rebuild with gap interpolation (mirrors what tracker() does when saving)
            cid, cname = int(obj_class[tw_oid]), name_lookup.get(obj_class[tw_oid], str(obj_class[tw_oid]))
            rebuild_cols = ["x", "y", "width", "height", "confidence"] + \
                           (["class_id_raw"] if "class_id_raw" in combined.columns else [])
            tdf = combined.set_index("frame")[rebuild_cols]
            tdf = tdf[~tdf.index.duplicated(keep="first")]
            tdf = tdf.reindex(range(int(tdf.index.min()), int(tdf.index.max()) + 1))
            tdf["detected"]    = tdf["x"].notna()
            tdf["x"]           = tdf["x"].interpolate(method="index")
            tdf["y"]           = tdf["y"].interpolate(method="index")
            tdf["object_id"]   = tw_oid
            tdf["class_id"]    = cid
            tdf["class_name"]  = cname
            tdf.index.name     = "frame"
            rebuilt_tracks[tw_oid] = tdf.reset_index()

        peds_to_delete.append(ped_oid)

    n_changes = len(peds_to_delete) + len(reclassify)

    if n_changes == 0:
        return {"n_changes": 0, "log_path": None}

    # Drop pedestrian tracks and any two-wheeler tracks that were rebuilt
    remove_ids = set(peds_to_delete) | set(rebuilt_tracks)
    df = df[~df["object_id"].isin(remove_ids)]

    if rebuilt_tracks:
        df = pd.concat([df] + list(rebuilt_tracks.values()), ignore_index=True)

    # Apply Scenario B reclassifications (label-only change, no structural merge)
    for ped_oid, (new_cid, new_cname) in reclassify.items():
        mask = df["object_id"] == ped_oid
        df.loc[mask, "class_id"]   = new_cid
        df.loc[mask, "class_name"] = new_cname

    # Audit columns — written to the merged Parquet so every change is traceable
    df["merged"] = False
    df["absorbed_from"] = pd.array([pd.NA] * len(df), dtype="Int32")
    for ped_oid, tw_oid in ped_to_tw.items():
        mask = df["object_id"] == tw_oid
        df.loc[mask, "merged"] = True
        df.loc[mask, "absorbed_from"] = ped_oid
    for ped_oid in reclassify:
        df.loc[df["object_id"] == ped_oid, "merged"] = True

    df = df.sort_values(["object_id", "frame"]).reset_index(drop=True)
    for col, dtype in parquet_dtype.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    if "detected" in df.columns:
        df["detected"] = df["detected"].astype(bool)
    df["merged"] = df["merged"].astype(bool)

    df.to_parquet(traj, index=False)

    # Write change log
    log_lines = [
        "Cyclist merge log",
        "=" * 60,
        f"File:       {traj.name}",
        f"Parameters: iou_overlap={iou_overlap}  prop_match={prop_match}  prop_tw={prop_tw}",
        "",
    ]
    if peds_to_delete:
        log_lines.append("Scenario A — merged (person track absorbed into two-wheeler):")
        for ped_oid in peds_to_delete:
            tw_oid   = ped_to_tw[ped_oid]
            ped_name = name_lookup.get(obj_class[ped_oid], str(obj_class[ped_oid]))
            tw_name  = name_lookup.get(obj_class[tw_oid],  str(obj_class[tw_oid]))
            log_lines.append(f"  object_id {ped_oid} ({ped_name}) → absorbed into object_id {tw_oid} ({tw_name})")
        log_lines.append("")
    if reclassify:
        log_lines.append("Scenario B — reclassified (person track relabelled as two-wheeler):")
        for ped_oid, (new_cid, new_cname) in reclassify.items():
            ped_name = name_lookup.get(obj_class[ped_oid], str(obj_class[ped_oid]))
            log_lines.append(f"  object_id {ped_oid} ({ped_name}) → {new_cname} (class_id {new_cid})")
        log_lines.append("")
    log_lines.append(f"Total changes: {n_changes}")
    log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    print(f"  Cyclist merge: {n_changes} change(s) → {traj.name}")
    return {"n_changes": n_changes, "log_path": log_path}


def smooth_trajectories(traj, window=5, cols=("x", "y"), out_cols=None):
    """Smooth trajectory positions in a Parquet file using a symmetric moving average.

    The input Parquet file is modified in place.

    :param traj          [str or Path]             Parquet trajectory file.
    :param window        [int]                     Moving-average window size in frames.
    :param cols          [tuple[str, ...]]         Input columns to smooth. Default ("x", "y").
    :param out_cols      [tuple[str, ...] | None]  Output column names. Defaults to appending "s" to each input name.

    :return  None
    """
    if out_cols is None:
        out_cols = tuple(c + "s" for c in cols)
    window = window if window % 2 == 1 else window + 1  # enforce odd

    df = pd.read_parquet(traj).sort_values(["object_id", "frame"])
    for col, out in zip(cols, out_cols):
        df[out] = (df.groupby("object_id")[col]
                   .transform(lambda s: s.rolling(window, center=True, min_periods=1).mean())
                   .astype("float32"))
    df.to_parquet(traj, index=False)


def apply_homography_to_trajectories(traj, homography_mat):
    """Convert pixel-coordinate trajectories in a Parquet file to world coordinates by applying a homography matrix.

    The input Parquet file is modified in place.

    :param traj            [str or Path]  Parquet trajectory file.
    :param homography_mat  [str or Path]  3x3 homography .txt file (pixel → world).

    :return  None
    """
    H  = np.loadtxt(homography_mat)
    df = pd.read_parquet(traj)

    df["x_px"] = df["x"]
    df["y_px"] = df["y"]

    # Vectorised homography projection: stack [x, y, 1] rows, multiply by H
    xy1   = np.column_stack([df["x"].values, df["y"].values, np.ones(len(df))])
    world = (H @ xy1.T).T                        # (N, 3)
    df["x"] = (world[:, 0] / world[:, 2]).astype("float32")
    df["y"] = (world[:, 1] / world[:, 2]).astype("float32")

    df.to_parquet(traj, index=False)


def correct_height_parallax(traj, camera_height, camera_world_pos, object_height=1.8):
    """Correct world-coordinate trajectories for the parallax error caused by tracking the bounding-box centre
    (mid-body) instead of the ground contact point. Relevant for low angle perspectives <30 degrees

    Applies the analytical ray-intersection correction:
        scale   = (camera_height - person_height/2) / camera_height
        x_corr  = x_cam + scale * (x_w - x_cam)
        y_corr  = y_cam + scale * (y_w - y_cam)

    The original centre-based positions are preserved as x_center / y_center backup columns.

    The input Parquet file is modified in place.

    :param traj              [str or Path]  Parquet trajectory file (world coordinates).
    :param camera_height     [float]        Camera height above the ground plane (metres).
    :param camera_world_pos  [tuple]        (x, y) world coordinates of the camera's
                                            ground-projection point (metres).
    :param object_height     [float]        Assumed standing object height in metres (default person=1.8).

    :return  None
    """
    h_c = object_height / 2.0
    scale = (camera_height - h_c) / camera_height
    x_cam, y_cam = float(camera_world_pos[0]), float(camera_world_pos[1])
    df = pd.read_parquet(traj)

    # Backup centre-based world positions
    df["x_center"] = df["x"]
    df["y_center"] = df["y"]

    # Transform
    df["x"] = (x_cam + scale * (df["x"] - x_cam)).astype("float32")
    df["y"] = (y_cam + scale * (df["y"] - y_cam)).astype("float32")

    df.to_parquet(traj, index=False)


# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
# Visualisation
# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
_UNKNOWN_COLOR = (200, 200, 200)   # fallback BGR for unrecognised class IDs


def id_color(class_id):
    """Deterministic BGR colour derived from an integer class ID."""
    h = (class_id * 37) % 360
    s, v = 0.85, 0.90
    c = v * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = v - c
    if   h < 60:  r, g, b = c, x, 0
    elif h < 120: r, g, b = x, c, 0
    elif h < 180: r, g, b = 0, c, x
    elif h < 240: r, g, b = 0, x, c
    elif h < 300: r, g, b = x, 0, c
    else:         r, g, b = c, 0, x
    return (int((b + m) * 255), int((g + m) * 255), int((r + m) * 255))  # BGR


def dotted_line(img, pt1, pt2, color, radius=2, spacing=8):
    """Draw a dotted line between two points."""
    x1, y1 = pt1
    x2, y2 = pt2
    dx, dy = x2 - x1, y2 - y1
    length = (dx ** 2 + dy ** 2) ** 0.5
    if length == 0:
        return
    ux, uy = dx / length, dy / length
    pos = 0.0
    while pos <= length:
        cx = int(round(x1 + ux * pos))
        cy = int(round(y1 + uy * pos))
        cv2.circle(img, (cx, cy), radius, color, -1)
        pos += spacing


def draw_label(img, lines, pt, color, scale):
    """Draw one or more text lines with a shared white background box."""
    font      = cv2.FONT_HERSHEY_SIMPLEX
    thickness = 1
    x, y      = pt

    metrics = [cv2.getTextSize(line, font, scale, thickness) for line in lines]

    ref_h = metrics[0][0][1]          # text height of first line, scales with label_scale
    pad   = max(1, ref_h // 5)
    gap   = max(1, ref_h // 4)

    max_w = max(m[0][0] for m in metrics)

    # Compute baseline y for each line; first line anchored at y
    ys = [y]
    for i in range(1, len(lines)):
        ys.append(ys[-1] + metrics[i - 1][1] + gap + metrics[i][0][1])

    top    = y - metrics[0][0][1] - pad
    bottom = ys[-1] + metrics[-1][1] + pad
    cv2.rectangle(img, (x - pad, top), (x + max_w + pad, bottom), (255, 255, 255), -1)
    for line, (metric, ly) in zip(lines, zip(metrics, ys)):
        cv2.putText(img, line, (x, ly), font, scale, color, thickness, cv2.LINE_AA)


def annotated_video(input_video, traj, output_video,
                    trail_length=60, show_ids=True, label_scale=0.45,
                    class_names=None, class_colors=None,
                    min_track_length=None,
                    first_frame=None, last_frame=None,
                    xy_cols=("x", "y"), show_bbox=True):
    """
    Overlay tracked trajectories (from a Parquet trajectory file) onto a video.
        Positions must already be in pixel coordinates.

    :param input_video           [str or Path]        Video the tracking ran on.
    :param traj                  [str or Path]        Parquet trajectory file.
    :param output_video          [str or Path]        Destination annotated video file (.mp4).
    :param trail_length          [int or "all"]       Number of past frames drawn as a trailing line.
                                                      Pass "all" to draw the full trajectory from the first detection.
    :param show_ids              [bool]               Draw a label with object ID (and class name if class_names given).
    :param label_scale           [float]              Font scale for the object label.
    :param class_names           [dict[int, str]]     Mapping of class_id → class name string.
                                                      When provided, the class name is appended to the label.
    :param class_colors          [dict[int, tuple]]   Mapping of class_id → BGR colour tuple.
                                                      When provided, colours are taken from this map.
                                                      When None, colours are derived from class_id via id_color().
    :param min_track_length      [int]                Skip objects with fewer detected frames than this.
    :param first_frame           [int]                First frame index to annotate (inclusive). Defaults to 0.
    :param last_frame            [int]                Last frame index to annotate (inclusive).
                                                      Defaults to the last frame in the video.
    :param xy_cols               [tuple[str, str]]    Parquet columns to use for (x, y) positions.
                                                      Default ("x", "y").
    :param show_bbox             [bool]               Draw the detection bounding box on detected frames.

    :return  [int]  Number of frames written.
    """
    x_col, y_col = xy_cols
    df = pd.read_parquet(Path(traj))
    has_detected = "detected" in df.columns
    has_wh = "width" in df.columns and "height" in df.columns
    keep = ["object_id", "class_id", "frame", x_col, y_col] + \
           (["detected"] if has_detected else []) + \
           (["width", "height"] if has_wh else [])
    df_sorted = df[keep].sort_values(["object_id", "frame"])
    obj_class = cast(dict[int, int], df_sorted.drop_duplicates("object_id").set_index("object_id")["class_id"].to_dict())
    traj_frames = defaultdict(list)
    _nan = float("nan")
    det_vals = df_sorted["detected"].values if has_detected else None
    w_vals = df_sorted["width"].to_numpy(dtype=float, na_value=_nan) if has_wh else None
    h_vals = df_sorted["height"].to_numpy(dtype=float, na_value=_nan) if has_wh else None
    for i, (oid, fn, x, y) in enumerate(zip(df_sorted["object_id"].values,
                                             df_sorted["frame"].values,
                                             df_sorted[x_col].values,
                                             df_sorted[y_col].values)):
        det = bool(det_vals[i]) if det_vals is not None else True
        bw = float(w_vals[i]) if w_vals is not None else _nan
        bh = float(h_vals[i]) if h_vals is not None else _nan
        traj_frames[int(oid)].append((int(fn), float(x), float(y), det, bw, bh))

    obj_data = []
    for oid, frames in traj_frames.items():
        detected_frames = [r for r in frames if r[3]]
        if min_track_length is not None and len(detected_frames) < min_track_length:
            continue
        cid = int(obj_class.get(oid, 0))
        color = class_colors.get(cid, _UNKNOWN_COLOR) if class_colors else id_color(cid)
        label = [str(oid), class_names.get(cid, str(cid))] if class_names else [str(oid)]
        sorted_fns = [r[0] for r in frames]
        frame_dict = {fn: (x, y, det, bw, bh) for fn, x, y, det, bw, bh in frames}
        obj_data.append((sorted_fns[0], sorted_fns[-1], sorted_fns, frame_dict, color, oid, label))

    cap, fps, w, h, n_frames = vu.open_video(input_video)
    start_frame = first_frame if first_frame is not None else 0
    stop_frame  = (min(last_frame + 1, n_frames) if last_frame is not None else n_frames)
    fourcc  = cv2.VideoWriter.fourcc(*"mp4v")
    writer  = cv2.VideoWriter(str(output_video), fourcc, fps, (w, h))
    if start_frame > 0:
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    written = 0
    for frame_idx in range(start_frame, stop_frame):
        ret, frame = cap.read()
        if not ret:
            break

        for t0, t1, sorted_fns, frame_dict, color, obj_id, label in obj_data:
            if frame_idx < t0 or frame_idx > t1:
                continue
            if frame_idx not in frame_dict:
                continue
            x_pos, y_pos, is_detected, bbox_w, bbox_h = frame_dict[frame_idx]
            cx, cy = int(round(x_pos)), int(round(y_pos))

            # Draw bounding box (detected frames only — interpolated frames have no valid w/h)
            if show_bbox and is_detected and not np.isnan(bbox_w) and not np.isnan(bbox_h):
                x1 = int(round(cx - bbox_w / 2))
                y1 = int(round(cy - bbox_h / 2))
                x2 = int(round(cx + bbox_w / 2))
                y2 = int(round(cy + bbox_h / 2))
                cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

            # Draw trail — solid between consecutive detected frames, dotted through gaps.
            end_idx   = bisect.bisect_right(sorted_fns, frame_idx)
            start_idx = 0 if trail_length == "all" else max(0, end_idx - trail_length)
            trail_fns = sorted_fns[start_idx:end_idx]
            pts = [(int(round(frame_dict[fn][0])), int(round(frame_dict[fn][1]))) for fn in trail_fns]
            for i in range(1, len(pts)):
                det_prev = frame_dict[trail_fns[i - 1]][2]
                det_curr = frame_dict[trail_fns[i]][2]
                if det_prev and det_curr:
                    cv2.line(frame, pts[i - 1], pts[i], color, 2)
                else:
                    dotted_line(frame, pts[i - 1], pts[i], color)

            cv2.circle(frame, (cx, cy), 5, color, -1)
            if show_ids:
                draw_label(frame, label, (cx + 6, cy - 4), color, label_scale)

        writer.write(frame)
        written += 1
        pct = (frame_idx - start_frame) * 100 // (stop_frame - start_frame)
        if pct in {0, 25, 50, 75} and (frame_idx == start_frame or (frame_idx - 1 - start_frame) * 100 // (stop_frame - start_frame) < pct):
            print(f"  {pct}%")

    cap.release()
    writer.release()
    print(f"  100%")
    return written
