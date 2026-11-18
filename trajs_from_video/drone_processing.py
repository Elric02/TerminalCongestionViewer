"""
Pipeline for processing drone footage:
  Find        — locate clips in data_dir and group by recording continuity      (always runs)
  Join        — concatenate clips per flying period into one video              (do_join = True)
  Stability   — measure frame-to-frame motion                                   (do_stability = True)
  Trim        — frame cut of the joined video                                   (do_trim = True)
  Stabilize   — register frames to a reference frame via stabilo                (do_stabilize = True)
  Mask        — apply a binary mask per mask_cuts; draws polygon if PNG missing (do_mask = True)
  Crop        — crop videos to a fixed region of interest                       (do_crop = True)
"""
import re
import shutil
from datetime import timedelta
import vidtraf_tracking_master.video_utils as video_utils
from config import video_folders

folders = video_folders([
    # ("Project", "City", "Location", "Video_Folder")
])
location_name = ""   # prefix for filenames

# Switches
do_join       = False  # join clips per period into one video
do_join_srt   = False  # also merge .SRT files into one when joining
do_srt        = False  # convert .srt files to .csv (independent of join)
do_time_shift = False  # add a constant offset to all .srt + .csv timestamps (fix a drone-clock error)
do_trim       = False  # trim the joined video per trim_cuts below
do_stability  = False  # measure frame-to-frame motion per stability_videos below
do_stabilize  = False  # register frames to a reference frame
do_mask       = False  # apply binary mask per mask_cuts below
do_crop       = False  # crop joined videos per crop_cuts below

# Join settings
gap_min       = 1     # clips with a gap ≤ this (minutes) are grouped into one period

# Time-shift settings
time_shift_hours = 0   # hours to add to every .srt + .csv timestamp (e.g. 1 = +1h, -0.5 = -30 min)

# Trim settings
# trim_cuts: joined video stem → (start_frame, end_frame)
#   None for start_frame → keep from frame 0
#   None for end_frame   → keep to the last frame
#   inspect = True       → open the frame inspector to pick both cut points interactively
inspect   = True
trim_cuts = {
    "video_stem": (0, None),
}

# Stability settings
stability_videos = []   # stems to check; empty = all .mp4 found in data_dir
stability_sample = 30   # analyze every Nth frame (~6 samples/s at 30 fps)

# Stabilize settings
stabilize_videos = [
    "video_stem",
]   # stems to stabilize; empty = all .mp4 found in data_dir
stabilize_reference_frame   = 0      # frame index used as the registration target
stabilize_write_diagnostics = True   # save a _log.npz next to the output

# Mask settings
# mask_cuts: video stem → mask PNG path (grayscale: white = keep, black = zero out)
#   {}  → all videos; mask PNG is saved next to each video as <stem>_mask.png
mask_mode          = 'poly'  # 'poly' = click polygon vertices, 'rect' = drag rectangle
mask_frame_index   = 0       # initial frame shown in the editor (only used if PNG doesn't exist yet)
mask_review_frames = True    # step through the video
mask_edit_existing = True    # reopen an existing mask PNG to add/undo shapes instead of reusing it as-is
mask_cuts = {
    "video_stem": None,
}

# Crop settings
# crop_cuts: video stem → (x1, y1, x2, y2) pixel coordinates, or None to draw interactively
#   {} → all videos, each cropped interactively
crop_cuts: dict[str, tuple[int, int, int, int] | None] = {}

# ======================================================================================================================


def fmt_hhmm(total_minutes):
    return f"{total_minutes // 60:02d}{total_minutes % 60:02d}"


def joined_stem(g):
    t0, t1 = fmt_hhmm(g[0]["start"]), fmt_hhmm(g[-1]["end"])
    return "_".join(p for p in (location_name, g[0]["date"], f"{t0}-{t1}") if p)


def parse_times(stem):
    m = time_pattern.search(stem)
    if not m:
        return None, None
    return int(m.group(1)) * 60 + int(m.group(2)), int(m.group(3)) * 60 + int(m.group(4))


def collect_and_sort(folder):
    result = []
    for f in sorted(folder.rglob("*")):
        if not f.is_file() or f.suffix.lower() not in video_extensions:
            continue
        if any(tag in f.stem for tag in ("_traj", "_mode", "_trimmed")):
            continue
        if joined_name_re and joined_name_re.match(f.stem):
            continue   # a previous run's joined output, not a source clip
        start, end = parse_times(f.stem)
        if start is None:
            print(f"  WARNING: cannot parse time from {f.name} — skipping")
            continue
        dm = date_pattern.search(f.stem)
        result.append({"path": f, "start": start, "end": end, "date": dm.group(0) if dm else ""})
    return sorted(result, key=lambda v: (v["date"], v["start"], v["end"]))


def group_by_continuity(videos, tolerance):
    groups = [[videos[0]]]
    for v in videos[1:]:
        prev = groups[-1][-1]
        if v["date"] == prev["date"] and v["start"] - prev["end"] <= tolerance:
            groups[-1].append(v)
        else:
            groups.append([v])
    return groups


# %% Find ==============================================================================================================
video_extensions = {".mp4", ".mov", ".avi", ".mkv"}
time_pattern     = re.compile(r"(\d{1,2})[.:]?(\d{2})-(\d{1,2})[.:]?(\d{2})")
date_pattern     = re.compile(r"(?<!\d)\d{6}(?!\d)")   # YYMMDD in clip filenames
joined_name_re   = re.compile(rf"^{re.escape(location_name)}_\d{{6}}_\d{{4}}-\d{{4}}$") if location_name else None
all_groups = {}   # folder → list of periods; each period is a list of video dicts

if do_join or do_join_srt:
    for folder in folders:
        videos = collect_and_sort(folder)
        if not videos:
            print(f"\n{folder.name}: no videos found.")
            continue

        print(f"\n{'=' * 60}")
        print(f"Folder: {folder.name} [{len(videos)} clips]")

        groups = group_by_continuity(videos, gap_min)
        print(f"{len(groups)} period(s)  (gap tolerance: {gap_min} min):")
        for i, g in enumerate(groups, 1):
            t0, t1, n = fmt_hhmm(g[0]["start"]), fmt_hhmm(g[-1]["end"]), len(g)
            print(f"\n  Period {i}: {t0} → {t1}  ({n} clip{'s' if n > 1 else ''})")
            for v in g:
                print(f"    {v['path'].name}")
            if i < len(groups):
                nxt = groups[i][0]
                if nxt["date"] == g[-1]["date"]:
                    print(f"    --- gap: {nxt['start'] - g[-1]['end']} min ---")
                else:
                    print(f"    --- date change: {g[-1]['date']} → {nxt['date']} ---")

        all_groups[folder] = groups
else:
    all_videos = sorted(p for folder in folders for p in folder.rglob("*.mp4"))
    active_ops = [
        ("Trim",      [v for v in all_videos if v.stem in trim_cuts])
            if do_trim and trim_cuts else None,
        ("Stabilize", [v for v in all_videos if not stabilize_videos or v.stem in stabilize_videos])
            if do_stabilize else None,
        ("Stability", [v for v in all_videos if "_tmp" not in v.stem
                       and (not stability_videos or v.stem in stability_videos)])
            if do_stability else None,
        ("Mask",      [v for v in all_videos if not mask_cuts or v.stem in mask_cuts])
            if do_mask else None,
        ("Crop",      [v for v in all_videos if not crop_cuts or v.stem in crop_cuts])
            if do_crop else None,
    ]
    for entry in active_ops:
        if entry is None:
            continue
        op_name, matched = entry
        print(f"\n{op_name} — {len(matched)} video(s):")
        for v in matched:
            print(f"  {v.name}")


# %% Join ==============================================================================================================

if do_join or do_join_srt:
    print(f"\n{'=' * 60}")
    print("Joining ...")
    for folder, groups in all_groups.items():
        for i, g in enumerate(groups, 1):
            stem       = joined_stem(g)
            clips      = [v["path"] for v in g]
            out_folder = clips[0].parent          # keep the join beside its source clips (per-drone folder)
            out_path   = out_folder / f"{stem}.mp4"

            if len(g) == 1:
                if do_join:
                    try:
                        shutil.copy2(clips[0], out_path)          # overwrites an existing file of the same name
                        note = "copied"
                    except shutil.SameFileError:
                        note = "already in place"                 # source clip already carries the joined name
                    print(f"\n  Period {i} ({fmt_hhmm(g[0]['start'])}-{fmt_hhmm(g[-1]['end'])}): single clip → {out_path.name} ({note})")
                if do_join_srt:
                    src_srt = clips[0].with_suffix(".SRT")
                    if not src_srt.exists():
                        src_srt = clips[0].with_suffix(".srt")
                    dst_srt = out_path.with_suffix(".srt")
                    if src_srt.exists():
                        try:
                            shutil.copy2(src_srt, dst_srt)
                        except shutil.SameFileError:
                            pass
                        print(f"  SRT → {dst_srt.name}")
                    else:
                        print(f"    WARNING: no SRT for {clips[0].name}")
                continue

            if do_join:
                vu.join_video(clips, out_path)
                print(f"  Video → {out_path.name}")
                vu.check_video_timing(out_path)   # catch a splice going wrong before any later re-encode hides it

            if do_join_srt:
                srt_files = []
                for clip in clips:
                    srt = clip.with_suffix(".SRT")
                    if not srt.exists():
                        srt = clip.with_suffix(".srt")
                    if not srt.exists():
                        print(f"    WARNING: no SRT for {clip.name} — skipping SRT merge")
                        srt_files = []
                        break
                    vu.pad_srt(srt, clip)
                    issues = vu.check_srt(clip, srt)
                    if issues:
                        print(f"    SRT check warnings for {srt.name}:")
                        for issue in issues:
                            print(f"      - {issue}")
                    srt_files.append(srt)
                if srt_files:
                    srt_out = out_path.with_suffix(".srt")
                    if vu.join_srt(srt_files, srt_out):
                        print(f"  SRT → {srt_out.name}")
                        vu.check_srt(out_path, srt_out)
    print("Done.")


# %% SRT → CSV =========================================================================================================

if do_srt:
    print(f"\n{'=' * 60}")
    print("Converting SRT → CSV ...")
    for folder in folders:
        for srt_path in sorted(p for p in folder.rglob("*") if p.suffix.lower() == ".srt"):
            video_path = next((c for c in srt_path.parent.glob(f"{srt_path.stem}.*")
                               if c.suffix.lower() in video_extensions), None)
            if video_path:
                vu.pad_srt(srt_path, video_path)
            csv_path = srt_path.with_suffix(".csv")
            n = vu.srt_to_csv(srt_path, csv_path)
            print(f"  {srt_path.name} → {csv_path.name}  ({n} frames)")
            if video_path:
                vu.check_frame_counts(video_path, srt_path, csv_path)
                issues = vu.check_csv(video_path, csv_path)
                if issues:
                    print(f"  CSV check warnings for {csv_path.name}:")
                    for issue in issues:
                        print(f"    - {issue}")
    print("Done.")


# %% Time shift ========================================================================================================

if do_time_shift and time_shift_hours:
    delta = timedelta(hours=time_shift_hours)
    print(f"\n{'=' * 60}")
    print(f"Shifting .srt + .csv timestamps by {time_shift_hours:+g} h ...")
    for folder in folders:
        for p in sorted(f for f in folder.rglob("*") if f.suffix.lower() in (".srt", ".csv")):
            n = vu.shift_timestamps(p, delta)
            print(f"  {p.name}: {n} timestamps")
    print("Done.")


# %% Stability =========================================================================================================

if do_stability:
    print(f"\n{'=' * 60}")
    print("Measuring stability ...")
    for video_path in sorted(p for folder in folders for p in folder.rglob("*.mp4")):
        if "_tmp" in video_path.stem:
            continue
        if stability_videos and video_path.stem not in stability_videos:
            continue
        vu.measure_stability(video_path, sample_every=stability_sample)
    print("Done.")


# %% Trim ==============================================================================================================

if do_trim and trim_cuts:
    print(f"\n{'=' * 60}")
    print("Trimming ...")
    for video_path in sorted(p for folder in folders for p in folder.rglob("*.mp4")):
        stem = video_path.stem
        if stem not in trim_cuts:
            continue
        start_frame, end_frame = trim_cuts[stem]
        trimmed_path = video_path.with_name(stem + "_trimmed.mp4")
        out, confirmed_end = vu.trim_video(
            video_path, trimmed_path,
            start_frame=start_frame,
            end_frame=end_frame,
            inspect=inspect,
        )
        if out is None:
            print(f"  Trim aborted for {video_path.name} — skipping.")
            continue
        print(f"  Trimmed → {trimmed_path.name}")
        for ext, fn in ((".srt", vu.trim_srt), (".csv", vu.trim_csv)):
            companion = video_path.with_suffix(ext)
            if companion.exists():
                trimmed_companion = trimmed_path.with_suffix(ext)
                trimmed_companion.write_bytes(companion.read_bytes())
                fn(trimmed_companion, start_frame=start_frame, end_frame=confirmed_end)
        trimmed_srt, trimmed_csv = trimmed_path.with_suffix(".srt"), trimmed_path.with_suffix(".csv")
        vu.check_frame_counts(trimmed_path, trimmed_srt, trimmed_csv)
        vu.check_video_timing(trimmed_path)   # re-encode is fresh CFR, but confirm no drops/dupes crept in
        for check_name, check_fn, companion in (("SRT", vu.check_srt, trimmed_srt), ("CSV", vu.check_csv, trimmed_csv)):
            if companion.exists():
                issues = check_fn(trimmed_path, companion)
                if issues:
                    print(f"  {check_name} check warnings for {companion.name}:")
                    for issue in issues:
                        print(f"    - {issue}")
    print("Done.")


# %% Stabilize =========================================================================================================

if do_stabilize:
    print(f"\n{'=' * 60}")
    print("Stabilizing ...")
    for video_path in sorted(p for folder in folders for p in folder.rglob("*.mp4")):
        stem = video_path.stem
        if stabilize_videos and stem not in stabilize_videos:
            continue
        stab_path = video_path.with_name(stem + "_stab.mp4")
        vu.stabilize_video(
            video_path, stab_path,
            reference_frame_index=stabilize_reference_frame,
            write_diagnostics=stabilize_write_diagnostics,
        )
        print(f"  Stabilized → {stab_path.name}")
    print("Done.")


# %% Mask ==============================================================================================================

if do_mask:
    print(f"\n{'=' * 60}")
    print("Masking ...")
    for video_path in sorted(p for folder in folders for p in folder.rglob("*.mp4")):
        stem = video_path.stem
        if mask_cuts and stem not in mask_cuts:
            continue
        mask_path = mask_cuts.get(stem) or video_path.parent / f"{stem}_mask.png"
        masked_path = video_path.with_name(stem + "_masked.mp4")
        out = vu.mask_video(video_path, masked_path, mask_path, mode=mask_mode,
                            frame_index=mask_frame_index, review_frames=mask_review_frames,
                            edit_mask=mask_edit_existing)
        if out is None:
            print(f"  Mask aborted for {video_path.name} — skipping.")
            continue
        print(f"  Masked → {masked_path.name}")
    print("Done.")


# %% Crop ==============================================================================================================

if do_crop:
    print(f"\n{'=' * 60}")
    print("Cropping ...")
    for video_path in sorted(p for folder in folders for p in folder.rglob("*.mp4")):
        stem = video_path.stem
        if crop_cuts and stem not in crop_cuts:
            continue
        rect = crop_cuts.get(stem)
        crop_path  = video_path.with_name(stem + "_cropped.mp4")
        if rect is not None:
            out, _ = vu.crop_video(video_path, crop_path, *rect)
        else:
            out, _ = vu.crop_video(video_path, crop_path)
        if out is None:
            print(f"  Crop aborted for {video_path.name} — skipping.")
            continue
        print(f"  Cropped → {video_path.name}")
    print("Done.")