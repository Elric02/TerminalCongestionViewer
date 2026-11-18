"""
Utilities for processing video files
"""

import cv2
import json
import subprocess
import sys
import threading
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import Counter
from collections.abc import Callable
import numpy as np
from pathlib import Path

# Video reading --------------------------------------------------------------------------------------------------------

def open_video(path):
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        raise FileNotFoundError(f"Cannot open: {path}")
    fps = cap.get(cv2.CAP_PROP_FPS)
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    n_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    return cap, fps, w, h, n_frames


def write_rec_txt(path, suffix, rec):
    """Write a get_timecode_start / get_creation_time rec dict as key/value lines next to the video."""
    txt_path = path.with_name(path.stem + suffix)
    txt_path.write_text("\n".join(f"{k:<18} {val}" for k, val in rec.items()), encoding="utf-8")


def get_creation_time(path, tz="Europe/Stockholm", offset=None, write_txt=False,
                      ffprobe_path="ffprobe", exiftool_path="exiftool"):
    """
    Frame-0 timestamp of a clip from its true capture time, not a timecode. Prefers ExifTool's
    DateTimeOriginal, which survives cloud re-encoding/re-muxing that regenerates the container's
    own creation-time fields (mvhd/tkhd/mdhd) with the export time instead of the capture time.
    Falls back to the container creation-time tag via ffprobe (com.apple.quicktime.creationdate,
    then creation_time) when DateTimeOriginal is unavailable or ExifTool isn't installed.

    :param path          [str or Path]  Video file path.
    :param tz            [str]          IANA zone used when the source timestamp has no offset.
    :param offset        [timedelta]    Added to frame 0 before local/UTC are computed.
    :param write_txt     [bool]         Write <stem>_creationtime.txt next to the video. Default False.
    :param ffprobe_path  [str]          Name or full path of the ffprobe executable.
    :param exiftool_path [str]          Name or full path of the exiftool executable.

    :return  [dict]  file, fps_ratio ('30000/1001'), fps [float], nb_frames [int or None],
        creation_time (raw source value, for reference), offset (str or None),
        frame0_local and frame0_utc (ISO strings; None if no usable timestamp was found).
    """
    path = Path(path)
    cmd = [ffprobe_path, "-v", "quiet", "-print_format", "json",
           "-show_format", "-show_streams", str(path)]
    data = json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout)

    fmt_tags = data.get("format", {}).get("tags", {})
    streams  = data.get("streams", [])
    vstream  = next(s for s in streams if s.get("codec_type") == "video")
    stream_tags = {}
    for stream in streams:
        stream_tags.update(stream.get("tags", {}))

    fps_ratio = vstream["r_frame_rate"]
    num, den  = (int(x) for x in fps_ratio.split("/"))
    nb_frames = vstream.get("nb_frames", "")

    exif_raw = None
    try:
        r = subprocess.run([exiftool_path, "-j", "-DateTimeOriginal", str(path)],
                           capture_output=True, text=True, check=True)
        exif_raw = json.loads(r.stdout)[0].get("DateTimeOriginal")
    except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError, IndexError):
        pass

    raw = exif_raw or (
        fmt_tags.get("com.apple.quicktime.creationdate")
        or stream_tags.get("com.apple.quicktime.creationdate")
        or fmt_tags.get("creation_time")
        or stream_tags.get("creation_time")
    )

    rec = {
        "file":          path.name,
        "fps_ratio":     fps_ratio,
        "fps":           round(num / den, 3),
        "nb_frames":     int(nb_frames) if nb_frames.isdigit() else None,
        "timecode":      raw,
        "offset":        f"{offset.total_seconds():+g}s" if offset else None,
        "frame0_local":  None,
        "frame0_utc":    None,
    }

    def finish():
        if write_txt:
            write_rec_txt(path, "_timecode.txt", rec)
        return rec

    if raw is None:
        print(f"  WARNING: no creation-time tag in {path.name}")
        return finish()
    assert isinstance(raw, str)

    if exif_raw:
        # ExifTool "YYYY:MM:DD HH:MM:SS[.ffffff][+-]HH:MM" -> ISO 8601
        m = re.match(r"(\d{4}):(\d{2}):(\d{2}) (\d{2}:\d{2}:\d{2}(?:\.\d+)?)([+-]\d{2}:\d{2})?", raw)
        start = datetime.fromisoformat(f"{m[1]}-{m[2]}-{m[3]}T{m[4]}{m[5] or ''}")
        if start.tzinfo is None:
            start = start.replace(tzinfo=ZoneInfo(tz))
    else:
        # Normalize to valid ISO 8601: "Z" → "+00:00", "+0200" → "+02:00"
        iso = raw.replace("Z", "+00:00")
        iso = re.sub(r"([+-])(\d{2})(\d{2})$", r"\1\2:\3", iso)
        start = datetime.fromisoformat(iso)

    if offset:
        start += offset

    rec["frame0_local"] = start.astimezone(ZoneInfo(tz)).isoformat()
    rec["frame0_utc"]   = start.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")

    return finish()


def get_timecode_start(path, tz="Europe/Stockholm", offset=None, write_txt=False, ffprobe_path="ffprobe"):
    """
    Frame-0 timestamp of a clip from its SMPTE timecode track (the camera's local wall clock) rather than container creation_time.

    :param path          [str or Path]  Video file path.
    :param tz            [str]          IANA zone the camera clock is set to.
    :param offset        [timedelta]    Added to frame 0 before local/UTC are computed.
    :param write_txt     [bool]         Write <stem>_timecode.txt next to the video. Default False.
    :param ffprobe_path  [str]          Name or full path of the ffprobe executable.

    :return  [dict]  file, timecode ('HH:MM:SS;FF' verbatim or None),
        timecode_corrected (same, after offset; equals timecode when offset is None),
        fps_ratio ('30000/1001'), fps [float], nb_frames [int or None],
        creation_time (container value, for reference), offset (str or None),
        frame0_local and frame0_utc (ISO strings; None if the timecode or a date
        could not be read).
    """
    path = Path(path)
    cmd = [ffprobe_path, "-v", "quiet", "-print_format", "json",
           "-show_format", "-show_streams", str(path)]
    data = json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout)

    fmt_tags = data.get("format", {}).get("tags", {})
    streams  = data.get("streams", [])
    vstream  = next(s for s in streams if s.get("codec_type") == "video")

    timecode = fmt_tags.get("timecode") or next(
        (s["tags"]["timecode"] for s in streams if s.get("tags", {}).get("timecode")), None)
    creation  = fmt_tags.get("creation_time")
    fps_ratio = vstream["r_frame_rate"]
    num, den  = (int(x) for x in fps_ratio.split("/"))
    nb_frames = vstream.get("nb_frames", "")

    rec = {
        "file":          path.name,
        "timecode":      timecode,
        "timecode_corrected": timecode,
        "fps_ratio":     fps_ratio,
        "fps":           round(num / den, 3),
        "nb_frames":     int(nb_frames) if nb_frames.isdigit() else None,
        "creation_time": creation,
        "offset":        f"{offset.total_seconds():+g}s" if offset else None,
        "frame0_local":  None,
        "frame0_utc":    None,
    }

    def finish():
        if write_txt:
            write_rec_txt(path, "_timecode.txt", rec)
        return rec

    if not timecode:
        print(f"  WARNING: no timecode track in {path.name}")
        return finish()
    assert isinstance(timecode, str)

    tc = re.match(r"(\d{2}):(\d{2}):(\d{2})([:;])(\d+)", timecode)
    h, mn, s = int(tc[1]), int(tc[2]), int(tc[3])
    sep, ff  = tc[4], int(tc[5])
    dm = re.search(r"(\d{4})(\d{2})(\d{2})", path.stem) or re.match(r"(\d{4})-(\d{2})-(\d{2})", creation or "")
    if not dm:
        print(f"  WARNING: no date in filename or creation_time for {path.name}")
        return finish()
    y, mo, d = map(int, dm.groups())

    local = datetime(y, mo, d, h, mn, s, tzinfo=ZoneInfo(tz)) + timedelta(seconds=ff * den / num)
    if offset:
        local += offset
    ff_corr = round(local.microsecond / 1e6 * num / den)
    rec["timecode_corrected"] = f"{local:%H:%M:%S}{sep}{ff_corr:02d}"
    rec["frame0_local"] = local.isoformat()
    rec["frame0_utc"]   = local.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")

    return finish()


def get_osmo_metadata(path, exiftool_path="exiftool", write_txt=True):
    """
    Read camera settings from a video file using ExifTool.

    Two ExifTool calls are made:
    1. Fast JSON call (no -ee) for static fields: make, model, serial, fps, resolution.
    2. -ee with -p print format for per-frame fields: one tab-separated line per frame is output.

    Tag names are tuned for DJI Osmo Action cameras (djmd Protobuf track):
    - camera_make   from QuickTime Encoder ("DJI OsmoAction6")
    - white_balance from Protobuf ColorTemperature (Kelvin)
    - aperture/lens_type are None (fixed-lens camera, not stored)

    :param path            [str or Path]  Video file path.
    :param exiftool_path   [str]          Name or full path of the exiftool
                                          executable. Defaults to "exiftool".
    :param write_txt       [bool]         Write results to a .txt file next
                                          to the video. Default True.

    :return  [dict]  Keys:
        camera_make     [str]   manufacturer / encoder string or None
        fps             [float] frame rate or None
        width           [int]   frame width in pixels or None
        height          [int]   frame height in pixels or None
        shutter_speed   [dict]  {mode, mean, std, min, max} all as "1/N" strings or None
        iso             [dict]  {mode [str], mean, std, min, max [int]} or None
        white_balance   [dict]  {mode [str], mean, std, min, max [int, Kelvin]} or None
    """
    path = Path(path)
    not_found_msg = (
        f"ExifTool not found at {exiftool_path!r}. "
        "Download from https://exiftool.org, extract, rename to exiftool.exe, "
        "and add it to PATH or pass its full path via exiftool_path."
    )

    # --- static fields: fast JSON without -ee ---
    cmd_static = [
        exiftool_path, "-json",
        "-Make", "-Encoder",
        "-VideoFrameRate", "-ImageWidth", "-ImageHeight",
        str(path),
    ]
    try:
        r = subprocess.run(cmd_static, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        raise FileNotFoundError(not_found_msg)

    doc = json.loads(r.stdout)[0]
    static_tag_map = {
        "camera_make": ["Make", "Encoder"],
        "fps":         ["VideoFrameRate"],
        "width":       ["ImageWidth"],
        "height":      ["ImageHeight"],
    }
    static_fields = {
        key: next((doc[t] for t in tags if t in doc), None)
        for key, tags in static_tag_map.items()
    }

    # --- per-frame fields: -ee with -p format (one line per frame) ---
    # ExifTool exit code 255 means warnings only (e.g. filename encoding) — valid output.
    p_fmt = "$ShutterSpeed\t$ISO\t$ColorTemperature"
    cmd_frames = [exiftool_path, "-ee", "-p", p_fmt, str(path)]
    r = subprocess.run(cmd_frames, capture_output=True, text=True)

    def shutter_speed_to_seconds(value):
        s = str(value).strip()
        if "/" in s:
            parts = s.split("/", 1)
            try:
                return float(parts[0]) / float(parts[1])
            except (ValueError, ZeroDivisionError):
                return None
        try:
            return float(s)
        except ValueError:
            return None

    ss_strs, ss_nums = [], []
    iso_strs, iso_nums = [], []
    ct_strs, ct_nums = [], []

    for line in r.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        ss, iso, ct = [p.strip() for p in parts]
        if ss:
            ss_strs.append(ss)
            v = shutter_speed_to_seconds(ss)
            if v is not None:
                ss_nums.append(v)
        if iso:
            iso_strs.append(iso)
            try:
                iso_nums.append(float(iso))
            except ValueError:
                pass
        if ct:
            ct_strs.append(ct)
            try:
                ct_nums.append(float(ct))
            except ValueError:
                pass

    def secs_to_ss(secs):
        return f"1/{round(1 / secs)}" if secs and secs > 0 else None

    def distrib(str_vals, num_vals, fmt: Callable = round):
        if not str_vals:
            return None
        mode = Counter(str_vals).most_common(1)[0][0]
        if not num_vals:
            return {"mode": mode, "mean": None, "std": None, "min": None, "max": None}
        return {
            "mode": mode,
            "mean": fmt(float(np.mean(num_vals))),
            "std":  fmt(float(np.std(num_vals))),
            "min":  fmt(min(num_vals)),
            "max":  fmt(max(num_vals)),
        }

    meta = {
        "camera_make":   static_fields["camera_make"],
        "fps":           static_fields["fps"],
        "width":         static_fields["width"],
        "height":        static_fields["height"],
        "shutter_speed": distrib(ss_strs, ss_nums, fmt=secs_to_ss),
        "iso":           distrib(iso_strs, iso_nums),
        "white_balance": distrib(ct_strs, ct_nums),
    }

    if write_txt:
        lines = []
        for key, value in meta.items():
            if isinstance(value, dict):
                lines.append(key)
                for stat, v in value.items():
                    lines.append(f"  {stat:<6} {v}")
            else:
                lines.append(f"{key:<16} {value}")
        txt_path = path.with_name(path.stem + "_metadata.txt")
        txt_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"Saved metadata to {txt_path.name}")

    return meta


def browse_video(video_path, entries=None):
    """
    Interactively browse a video in an OpenCV window.

    entries is a list of positions to flag, each either:
      float or (float, str)  — time in seconds (converted to nearest frame)
      int   or (int,   str)  — frame index

    Keys:
      <- ->   : +-1 frame
      q  w    : +-1 second
      a  s    : +-10 seconds
      n  p    : next / previous flagged entry
      Esc     : quit

    :param video_path  [str or Path]                              Input video file.
    :param entries     [list[int|float|tuple[int|float, str]]]   Positions to flag.
    """
    cap, fps, w, h, n_frames = open_video(video_path)
    fps = fps if fps > 0 else 30.0

    flagged = []
    if entries:
        for entry in entries:
            if isinstance(entry, (list, tuple)) and len(entry) == 2:
                val, lbl = entry[0], str(entry[1])
            else:
                val, lbl = entry, ""
            assert isinstance(val, (int, float)), f"Invalid entry: {entry!r}"
            fi = int(val) if isinstance(val, int) else round(float(val) * fps)
            flagged.append((fi, lbl))
        flagged.sort(key=lambda x: x[0])

    cur_fi = flagged[0][0] if flagged else 0
    flag_pos = 0

    win = (f"Browse - {Path(video_path).name}  |  <- ->: +-1f   q w: +-1s   a s: +-10s" +
           ("   n p: next/prev flag" if flagged else "") + "   Esc: quit")
    cv2.namedWindow(win, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(win, min(w, 1600), min(h, 900))

    while True:
        cur_fi = max(0, min(cur_fi, n_frames - 1))
        cap.set(cv2.CAP_PROP_POS_FRAMES, cur_fi)
        ret, frame = cap.read()
        if not ret:
            frame = np.zeros((h, w, 3), dtype=np.uint8)
        disp = frame.copy()

        cur_ms = cur_fi * 1000.0 / fps
        cv2.putText(disp, f"Frame {cur_fi}  t={cur_ms/1000:.3f}s",
                    (20, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.1, (0, 255, 0), 2)

        if flagged:
            flag_fi, flag_lbl = flagged[flag_pos]
            color = (0, 80, 255) if cur_fi == flag_fi else (120, 120, 120)
            cv2.putText(disp, f"[{flag_pos + 1}/{len(flagged)}]  {flag_lbl}",
                        (20, 90), cv2.FONT_HERSHEY_SIMPLEX, 1.0, color, 2)

        cv2.imshow(win, disp)
        k = cv2.waitKeyEx(0)

        if   k == 27:           break
        elif k == 2424832:      cur_fi -= 1
        elif k == 2555904:      cur_fi += 1
        elif k == ord('q'):     cur_fi -= round(fps)
        elif k == ord('w'):     cur_fi += round(fps)
        elif k == ord('a'):     cur_fi -= round(10 * fps)
        elif k == ord('s'):     cur_fi += round(10 * fps)
        elif k == ord('n') and flagged:
            flag_pos = min(flag_pos + 1, len(flagged) - 1)
            cur_fi = flagged[flag_pos][0]
        elif k == ord('p') and flagged:
            flag_pos = max(flag_pos - 1, 0)
            cur_fi = flagged[flag_pos][0]

    cap.release()
    cv2.destroyWindow(win)


def get_frame(input_video, output_frame, frame_index):
    """
    Extract a single frame from a video and save it as an image.

    :param input_video    [str or Path]  Input video file.
    :param output_frame   [str or Path]  Destination image file (.png).
    :param frame_index    [int]          Frame number to extract (0 = first frame).

    :return  [Path]  Path to the saved image.
    """
    cap, _, _, _, n_frames = open_video(input_video)
    if frame_index >= n_frames:
        raise ValueError(f"frame_index {frame_index} >= total frames {n_frames}")

    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
    ret, frame = cap.read()
    cap.release()

    if not ret:
        raise RuntimeError(f"Failed to read frame {frame_index} from {input_video}")

    output_frame = Path(output_frame)
    output_frame.parent.mkdir(parents=True, exist_ok=True)
    ext = output_frame.suffix.lower()
    success, buf = cv2.imencode(ext, frame)
    if not success:
        raise RuntimeError(f"Failed to encode frame as {ext}")
    buf.tofile(str(output_frame))
    print(f"Saved frame {frame_index} to {output_frame}")

    return output_frame


def get_frames(input_video, output_frame, frames_per_second=1):
    """
    Extract frames from a video at a regular interval and save them as images.

    :param input_video          [str or Path]  Input video file.
    :param output_frame         [str or Path]  Directory where extracted frames are saved (created if needed).
    :param frames_per_second    [float]        How many frames to extract per second of video.

    :return  [int]  Number of frames saved.
    """
    cap, fps, _, _, n_frames = open_video(input_video)
    output_frame = Path(output_frame)
    output_frame.mkdir(parents=True, exist_ok=True)
    step = max(1, int(round(fps / frames_per_second)))
    saved = 0
    for i in range(n_frames):
        ret, frame = cap.read()
        if not ret:
            break
        if i % step == 0:
            cv2.imwrite(str(output_frame / f"frame_{i:06d}.png"), frame)
            saved += 1

    cap.release()
    print(f"Extracted {saved} frames to {output_frame}")

    return saved


def get_frame_count(video_path, ffprobe_path="ffprobe") -> int:
    """
    Count a video's actual frame count via ffprobe, counting packets rather than decoding.
     One packet = one frame for a single video stream

    :param video_path    [str or Path]  Video file to count.
    :param ffprobe_path  [str]          Name or full path of the ffprobe executable.

    :return  [int]  Actual number of video frames.
    """
    cmd = [ffprobe_path, "-v", "error", "-select_streams", "v:0",
          "-count_packets", "-show_entries", "stream=nb_read_packets",
          "-of", "csv=p=0", str(video_path)]
    r = subprocess.run(cmd, capture_output=True, text=True, check=True)

    return int(r.stdout.strip().rstrip(","))


def generate_timestamps(video_path, start, fps_ratio, n_frames=None, out_path=None, ffprobe_path="ffprobe"):
    """
    Write a frame→timestamp CSV ('frame,timestamp', UTC 'YYYY-MM-DD HH:MM:SS.ffffff')
    by advancing a fixed start time at the video's frame rate.

    :param video_path  [str or Path]      Video the timestamps describe.
    :param start       [str or datetime]  Frame-0 time; ISO string (may end 'Z') or datetime.
    :param fps_ratio   [str]              Frame rate as 'num/den' (ffprobe r_frame_rate).
    :param n_frames    [int, optional]    Row count; counted via ffprobe if omitted.
    :param out_path    [str or Path, optional]  Defaults to <video stem>_timestamps.csv.

    :return  [int]  Number of frame rows written.
    """
    video_path = Path(video_path)
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace("Z", "+00:00"))
    if start.tzinfo is not None:
        start = start.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    num, den = (int(x) for x in fps_ratio.split("/"))
    n = n_frames or get_frame_count(video_path, ffprobe_path)
    out_path = Path(out_path) if out_path else video_path.with_name(video_path.stem + "_timestamps.csv")

    with out_path.open("w", encoding="utf-8", newline="") as f:
        f.write("frame,timestamp\n")
        for i in range(n):
            ts = start + timedelta(seconds=i * den / num)
            f.write(f"{i},{ts:%Y-%m-%d %H:%M:%S.%f}\n")
    print(f"  {n} frames -> {out_path.name}")

    return n


# Overall checks -------------------------------------------------------------------------------------------------------

def check_srt(video_path, srt_path) -> list:
    """
    Validate a DJI SRT telemetry file against its corresponding video.

    Checks duration match, entry count, timestamp monotonicity, inter-frame gaps, and per-entry duration.
     Returns an empty list if the SRT is clean.

    :param video_path  [str or Path]  Video file to validate against.
    :param srt_path    [str or Path]  DJI SRT subtitle/telemetry file.

    :return  [list of str]  Issue descriptions; empty list means no problems found.
    """
    ts_re = re.compile(r'(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})')

    def parse_time(s):
        return timedelta(hours=int(s[0:2]), minutes=int(s[3:5]),
                         seconds=int(s[6:8]), milliseconds=int(s[9:12]))

    srt_path = Path(srt_path)
    text = srt_path.read_text(encoding='utf-8', errors='replace')
    entries = [{'start': parse_time(m.group(1)), 'end': parse_time(m.group(2))}
               for m in ts_re.finditer(text)]
    if not entries:
        return [f"Could not parse any entries from {srt_path.name}"]

    cap, fps, _, _, _ = open_video(video_path)
    cap.release()
    n_frames = get_frame_count(video_path)
    frame_s = 1.0 / fps

    issues = []
    srt_dur_s = entries[-1]['end'].total_seconds()
    video_dur_s = n_frames / fps
    diff_s = abs(srt_dur_s - video_dur_s)
    if diff_s > 5 * frame_s:
        issues.append(
            f"Duration mismatch: SRT={srt_dur_s:.3f}s  video={video_dur_s:.3f}s  "
            f"(Δ={diff_s:.3f}s, threshold={5*frame_s:.3f}s)"
        )

    count_diff = n_frames - len(entries)
    if count_diff != 0:
        issues.append(f"Entry count: {len(entries)} SRT entries vs {n_frames} video frames (diff={count_diff})")

    non_mono, large_gaps, bad_dur = [], [], []
    for i in range(1, len(entries)):
        prev, curr = entries[i - 1], entries[i]
        if curr['start'] < prev['end']:
            non_mono.append(i)
        gap_s = (curr['start'] - prev['end']).total_seconds()
        if abs(gap_s) > frame_s:
            large_gaps.append((i, gap_s))
        dur_s = (curr['end'] - curr['start']).total_seconds()
        if abs(dur_s - frame_s) > frame_s:
            bad_dur.append((i, dur_s))

    if non_mono:
        issues.append(f"Non-monotonic timestamps at {len(non_mono)} entries (first at: {non_mono[:3]}{'...' if len(non_mono) > 3 else ''})")
    if large_gaps:
        issues.append(f"Gaps >1 frame at {len(large_gaps)} pairs (examples: {[(i, f'{g:+.3f}s') for i, g in large_gaps[:3]]}{'...' if len(large_gaps) > 3 else ''})")
    if bad_dur:
        issues.append(f"Entry duration off by >1 frame at {len(bad_dur)} entries (expected ~{frame_s:.3f}s, examples: {[(i, f'{d:.3f}s') for i, d in bad_dur[:3]]}{'...' if len(bad_dur) > 3 else ''})")

    return issues


def check_csv(video_path, csv_path) -> list:
    """
    Validate a frame->timestamp CSV (as saved by srt_to_csv/trim_csv) against its corresponding video.

    Checks duration match, row count, and monotonicity/gaps of the 'timestamp' column (the DJI-logged
    absolute UTC time per frame) — an independent measurement from the SRT file.

    :param video_path  [str or Path]  Video file to validate against.
    :param csv_path    [str or Path]  CSV file with a 'timestamp' column (format 'YYYY-MM-DD HH:MM:SS.ffffff').

    :return  [list of str]  Issue descriptions; empty list means no problems found.
    """
    csv_path = Path(csv_path)
    lines = csv_path.read_text(encoding='utf-8').splitlines()
    if len(lines) < 2:
        return [f"No data rows in {csv_path.name}"]
    header, data = lines[0], lines[1:]
    ts_idx = header.split(',').index('timestamp')
    timestamps = [datetime.fromisoformat(line.split(',')[ts_idx]) for line in data]

    cap, fps, _, _, _ = open_video(video_path)
    cap.release()
    n_frames = get_frame_count(video_path)
    frame_s = 1.0 / fps

    issues = []
    csv_dur_s = (timestamps[-1] - timestamps[0]).total_seconds()
    video_dur_s = (n_frames - 1) / fps   # start-of-first-frame to start-of-last-frame, same span as csv_dur_s
    diff_s = abs(csv_dur_s - video_dur_s)
    if diff_s > 5 * frame_s:
        issues.append(
            f"Duration mismatch: CSV={csv_dur_s:.3f}s  video={video_dur_s:.3f}s  "
            f"(Δ={diff_s:.3f}s, threshold={5*frame_s:.3f}s)"
        )

    count_diff = n_frames - len(timestamps)
    if count_diff != 0:
        issues.append(f"Row count: {len(timestamps)} CSV rows vs {n_frames} video frames (diff={count_diff})")

    non_mono, large_gaps = [], []
    for i in range(1, len(timestamps)):
        gap_s = (timestamps[i] - timestamps[i - 1]).total_seconds()
        if gap_s < 0:
            non_mono.append(i)
        elif abs(gap_s - frame_s) > frame_s:
            large_gaps.append((i, gap_s))

    if non_mono:
        issues.append(f"Non-monotonic timestamps at {len(non_mono)} rows (first at: {non_mono[:3]}{'...' if len(non_mono) > 3 else ''})")
    if large_gaps:
        issues.append(f"Gaps >1 frame at {len(large_gaps)} rows (examples: {[(i, f'{g:+.3f}s') for i, g in large_gaps[:3]]}{'...' if len(large_gaps) > 3 else ''})")

    return issues


def check_frame_counts(video_path, srt_path=None, csv_path=None):
    """
    Sanity-check that a video's frame count matches its companion SRT/CSV entry counts.

    :param video_path  [str or Path]            Video file.
    :param srt_path    [str or Path, optional]  SRT file to check against the video. Skipped if None.
    :param csv_path    [str or Path, optional]  CSV file to check against the video. Skipped if None.

    :return  [list of str]  Issue descriptions; empty list means all counts match.
    """
    n_frames = get_frame_count(video_path)

    counts = {}
    if srt_path is not None and Path(srt_path).exists():
        text = Path(srt_path).read_text(encoding='utf-8', errors='replace')
        counts["srt"] = len([b for b in re.split(r'\n\s*\n', text.strip()) if b.strip()])
    if csv_path is not None and Path(csv_path).exists():
        counts["csv"] = max(0, len(Path(csv_path).read_text(encoding='utf-8').splitlines()) - 1)

    issues = [f"{name} has {count} frames, video has {n_frames} (diff={n_frames - count})"
             for name, count in counts.items() if count != n_frames]
    if issues:
        print(f"  WARNING: frame count mismatch for {Path(video_path).name} "
              f"(video={n_frames}, {counts}):")
        for issue in issues:
            print(f"    - {issue}")

    return issues


def check_video_timing(video_path, ffprobe_path="ffprobe", jump_tolerance=0.5):
    """
    Validate a video's own internal frame timing: every encoded frame's timestamp should advance
    by one constant-frame-rate interval, with no dropped or duplicated frames anywhere inside it.

    :param video_path      [str or Path]  Video file to check.
    :param ffprobe_path    [str]          Name or full path of the ffprobe executable.
    :param jump_tolerance  [float]        Fraction of one frame interval allowed before an inter-frame
                                          gap is flagged (e.g. 0.5 = must be within +/-50% of 1/fps).

    :return  [list of str]  Issue descriptions; empty list means timing is clean.
    """
    video_path = Path(video_path)
    cmd = [ffprobe_path, "-v", "error", "-select_streams", "v:0",
          "-show_entries", "frame=best_effort_timestamp_time",
          "-of", "csv=p=0", str(video_path)]
    r = subprocess.run(cmd, capture_output=True, text=True, check=True)
    times = [float(line.split(',')[0]) for line in r.stdout.splitlines() if line.strip()]
    if not times:
        return [f"Could not read any frame timestamps from {video_path.name}"]

    cap, fps, _, _, n_frames = open_video(video_path)
    cap.release()
    frame_s = 1.0 / fps

    issues = []
    if len(times) != n_frames:
        issues.append(f"Decoded {len(times)} frames but container metadata reports {n_frames}")

    dupes, jumps = [], []
    for i in range(1, len(times)):
        gap = times[i] - times[i - 1]
        if gap <= 0:
            dupes.append((i, gap))
        elif abs(gap - frame_s) > jump_tolerance * frame_s:
            jumps.append((i, gap))

    if dupes:
        issues.append(f"Duplicate/backwards timestamps at {len(dupes)} frames "
                      f"(examples: {[(i, f'{g:.4f}s') for i, g in dupes[:3]]}{'...' if len(dupes) > 3 else ''})")
    if jumps:
        issues.append(f"Frame interval off by >{jump_tolerance:.0%} at {len(jumps)} frames "
                      f"(expected ~{frame_s:.4f}s, examples: {[(i, f'{g:.4f}s') for i, g in jumps[:3]]}{'...' if len(jumps) > 3 else ''})")

    total_span = times[-1] - times[0] + frame_s
    expected_dur = n_frames / fps
    if abs(total_span - expected_dur) > frame_s:
        issues.append(f"Total span {total_span:.3f}s vs {expected_dur:.3f}s expected from frame count/fps "
                      f"(Δ={total_span - expected_dur:+.3f}s)")

    if issues:
        print(f"  WARNING: internal timing issues in {video_path.name}:")
        for issue in issues:
            print(f"    - {issue}")

    return issues


# SRT files (DJI-specific) ---------------------------------------------------------------------------------------------

def pad_srt(srt_path, video_path, ffprobe_path="ffprobe") -> int:
    """
    Pad an SRT file in place with placeholder entries to meet actual frame count.
    DJI's video encoder can flush a trailing frame or two after its own telemetry logger has already stopped ticking.

    :param srt_path      [str or Path]  SRT file to pad (rewritten in place if a deficit is found).
    :param video_path    [str or Path]  Video file to measure ground truth against.
    :param ffprobe_path  [str]          Name or full path of the ffprobe executable.

    :return  [int]  Number of placeholder entries appended (0 if the file already matched).
    """
    time_re = re.compile(r"(\d{2}):(\d{2}):(\d{2}),(\d{3})")
    utc_re = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+')

    def parse_time(s):
        h, m, sec, ms = map(int, time_re.match(s.strip()).groups())
        return timedelta(hours=h, minutes=m, seconds=sec, milliseconds=ms)

    def fmt_time(td):
        total_ms = int(td.total_seconds() * 1000)
        h, rem = divmod(total_ms, 3_600_000)
        m, rem = divmod(rem, 60_000)
        s, ms  = divmod(rem, 1_000)
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

    def parse_utc(content):
        m = utc_re.search(content)
        return datetime.strptime(m.group(), '%Y-%m-%d %H:%M:%S.%f') if m else None

    srt_path = Path(srt_path)
    blocks = [b.strip() for b in re.split(r'\n\s*\n', srt_path.read_text(encoding='utf-8', errors='replace').strip()) if b.strip()]

    entries = []
    for block in blocks:
        lines = block.splitlines()
        for idx, line in enumerate(lines):
            if '-->' not in line:
                continue
            start, end = line.split('-->')
            entries.append({'start': parse_time(start), 'end': parse_time(end), 'content': '\n'.join(lines[idx + 1:])})
            break

    if not entries:
        return 0

    n_frames = get_frame_count(video_path, ffprobe_path)
    deficit = n_frames - len(entries)
    if deficit < 1:
        return 0

    durations = sorted((e['end'] - e['start']).total_seconds() for e in entries)
    nominal_dt = durations[len(durations) // 2]   # median entry duration for this clip
    step = timedelta(seconds=nominal_dt)
    offset = entries[-1]['end']
    last_utc = parse_utc(entries[-1]['content'])

    print(f"    {srt_path.name}: video [{n_frames} frames] vs SRT [{len(entries)} frames] "
          f"— appending {deficit} placeholder entry/entries")
    for k in range(1, deficit + 1):
        entries.append({
            'start': offset, 'end': offset + step,
            'content': (last_utc + k * step).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if last_utc else '',
        })
        offset += step

    out_lines = []
    for i, e in enumerate(entries, 1):
        out_lines += [str(i), f"{fmt_time(e['start'])} --> {fmt_time(e['end'])}", e['content'], '']
    srt_path.write_text('\n'.join(out_lines), encoding='utf-8')

    return deficit


def srt_to_csv(srt_path, csv_path) -> int:
    """
    Extract UTC timestamps and all telemetry fields from a DJI SRT file and write them to a CSV.

    :param srt_path  [str or Path]  DJI SRT file to read.
    :param csv_path  [str or Path]  Destination CSV file.

    :return  [int]  Number of frames written; 0 if no timestamps were found.
    """
    utc_re = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+')
    bracket_re = re.compile(r'\[([^]]+)]')
    field_re = re.compile(r'(\w+)\s*:\s*([^\s,\]]+)')

    text = Path(srt_path).read_text(encoding='utf-8', errors='replace')
    blocks = [b.strip() for b in re.split(r'\n\s*\n', text.strip()) if b.strip()]

    rows = []
    field_names = []
    field_set = set()

    for block in blocks:
        timestamp = None
        for line in block.splitlines():
            m = utc_re.search(line)
            if m:
                timestamp = m.group()
                break
        if timestamp is None:
            continue

        fields = {}
        for bracket in bracket_re.finditer(block):
            for field in field_re.finditer(bracket.group(1)):
                name, value = field.group(1), field.group(2)
                fields[name] = value
                if name not in field_set:
                    field_set.add(name)
                    field_names.append(name)

        rows.append((timestamp, fields))

    if not rows:
        print(f"  WARNING: no UTC timestamps found in {Path(srt_path).name}")
        return 0

    csv_path = Path(csv_path)
    with csv_path.open('w', encoding='utf-8', newline='') as f:
        f.write('frame,timestamp,' + ','.join(field_names) + '\n')
        for i, (ts, fields) in enumerate(rows):
            f.write(f'{i},{ts},' + ','.join(fields.get(n, '') for n in field_names) + '\n')

    return len(rows)


# Video editing --------------------------------------------------------------------------------------------------------

def join_video(input_videos, output_video, ffmpeg_path="ffmpeg"):
    """
    Concatenate a list of video clips into one continuous video with consistent timestamps.
     All input clips must share the same resolution and frame rate.

    :param input_videos  [list of str or Path]  Ordered list of video clip paths to concatenate.
    :param output_video  [str or Path]          Output video file path.
    :param ffmpeg_path   [str]                  Name or full path of the ffmpeg executable.

    :return  [Path]  Path to the concatenated output video.
    """
    if not input_videos:
        raise ValueError("input_videos is empty")

    input_videos = [Path(p) for p in input_videos]
    output_video = Path(output_video)

    for p in input_videos:
        if not p.exists():
            raise FileNotFoundError(f"Cannot find: {p}")

    props = []
    for p in input_videos:
        cap, fps, w, h, n_frames = open_video(p)
        cap.release()
        props.append((fps, w, h, n_frames))

    ref_fps, ref_w, ref_h, _ = props[0]
    for i, (fps, w, h, _) in enumerate(props[1:], start=1):
        if abs(fps - ref_fps) > 0.01:
            raise ValueError(
                f"FPS mismatch: clip 0 is {ref_fps:.3f} fps, clip {i} is {fps:.3f} fps")
        if w != ref_w or h != ref_h:
            raise ValueError(
                f"Resolution mismatch: clip 0 is {ref_w}x{ref_h}, clip {i} is {w}x{h}")

    total_frames = sum(n for _, _, _, n in props)
    print(f"Joining {len(input_videos)} clips → {total_frames} frames total "
          f"({ref_w}x{ref_h} @ {ref_fps:.2f} fps)")
    for i, (p, (_, _, _, n)) in enumerate(zip(input_videos, props)):
        print(f"  [{i}] {p.name}  ({n} frames)")

    list_path = output_video.with_suffix(".concat_list.txt")
    try:
        list_path.write_text(
            "".join(f"file '{p.resolve().as_posix()}'\n" for p in input_videos),
            encoding="utf-8",
        )
        output_video.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            ffmpeg_path, "-y",
            "-f", "concat", "-safe", "0",
            "-i", str(list_path),
            "-c", "copy",
            "-progress", "pipe:1",
            "-nostats",
            str(output_video),
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        assert proc.stderr is not None
        assert proc.stdout is not None
        stderr_stream = proc.stderr
        stdout_stream = proc.stdout
        stderr_buf = []
        def _read_stderr():
            for line in stderr_stream:
                stderr_buf.append(line)
        threading.Thread(target=_read_stderr, daemon=True).start()

        frame = 0
        for line in stdout_stream:
            if line.startswith("frame="):
                try:
                    frame = int(line.split("=", 1)[1].strip())
                except ValueError:
                    pass
                pct = frame / total_frames * 100 if total_frames else 0
                print(f"\r  {frame}/{total_frames} frames  ({pct:.1f}%)", end="", flush=True)

        proc.wait()
        print()
        if proc.returncode != 0:
            raise RuntimeError(
                f"FFmpeg failed (exit {proc.returncode}):\n{''.join(stderr_buf)}")
    finally:
        if list_path.exists():
            list_path.unlink()

    print(f"Saved joined video to {output_video}")
    return output_video


def trim_video(input_video, output_video, start=0, duration=None, start_frame=None,
               end_frame=None, inspect=False, ffmpeg_path="ffmpeg", crf=18):
    """
    Extract a clip from a video with frame-accurate cuts using FFmpeg H.264 encoding.

    Start and end can each be specified as seconds or frame index. The end point can
    also be picked interactively via the built-in frame inspector (inspect=True).

    :param input_video   [str or Path]  Input video file.
    :param output_video  [str or Path]  Output clip file.
    :param start         [float]        Start position in seconds. Default 0.
    :param start_frame   [int|None]     Start frame index. Overrides start if provided.
    :param duration      [float|None]   Length of the clip in seconds. Required if end_frame and inspect are not used.
    :param end_frame     [int|None]     End frame index (exclusive). Alternative to duration.
    :param inspect       [bool]         If True, open a two-phase interactive frame inspector:
                                        Phase 1 picks the start frame, Phase 2 picks the end frame.
                                        Any start_frame/end_frame values are used as the initial position only.
    :param ffmpeg_path   [str]          Name or full path of the ffmpeg executable.
    :param crf           [int]          H.264 quality (0=lossless, 51=worst). 18 is visually lossless.

    :return  [(Path, int)]   Tuple of (output path, confirmed end frame) on success.
             [(None, None)]  If the inspector was aborted with Q.
    """
    cap, fps, _, _, _ = open_video(input_video)
    cap.release()
    n_frames = get_frame_count(input_video)

    def inspector(initial_frame, phase, total_phases, start_ref=None):
        cap = cv2.VideoCapture(str(input_video))
        fi = max(0, min(initial_frame, n_frames - 1))
        is_start_phase = phase == 1 and total_phases == 2
        frame_label = "first kept frame" if is_start_phase else "last kept frame"
        action = ("Navigate to the FIRST frame you want to keep"
                  if is_start_phase else
                  "Navigate to the LAST frame you want to keep")
        phase_tag = f"[{phase}/{total_phases}] " if total_phases > 1 else ""
        print(f"  {phase_tag}{action}, then press Enter.")
        print("  ← →   : ±1")
        print("  Q  W  : ±10   A  S  : ±500   Z  X : ±1000   K  L : first/last frame")
        print("  Enter : confirm   Esc : abort")
        win_title = f"Inspector {phase_tag}— {action.lower()}  |  Enter: confirm   Esc: abort"
        while True:
            fi = max(0, min(fi, n_frames - 1))
            cap.set(cv2.CAP_PROP_POS_FRAMES, fi)
            ret, frame = cap.read()
            if not ret:
                break
            disp = cv2.resize(frame, (1280, 720))
            cv2.putText(disp, f"Frame {fi} / {n_frames - 1}  ({fi / fps:.1f}s)  [{frame_label}]",
                        (20, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 255, 0), 2)
            cv2.imshow(win_title, disp)
            k = cv2.waitKeyEx(0)
            if   k == 13:          break
            elif k == 27:          fi = -1; break
            elif k == 2424832:     fi -= 1
            elif k == 2555904:     fi += 1
            elif k == ord('q'):    fi -= 10
            elif k == ord('w'):    fi += 10
            elif k == ord('a'):    fi -= 500
            elif k == ord('s'):    fi += 500
            elif k == ord('z'):    fi -= 1000
            elif k == ord('x'):    fi += 1000
            elif k == ord('k'):    fi = 0
            elif k == ord('l'):    fi = n_frames - 1
        cap.release()
        cv2.destroyAllWindows()
        return None if fi == -1 else (fi if is_start_phase else fi + 1)

    if inspect:
        initial_start = start_frame if start_frame is not None else 0
        frame_start = inspector(initial_start, phase=1, total_phases=2)
        if frame_start is None:
            print("  Trim aborted.")
            return None, None
        start = frame_start / fps

        initial_end = (end_frame - 1) if end_frame is not None else frame_start
        end_frame = inspector(max(frame_start, initial_end), phase=2, total_phases=2,
                              start_ref=frame_start)
        if end_frame is None:
            print("  Trim aborted.")
            return None, None
    else:
        if start_frame is not None:
            start = start_frame / fps
        frame_start = int(round(start * fps))

        if end_frame is None and duration is None:
            end_frame = n_frames

    if end_frame is not None:
        duration = (end_frame - frame_start) / fps

    total_frames = int(round(duration * fps))
    cmd = [
        ffmpeg_path, "-y",
        "-ss", str(start),
        "-i", str(input_video),
        "-t", str(duration),
        "-c:v", "libx264", "-crf", str(crf),
        "-c:a", "copy",
        "-progress", "pipe:1",
        "-nostats",
        str(output_video),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    assert proc.stderr is not None
    assert proc.stdout is not None
    stderr_stream = proc.stderr
    stdout_stream = proc.stdout
    stderr_buf = []
    def _read_stderr():
        for line in stderr_stream:
            stderr_buf.append(line)
    threading.Thread(target=_read_stderr, daemon=True).start()

    frame = 0
    for line in stdout_stream:
        if line.startswith("frame="):
            try:
                frame = int(line.split("=", 1)[1].strip())
            except ValueError:
                pass
            pct = frame / total_frames * 100 if total_frames else 0
            print(f"\r  {frame}/{total_frames} frames  ({pct:.1f}%)", end="", flush=True)

    proc.wait()
    print()
    if proc.returncode != 0:
        raise RuntimeError(
            f"FFmpeg failed (exit {proc.returncode}):\n{''.join(stderr_buf)}")

    confirmed_end = end_frame if end_frame is not None else int(round((start + duration) * fps))
    print(f"Saved trimmed video to {output_video}  (frames {frame_start}–{confirmed_end - 1})")

    return Path(output_video), confirmed_end


def select_region(video_path, frame_index=0, mode='rect', review_frames=False, seed_regions=None):
    """
    Interactively select a region on a video frame.

    :param video_path     [str or Path]  Source video (provides the reference frame).
    :param frame_index    [int]          Initial frame to display. Default 0.
    :param mode           [str]          'rect' — drag a rectangle. With review_frames=False (the
                                         cv2.selectROI path), returns a single (x1, y1, x2, y2).
                                         With review_frames=True, a new drag starts a new rectangle
                                         (Right-click has no effect in rect mode); returns a list of
                                         (x1, y1, x2, y2) tuples, one per rectangle.
                                         'poly' — click polygon vertices; Right-click or N closes the
                                         current polygon and starts a new one. Returns a list of
                                         vertex-lists [[(x, y), ...], ...], one per polygon.
                                         All coordinates are in the original video resolution.
    :param review_frames  [bool]         If True, step through other frames of the video while
                                         drawing to check the region(s) hold up throughout (e.g. a
                                         drone shot drifting or panning), editing them as needed
                                         before confirming. Adds the frame keys below. Default False.
    :param seed_regions   [list|None]    Existing shapes to pre-load as already-confirmed, in the
                                         same format select_region returns for the given mode
                                         (forces review_frames-style multi-shape editing even for
                                         mode='rect'). Lets you reopen a previous selection and add
                                         to or trim it instead of starting from a blank frame.

    Frame keys (only active when review_frames=True):
      <- ->   : +-1 frame
      q  w    : +-10 frames
      a  s    : +-100 frames
      z  x    : +-1000 frames
      k  l    : first / last frame

    Editing keys (poly, or rect with review_frames=True or seed_regions):
      Backspace : undo — last vertex of the shape in progress, else the last confirmed shape
      C         : clear everything and start over
      Enter     : confirm the shape in progress (if valid) plus all previously confirmed shapes
    Dragging a vertex or corner (of the shape in progress, or any already-confirmed shape) moves
    it — a red ring shows which one is about to be grabbed.

    :return  [tuple|list|None]  Selection(s) in original-video coordinates, or None if aborted.
    """
    cap, fps, vid_w, vid_h, n_frames = open_video(video_path)
    fps = fps if fps > 0 else 30.0
    scale = min(1280 / vid_w, 720 / vid_h, 1.0)
    print(f"\nSelect region: {Path(video_path).name}  ({vid_w}x{vid_h})")

    if mode == 'rect' and not review_frames and not seed_regions:
        cap.set(cv2.CAP_PROP_POS_FRAMES, min(frame_index, n_frames - 1))
        ret, frame = cap.read()
        cap.release()
        if not ret:
            raise RuntimeError(f"Cannot read frame {frame_index} from {video_path}")
        disp = cv2.resize(frame, (int(vid_w * scale), int(vid_h * scale))) if scale < 1.0 else frame.copy()

        print("  Drag to draw rectangle   Space/Enter: confirm   C/Esc: abort")
        roi = cv2.selectROI("Select region — Space/Enter: confirm, C/Esc: abort",
                            disp, fromCenter=False, showCrosshair=False)
        cv2.destroyAllWindows()
        if roi == (0, 0, 0, 0):
            print("  Aborted.")
            return None
        rx, ry, rw, rh = roi
        x1 = int(rx / scale)
        y1 = int(ry / scale)
        x2 = int((rx + rw) / scale)
        y2 = int((ry + rh) / scale)
        print(f"  ({x1}, {y1}) → ({x2}, {y2})  [{x2-x1} x {y2-y1} px]")
        return x1, y1, x2, y2

    if mode not in ('rect', 'poly'):
        raise ValueError(f"mode must be 'rect' or 'poly'; got {mode!r}")

    cur_fi = [max(0, min(frame_index, n_frames - 1))]
    frame_cache: dict = {"fi": None, "img": None}

    def cur_img():
        if frame_cache["fi"] != cur_fi[0]:
            cap.set(cv2.CAP_PROP_POS_FRAMES, cur_fi[0])
            ret, frame = cap.read()
            if not ret:
                raise RuntimeError(f"Cannot read frame {cur_fi[0]} from {video_path}")
            frame_cache["img"] = (cv2.resize(frame, (int(vid_w * scale), int(vid_h * scale)))
                                  if scale < 1.0 else frame.copy())
            frame_cache["fi"] = cur_fi[0]
        return frame_cache["img"]

    # finalized shapes: poly → list of vertex-lists  |  rect → list of corner-pairs (display coords)
    if seed_regions:
        if mode == 'poly':
            regions = [[(int(x * scale), int(y * scale)) for x, y in poly] for poly in seed_regions]
        else:
            regions = [[(int(x1 * scale), int(y1 * scale)), (int(x2 * scale), int(y2 * scale))]
                      for x1, y1, x2, y2 in seed_regions]
    else:
        regions = []
    cur = []          # shape being drawn now: poly → vertices so far  |  rect → [] or [corner1, corner2]
    dragging = [False]
    drag_target: list = [None]   # None, or ("cur", idx), or ("region", ri, pi) — vertex currently being moved
    state = {"confirm": False, "abort": False}

    disp_w = int(vid_w * scale) if scale < 1.0 else vid_w
    disp_h = int(vid_h * scale) if scale < 1.0 else vid_h
    edge_snap = 12    # px (display coords) — clicks this close to a border snap onto it, corners too
    vertex_grab = 10  # px (display coords) — clicks this close to an existing vertex grab it instead

    def snap(x, y):
        if x <= edge_snap:                x = 0
        elif x >= disp_w - 1 - edge_snap: x = disp_w - 1
        if y <= edge_snap:                y = 0
        elif y >= disp_h - 1 - edge_snap: y = disp_h - 1
        return x, y

    def find_vertex(x, y):
        """Nearest vertex (of `cur` or any confirmed shape) within vertex_grab, or None."""
        best, best_d2 = None, vertex_grab ** 2
        for i, (vx, vy) in enumerate(cur):
            d2 = (vx - x) ** 2 + (vy - y) ** 2
            if d2 <= best_d2:
                best, best_d2 = ("cur", i), d2
        for ri, shape in enumerate(regions):
            for pi, (vx, vy) in enumerate(shape):
                d2 = (vx - x) ** 2 + (vy - y) ** 2
                if d2 <= best_d2:
                    best, best_d2 = ("region", ri, pi), d2
        return best

    def move_vertex(target, x, y):
        if target[0] == "cur":
            cur[target[1]] = (x, y)
        else:
            regions[target[1]][target[2]] = (x, y)

    def close_current():
        """Commit `cur` into `regions` if it's a valid shape, then reset it for the next one."""
        if (mode == 'poly' and len(cur) >= 3) or (mode == 'rect' and len(cur) == 2):
            regions.append(list(cur))
            cur.clear()
            return True
        return False

    if mode == 'poly':
        edit_hint = ("Left-click: add vertex   Drag a vertex: move it   Right-click/N: next polygon"
                     "   Backspace: undo   C: clear all")
    else:
        edit_hint = ("Drag: draw rectangle (next drag starts a new one)   Drag a corner: move it"
                     "   Backspace: undo   C: clear all")
    nav_hint = ("   ← →: ±1f   q w: ±10f   a s: ±100f   z x: ±1000f   k l: first/last frame"
               if review_frames else "")
    win = f"Select region — {edit_hint}   Enter: confirm all   Esc: abort{nav_hint}"
    print(f"  {edit_hint}   Enter: confirm all   Esc: abort{nav_hint}")
    print(f"  (clicks within {edge_snap}px of a frame edge/corner snap onto it;"
          f" within {vertex_grab}px of an existing vertex grabs it for dragging)")
    print("  Draw as many separate shapes as you need — Enter finishes and combines them all.")
    cv2.namedWindow(win)

    def draw_shape(img, points, color, closed, highlight=None):
        if len(points) >= 2:
            cv2.polylines(img, [np.array(points, dtype=np.int32)], isClosed=closed, color=color, thickness=2)
        for p in points:
            cv2.circle(img, p, 6 if p == highlight else 4, color, -1)

    def redraw(cursor=None):
        img = cur_img().copy()
        if review_frames:
            cv2.putText(img, f"Frame {cur_fi[0]} / {n_frames - 1}  ({cur_fi[0] / fps:.1f}s)",
                        (20, 40), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 255, 0), 2)
        if regions:
            cv2.putText(img, f"{len(regions)} shape(s) confirmed", (20, 70),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 200, 255), 2)

        hover = drag_target[0] or (find_vertex(*cursor) if cursor is not None else None)
        hover_pt = None
        if hover is not None:
            hover_pt = cur[hover[1]] if hover[0] == "cur" else regions[hover[1]][hover[2]]

        if mode == 'poly':
            for ri, poly in enumerate(regions):
                hl = hover_pt if hover is not None and hover[0] == "region" and hover[1] == ri else None
                draw_shape(img, poly, (0, 255, 0), True, highlight=hl)
            if cur:
                hl = hover_pt if hover is not None and hover[0] == "cur" else None
                draw_shape(img, cur, (0, 220, 255), False, highlight=hl)
                if len(cur) > 2 and cursor is None:
                    cv2.line(img, cur[-1], cur[0], (0, 220, 255), 1)
                if cursor is not None and drag_target[0] is None:
                    cv2.line(img, cur[-1], cursor, (0, 180, 220), 1)
        else:
            for (rx1, ry1), (rx2, ry2) in regions:
                cv2.rectangle(img, (rx1, ry1), (rx2, ry2), (0, 255, 0), 2)
            if len(cur) == 2:
                cv2.rectangle(img, cur[0], cur[1], (0, 220, 255), 2)
        if hover_pt is not None:
            cv2.circle(img, hover_pt, 9, (0, 0, 255), 2)
        cv2.imshow(win, img)

    def on_mouse(event, x, y, flags, param):
        x, y = snap(x, y)

        if event == cv2.EVENT_LBUTTONDOWN and drag_target[0] is None:
            hit = find_vertex(x, y)
            if hit is not None:
                drag_target[0] = hit
                redraw()
                return

        if drag_target[0] is not None:
            if event == cv2.EVENT_MOUSEMOVE:
                move_vertex(drag_target[0], x, y); redraw()
            elif event == cv2.EVENT_LBUTTONUP:
                drag_target[0] = None; redraw(cursor=(x, y))
            return

        if mode == 'poly':
            if event == cv2.EVENT_LBUTTONDOWN:
                cur.append((x, y)); redraw()
            elif event == cv2.EVENT_RBUTTONDOWN:
                close_current(); redraw()
            elif event == cv2.EVENT_MOUSEMOVE:
                redraw(cursor=(x, y))
        else:
            if event == cv2.EVENT_LBUTTONDOWN:
                close_current()  # commit whatever rectangle was drawn before this one
                cur[:] = [(x, y), (x, y)]
                dragging[0] = True
                redraw()
            elif event == cv2.EVENT_MOUSEMOVE and dragging[0]:
                cur[1] = (x, y); redraw()
            elif event == cv2.EVENT_MOUSEMOVE:
                redraw(cursor=(x, y))
            elif event == cv2.EVENT_LBUTTONUP and dragging[0]:
                dragging[0] = False; cur[1] = (x, y); redraw()

    cv2.setMouseCallback(win, on_mouse)
    redraw()

    while not state["confirm"] and not state["abort"]:
        k = cv2.waitKeyEx(20)
        if   k == 13:                        close_current(); state["confirm"] = True
        elif k == 27:                          state["abort"] = True
        elif mode == 'poly' and k == ord('n'): close_current(); redraw()
        elif k in (8, 127):
            if cur:
                cur.pop() if mode == 'poly' else cur.clear()
            elif regions:
                regions.pop()
            redraw()
        elif k == ord('c'):
            regions.clear(); cur.clear(); redraw()
        elif review_frames and k == 2424832: cur_fi[0] = max(0, cur_fi[0] - 1); redraw()
        elif review_frames and k == 2555904: cur_fi[0] = min(n_frames - 1, cur_fi[0] + 1); redraw()
        elif review_frames and k == ord('q'): cur_fi[0] = max(0, cur_fi[0] - 10); redraw()
        elif review_frames and k == ord('w'): cur_fi[0] = min(n_frames - 1, cur_fi[0] + 10); redraw()
        elif review_frames and k == ord('a'): cur_fi[0] = max(0, cur_fi[0] - 100); redraw()
        elif review_frames and k == ord('s'): cur_fi[0] = min(n_frames - 1, cur_fi[0] + 100); redraw()
        elif review_frames and k == ord('z'): cur_fi[0] = max(0, cur_fi[0] - 1000); redraw()
        elif review_frames and k == ord('x'): cur_fi[0] = min(n_frames - 1, cur_fi[0] + 1000); redraw()
        elif review_frames and k == ord('k'): cur_fi[0] = 0; redraw()
        elif review_frames and k == ord('l'): cur_fi[0] = n_frames - 1; redraw()

    cv2.destroyWindow(win)
    cap.release()

    if state["abort"]:
        print("  Aborted.")
        return None

    if mode == 'rect':
        if not regions:
            print("  No rectangle drawn — aborted.")
            return None
        result = []
        for (dx1, dy1), (dx2, dy2) in regions:
            x1, x2 = sorted((dx1, dx2))
            y1, y2 = sorted((dy1, dy2))
            result.append(tuple(int(v / scale) for v in (x1, y1, x2, y2)))
        for x1, y1, x2, y2 in result:
            print(f"  ({x1}, {y1}) → ({x2}, {y2})  [{x2-x1} x {y2-y1} px]")
        print(f"  {len(result)} rectangle(s)")
        return result

    if not regions:
        print("  Need at least one polygon (3+ points) — aborted.")
        return None
    result = [[(int(x / scale), int(y / scale)) for x, y in poly] for poly in regions]
    print(f"  {len(result)} polygon(s), {sum(len(p) for p in result)} total vertices")
    return result


def crop_video(input_video, output_video, x1=None, y1=None, x2=None, y2=None,
               inspect=False, frame_index=0, ffmpeg_path="ffmpeg", crf=18):
    """
    Crop every frame of a video to the rectangle defined by (x1, y1) – (x2, y2).

    Coordinates can be given directly or selected interactively. The selector opens
    when inspect=True or when any coordinate is None: drag a rectangle with the mouse,
    Space/Enter to confirm, C/Esc to abort.

    :param input_video   [str or Path]  Input video file path.
    :param output_video  [str or Path]  Output video file path.
    :param x1            [int|None]     Left pixel. None → pick interactively.
    :param y1            [int|None]     Top pixel. None → pick interactively.
    :param x2            [int|None]     Right pixel (exclusive). None → pick interactively.
    :param y2            [int|None]     Bottom pixel (exclusive). None → pick interactively.
    :param inspect       [bool]         Open selector even when all coordinates are provided.
    :param frame_index   [int]          Frame shown in the selector. Default 0.
    :param ffmpeg_path   [str]          Name or full path of the ffmpeg executable.
    :param crf           [int]          H.264 quality (0=lossless, 51=worst). 18 is visually lossless.

    :return  [(Path, tuple)]  (output path, (x1, y1, x2, y2)) on success.
             [(None, None)]   If the selector was aborted.
    """
    if inspect or any(c is None for c in (x1, y1, x2, y2)):
        result = select_region(input_video, frame_index=frame_index, mode='rect')
        if result is None:
            return None, None
        x1, y1, x2, y2 = result

    crop_w = x2 - x1
    crop_h = y2 - y1
    cmd = [
        ffmpeg_path, "-y",
        "-i", str(input_video),
        "-vf", f"crop={crop_w}:{crop_h}:{x1}:{y1}",
        "-c:v", "libx264", "-crf", str(crf),
        "-c:a", "copy",
        str(output_video),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"Saved cropped video to {output_video}")

    return Path(output_video), (x1, y1, x2, y2)


def undistort_image(img, K, D, model="pinhole", balance=0.0):
    """
    Undistort a single image given known camera intrinsics.

    :param img      [np.ndarray]  Input distorted image.
    :param K        [np.ndarray]  Camera intrinsic matrix.
    :param D        [np.ndarray]  Distortion coefficients.
    :param model    [str]         'pinhole' or 'fisheye'.
    :param balance  [float]       Fisheye only: 0 = crop to valid pixels, 1 = keep all pixels (black borders).

    :return  [np.ndarray]  Undistorted image, same shape as input.
    """
    h, w = img.shape[:2]

    if model == "pinhole":
        new_k, _ = cv2.getOptimalNewCameraMatrix(K, D, (w, h), 1, (w, h))
        return cv2.undistort(img, K, D, None, new_k)

    if model == "fisheye":
        new_k = cv2.fisheye.estimateNewCameraMatrixForUndistortRectify(
            K, D, (w, h), np.eye(3), balance=balance)
        map1, map2 = cv2.fisheye.initUndistortRectifyMap(
            K, D, np.eye(3), new_k, (w, h), cv2.CV_16SC2)
        return cv2.remap(img, map1, map2, interpolation=cv2.INTER_LINEAR,
                         borderMode=cv2.BORDER_CONSTANT)

    raise ValueError(f"Unknown model: {model!r}")


def undistort_video(input_video, output_video, K, D, model="pinhole", balance=0.0):
    """
    Undistort a video given known camera intrinsics.

    :param input_video   [str or Path]  Input video file path.
    :param output_video  [str or Path]  Output video file path.
    :param K             [np.ndarray]   Camera intrinsic matrix.
    :param D             [np.ndarray]   Distortion coefficients.
    :param model         [str]          'pinhole' or 'fisheye'.
    :param balance       [float]        Fisheye only: 0 = crop to valid pixels, 1 = keep all pixels (black borders).
    """
    cap, fps, w, h, n_frames = open_video(input_video)
    fourcc = cv2.VideoWriter.fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(output_video), fourcc, fps, (w, h))

    if model == "fisheye":
        new_k = cv2.fisheye.estimateNewCameraMatrixForUndistortRectify(
            K, D, (w, h), np.eye(3), balance=balance)
        map1, map2 = cv2.fisheye.initUndistortRectifyMap(
            K, D, np.eye(3), new_k, (w, h), cv2.CV_16SC2)
    else:
        new_k, _ = cv2.getOptimalNewCameraMatrix(K, D, (w, h), 1, (w, h))
        map1, map2 = cv2.initUndistortRectifyMap(
            K, D, None, new_k, (w, h), cv2.CV_16SC2)

    print(f"Undistorting {n_frames} frames...")
    i = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        writer.write(cv2.remap(frame, map1, map2, interpolation=cv2.INTER_LINEAR))
        i += 1
        if i % 100 == 0:
            print(f"  {i}/{n_frames}")

    cap.release()
    writer.release()
    print(f"Saved undistorted video to {output_video}")


# Timestamps editing ---------------------------------------------------------------------------------------------------

def shift_timestamps(path, delta) -> int:
    """
    Add a constant offset to every wall-clock datetime ('YYYY-MM-DD HH:MM:SS.fff') in a text file, in place.
     Works for DJI SRT files and frame/results CSVs alike.

    Use to correct a drone-clock offset (e.g. a DST error): delta = timedelta(hours=1) adds an hour;
    a negative timedelta subtracts.

    :param path   [str or Path]  SRT or CSV file to edit in place.
    :param delta  [timedelta]    Offset to add.
    :return  [int]  Number of timestamps shifted.
    """
    dt_re = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+')

    def repl(m):
        dt = datetime.strptime(m.group(), '%Y-%m-%d %H:%M:%S.%f')
        return (dt + delta).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # keep milliseconds (3 dp)

    text = Path(path).read_bytes().decode('utf-8', errors='replace')  # bytes -> keep line endings
    new, n = dt_re.subn(repl, text)
    Path(path).write_bytes(new.encode('utf-8'))
    return n


def join_srt(srt_files: list, output) -> bool:
    """
    Merge a list of SRT files into a single continuous SRT, offsetting timestamps.

    :param srt_files  [list of Path]  Ordered SRT files to merge.
    :param output     [str or Path]   Output .srt file path.
    """
    time_re = re.compile(r"(\d{2}):(\d{2}):(\d{2}),(\d{3})")

    def parse_time(s):
        h, m, sec, ms = map(int, time_re.match(s.strip()).groups())
        return timedelta(hours=h, minutes=m, seconds=sec, milliseconds=ms)

    def fmt_time(td):
        total_ms = int(td.total_seconds() * 1000)
        h, rem = divmod(total_ms, 3_600_000)
        m, rem = divmod(rem, 60_000)
        s, ms  = divmod(rem, 1_000)
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

    def parse_srt(path):
        text = path.read_text(encoding='utf-8', errors='replace')
        entries = []
        for block in re.split(r'\n\s*\n', text.strip()):
            block_lines = block.strip().splitlines()
            for idx, line in enumerate(block_lines):
                if '-->' not in line:
                    continue
                parts = line.split('-->')
                try:
                    entries.append({
                        'start':   parse_time(parts[0]),
                        'end':     parse_time(parts[1]),
                        'content': '\n'.join(block_lines[idx + 1:]),
                    })
                except (AttributeError, ValueError):
                    pass
                break
        return entries

    all_entries = []
    offset = timedelta(0)
    for srt_path in [Path(p) for p in srt_files]:
        entries = parse_srt(srt_path)
        if not entries:
            print(f"    WARNING: could not parse {srt_path.name} — skipping SRT merge")
            return False

        for e in entries:
            all_entries.append({
                'start':   e['start'] + offset,
                'end':     e['end']   + offset,
                'content': e['content'],
            })
        offset = all_entries[-1]['end']  # cumulative end → offset for next file

    out_lines = []
    for i, e in enumerate(all_entries, 1):
        out_lines += [str(i), f"{fmt_time(e['start'])} --> {fmt_time(e['end'])}", e['content'], '']
    Path(output).write_text('\n'.join(out_lines), encoding='utf-8')

    return True


def trim_srt(srt_path, start_frame=None, end_frame=None):
    """
    Trim a SRT file to [start_frame, end_frame) in-place.

    :param srt_path     [str or Path]  SRT file to trim (modified in-place).
    :param start_frame  [int|None]     First entry to keep (0-based, inclusive). None = 0.
    :param end_frame    [int|None]     First entry to drop (exclusive). None = keep all remaining.
    """
    ts_re = re.compile(r'(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})')
    seq_re = re.compile(r'^\d+')
    srtcnt_re = re.compile(r'(SrtCnt\s*:\s*)\d+')

    def parse_td(s):
        h, m, rest = s.split(':')
        sec, ms = rest.split(',')
        return timedelta(hours=int(h), minutes=int(m), seconds=int(sec), milliseconds=int(ms))

    def fmt_td(td):
        total_ms = int(td.total_seconds() * 1000)
        h, rem = divmod(total_ms, 3_600_000)
        m, rem = divmod(rem, 60_000)
        s, ms  = divmod(rem, 1_000)
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

    text = Path(srt_path).read_text(encoding='utf-8', errors='replace')
    blocks = [b.strip() for b in re.split(r'\n\s*\n', text.strip()) if b.strip()]
    kept = blocks[start_frame:end_frame]

    first_match = ts_re.search(kept[0]) if kept else None
    if first_match:
        trim_offset = parse_td(first_match.group(1))
        reoffset = []
        for block in kept:
            def shift(m, off=trim_offset):
                return f"{fmt_td(parse_td(m.group(1)) - off)} --> {fmt_td(parse_td(m.group(2)) - off)}"
            reoffset.append(ts_re.sub(shift, block))
        kept = reoffset

    renumbered = []
    for i, block in enumerate(kept, start=1):
        block = seq_re.sub(str(i), block, count=1)
        block = srtcnt_re.sub(rf'\g<1>{i}', block)
        renumbered.append(block)
    kept = renumbered

    Path(srt_path).write_text('\n\n'.join(kept) + '\n', encoding='utf-8')
    print(f"  SRT trimmed to {len(kept)} entries → {Path(srt_path).name}")


def trim_csv(csv_path, start_frame=None, end_frame=None):
    """
    Trim a frame→timestamp CSV file to [start_frame, end_frame) in-place.

    :param csv_path     [str or Path]  CSV file to trim (modified in-place).
    :param start_frame  [int|None]     First data row to keep (0-based, inclusive). None = 0.
    :param end_frame    [int|None]     First data row to drop (exclusive). None = keep all remaining.
    """
    lines = Path(csv_path).read_text(encoding='utf-8').splitlines()
    header, data = lines[0], lines[1:]
    kept = data[start_frame:end_frame]
    frame_idx = header.split(',').index('frame')

    renumbered = []
    for i, line in enumerate(kept):
        fields = line.split(',')
        fields[frame_idx] = str(i)
        renumbered.append(','.join(fields))
    Path(csv_path).write_text('\n'.join([header] + renumbered) + '\n', encoding='utf-8')

    print(f"  CSV trimmed to {len(renumbered)} rows → {Path(csv_path).name}")


# Video stabilization --------------------------------------------------------------------------------------------------

def stabilize_register_frame(ref_kp, ref_desc, frame_gray, static_mask=None,
                             ratio_thresh=0.75, min_matches=20):
    """
    Estimate the registration transform that maps frame_gray onto the reference frame.

    Uses ORB feature matching with Lowe's ratio test followed by RANSAC to find the
    3x3 perspective transform (registration transform) aligning the frame to the reference.

    :param ref_kp        [list]             Keypoints of the reference frame.
    :param ref_desc      [np.ndarray]       Descriptors of the reference frame.
    :param frame_gray    [np.ndarray]       Grayscale frame to register.
    :param static_mask   [np.ndarray|None]  Binary mask restricting feature detection to static regions.
    :param ratio_thresh  [float]            Lowe's ratio test threshold for match filtering.
    :param min_matches   [int]              Minimum number of inlier matches required for a valid transform.

    :return  T          [np.ndarray (3,3)|None]   Registration transform mapping frame → reference.
                                                  None if registration failed.
             n_inliers  [int]                     Number of RANSAC inliers.
    """
    orb = cv2.ORB.create(nfeatures=2000)
    kp, desc = orb.detectAndCompute(frame_gray, static_mask)
    if desc is None or len(desc) < min_matches:
        return None, 0

    matcher = cv2.BFMatcher(cv2.NORM_HAMMING)
    knn = matcher.knnMatch(desc, ref_desc, k=2)

    good = []
    for pair in knn:
        if len(pair) < 2:
            continue
        m, n = pair
        if m.distance < ratio_thresh * n.distance:
            good.append(m)

    if len(good) < min_matches:
        return None, 0

    src_pts = np.float32([kp[m.queryIdx].pt for m in good]).reshape(-1, 1, 2)
    dst_pts = np.float32([ref_kp[m.trainIdx].pt for m in good]).reshape(-1, 1, 2)

    transform, inlier_mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 3.0)
    n_inliers = int(inlier_mask.sum()) if inlier_mask is not None else 0

    if transform is None or n_inliers < min_matches:
        return None, n_inliers

    return transform, n_inliers


import cv2
import numpy as np
from pathlib import Path


def _to_3x3(M):
    """Promote stabilo's 2x3 affine or 3x3 projective matrix to a finite 3x3, else None."""
    if M is None:
        return None
    M = np.asarray(M, dtype=np.float64)
    if M.shape == (2, 3):
        M = np.vstack([M, [0.0, 0.0, 1.0]])
    if M.shape != (3, 3) or not np.isfinite(M).all():
        return None
    return M


def _read_reference_frame(input_path, index):
    """Read one frame without disturbing the capture used for the main pass."""
    cap = cv2.VideoCapture(str(input_path))
    try:
        if index > 0:
            cap.set(cv2.CAP_PROP_POS_FRAMES, index)
            if int(cap.get(cv2.CAP_PROP_POS_FRAMES)) != index:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                for _ in range(index):
                    if not cap.grab():
                        raise RuntimeError(f"Video shorter than frame {index}")
        ret, frame = cap.read()
        if not ret:
            raise RuntimeError(f"Cannot read reference frame {index}")
        return frame
    finally:
        cap.release()


def stabilize_video(input_path, output_path, reference_frame_index=0,
                    min_inliers=25, write_diagnostics=True, **stabilizer_kwargs):
    """
    Stabilize a video by registering all frames to a reference frame, using the `stabilo` package.

    Every output frame is expressed in the coordinate system of the reference frame, so a
    homography computed against the reference frame stays valid for the whole output. When direct
    registration to the reference weakens (viewpoint has drifted too far), frames are registered
    against a recent keyframe instead and the transform is composed back to reference coordinates.

    :param input_path              [str or Path]  Input video file path.
    :param output_path             [str or Path]  Output stabilized video file path.
    :param reference_frame_index   [int]          Frame index used as the registration target.
    :param min_inliers             [int]          Inlier count below which a registration is
                                                  distrusted and the keyframe chain is used.
    :param write_diagnostics       [bool]         If True, save a _log.npz with per-frame inlier
                                                  counts and the composed transform matrices.
    :param stabilizer_kwargs                      Extra kwargs forwarded to stabilo.Stabilizer.

    :return  diagnostics  [dict]  Keys: reference_frame_index, n_inliers_per_frame, matrices,
                                        chained_frames, failed_frames, mean_inliers, min_inliers.
    """
    from stabilo import Stabilizer

    # Registers against the fixed reference frame. Never re-anchored.
    stab_ref = Stabilizer(mask_use=False, viz=True, **stabilizer_kwargs)
    # Registers against a rolling keyframe. Re-anchored on demand.
    stab_key = Stabilizer(mask_use=False, viz=True, **stabilizer_kwargs)

    ref_frame = _read_reference_frame(input_path, reference_frame_index)
    stab_ref.set_ref_frame(ref_frame)
    stab_key.set_ref_frame(ref_frame)
    n_ref_features = len(stab_ref.ref_kpts) if stab_ref.ref_kpts is not None else 0
    print(f"Reference frame: {reference_frame_index}, {n_ref_features} features")

    cap, fps, w, h, n_frames = open_video(input_path)   # fresh handle, never seeked

    fourcc = cv2.VideoWriter.fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(output_path), fourcc, fps, (w, h))
    if not writer.isOpened():
        cap.release()
        raise RuntimeError(f"Could not open writer for {output_path}")

    H_key_to_ref = np.eye(3)      # keyframe coords -> reference coords
    prev_frame = None
    H_prev_to_ref = np.eye(3)
    last_good_H = np.eye(3)

    inliers_per_frame, matrices, chained, failed = [], [], [], []
    print(f"Stabilizing (container reports {n_frames} frames)...")

    i = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if frame.shape[1] != w or frame.shape[0] != h:
            raise RuntimeError(f"Frame {i} is {frame.shape[1]}x{frame.shape[0]}, writer expects {w}x{h}")

        # --- attempt 1: straight to the reference frame (no drift) ---
        stab_ref.stabilize(frame)
        H = _to_3x3(stab_ref.get_cur_trans_matrix())
        n_in = stab_ref.get_cur_inliers_count() or 0
        used_chain = False

        # --- attempt 2: via the rolling keyframe, composed back to reference ---
        if H is None or n_in < min_inliers:
            stab_key.stabilize(frame)
            H_ck = _to_3x3(stab_key.get_cur_trans_matrix())
            n_key = stab_key.get_cur_inliers_count() or 0

            # keyframe itself has gone stale -> re-anchor it on the previous frame
            if (H_ck is None or n_key < min_inliers) and prev_frame is not None:
                stab_key.set_ref_frame(prev_frame)
                H_key_to_ref = H_prev_to_ref
                stab_key.stabilize(frame)
                H_ck = _to_3x3(stab_key.get_cur_trans_matrix())
                n_key = stab_key.get_cur_inliers_count() or 0

            if H_ck is not None and n_key >= min_inliers:
                H = _to_3x3(H_key_to_ref @ H_ck)
                n_in = n_key
                used_chain = True
                chained.append(i)

        if H is None:
            failed.append(i)
            H = last_good_H
            n_in = 0
        else:
            last_good_H = H

        warped = cv2.warpPerspective(frame, H, (w, h))
        writer.write(warped)

        inliers_per_frame.append(n_in)
        matrices.append(H)
        prev_frame, H_prev_to_ref = frame, H

        if i % 100 == 0:
            tag = " [chained]" if used_chain else ""
            print(f"  frame {i}, {n_in} inliers{tag}")
        i += 1

    cap.release()
    writer.release()
    print(f"Wrote {i} frames (container claimed {n_frames}).")

    if chained:
        print(f"{len(chained)} frames registered via the keyframe chain "
              f"(first at {chained[0]}, {100 * len(chained) / max(i, 1):.0f}% of frames).")
    if failed:
        print(f"WARNING: {len(failed)} frames had no valid transform and reused the last "
              f"known-good one: {failed[:10]}...")

    inliers_per_frame = np.array(inliers_per_frame)
    diagnostics = {
        "reference_frame_index": reference_frame_index,
        "n_inliers_per_frame": inliers_per_frame,
        "matrices": np.array(matrices),
        "chained_frames": np.array(chained),
        "failed_frames": np.array(failed),
        "mean_inliers": float(inliers_per_frame.mean()) if len(inliers_per_frame) else 0.0,
        "min_inliers": int(inliers_per_frame.min()) if len(inliers_per_frame) else 0,
    }

    if write_diagnostics:
        log_path = Path(output_path).with_name(Path(output_path).stem + "_log.npz")
        np.savez(str(log_path), **diagnostics)
        print(f"Log saved to {log_path}")
        print(f"Mean inliers: {diagnostics['mean_inliers']:.0f}, min: {diagnostics['min_inliers']}")

    return diagnostics


def stabilize_residual_drift(video_path, reference_frame_index=0,
                             static_mask_path=None, sample_step=30):
    """
    Estimate residual drift in a stabilized video relative to the reference frame.

    :param video_path              [str or Path]       Path to the stabilized video.
    :param reference_frame_index   [int]               Reference frame index used during stabilization.
    :param static_mask_path        [str or Path|None]  Same mask used during stabilization, if any.
    :param sample_step             [int]               Check every Nth frame instead of all frames.

    :return  results  [dict]  Keys: sample_indices, translation_px, rotation_deg.
    """
    cap, _, _, _, n_frames = open_video(video_path)

    static_mask = None
    if static_mask_path:
        static_mask = cv2.imread(str(static_mask_path), cv2.IMREAD_GRAYSCALE)

    cap.set(cv2.CAP_PROP_POS_FRAMES, reference_frame_index)
    ret, ref = cap.read()
    if not ret:
        raise RuntimeError(f"Cannot read reference frame {reference_frame_index}")
    ref_gray = cv2.cvtColor(ref, cv2.COLOR_BGR2GRAY)
    orb = cv2.ORB.create(nfeatures=2000)
    ref_kp, ref_desc = orb.detectAndCompute(ref_gray, static_mask)

    sample_indices = list(range(0, n_frames, sample_step))
    translations = []
    rotations_deg = []

    for idx in sample_indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ret, frame = cap.read()
        if not ret:
            continue
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        transform, n_in = stabilize_register_frame(ref_kp, ref_desc, gray, static_mask)
        if transform is None:
            translations.append(np.nan)
            rotations_deg.append(np.nan)
            continue
        tx, ty = transform[0, 2], transform[1, 2]
        translations.append(np.hypot(tx, ty))
        rot = np.degrees(np.arctan2(transform[1, 0], transform[0, 0]))
        rotations_deg.append(rot)

    cap.release()

    translations = np.array(translations)
    rotations_deg = np.array(rotations_deg)

    print(f"Residual drift over {len(sample_indices)} samples:")
    print(f"  Translation: mean {float(np.nanmean(translations)):.2f} px, "
          f"max {float(np.nanmax(translations)):.2f} px")
    print(f"  Rotation:    mean {float(np.nanmean(np.abs(rotations_deg))):.3f} deg, "
          f"max {float(np.nanmax(np.abs(rotations_deg))):.3f} deg")

    return {
        "sample_indices": np.array(sample_indices),
        "translation_px": translations,
        "rotation_deg": rotations_deg,
    }


def measure_stability(video_path, sample_every=5, max_corners=300):
    """
    Measure frame-to-frame camera motion in a video using sparse optical flow.

    Two metrics are computed per sampled interval:
    - displacement: translation magnitude between consecutive samples [px]. High
      values indicate an unstable or rapidly moving segment.
    - cumulative drift: total displacement accumulated from the first frame [px
      per axis]. The peak absolute drift gives the minimum crop margin per side.

    :param video_path    [str or Path]  Input video file.
    :param sample_every  [int]          Analyse every Nth frame. Default 5 (~6 samples/s at 30 fps).
    :param max_corners   [int]          Feature points to track per frame. Default 300.

    :return  [dict]  Keys:
        sample_indices    [np.ndarray (N,)]  Frame indices of each analysed interval.
        displacement_px   [np.ndarray (N,)]  Frame-to-frame translation magnitude [px].
        drift_x_px        [np.ndarray (N,)]  Cumulative horizontal drift from frame 0 [px].
        drift_y_px        [np.ndarray (N,)]  Cumulative vertical drift from frame 0 [px].
        mean_displacement [float]            Mean frame-to-frame displacement [px].
        max_displacement  [float]            Peak frame-to-frame displacement [px].
        crop_margin_x     [int]              Minimum horizontal crop margin per side [px].
        crop_margin_y     [int]              Minimum vertical crop margin per side [px].
    """
    cap, fps, w, h, n_frames = open_video(video_path)

    lk_params  = dict(winSize=(21, 21), maxLevel=3,
                      criteria=(cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 30, 0.01))
    feat_params = dict(maxCorners=max_corners, qualityLevel=0.01, minDistance=10)

    ret, first = cap.read()
    if not ret:
        cap.release()
        raise RuntimeError(f"Cannot read first frame from {video_path}")
    prev_gray = cv2.cvtColor(first, cv2.COLOR_BGR2GRAY)

    current_frame = 0
    drift_x = 0.0
    drift_y = 0.0
    valid_indices = []
    displacements = []
    drift_xs = []
    drift_ys = []

    print(f"Measuring stability: {Path(video_path).name}  ({n_frames} frames, every {sample_every})")
    done = False
    while not done:
        for _ in range(sample_every - 1):
            if not cap.grab():
                done = True
                break
            current_frame += 1
        if done:
            break

        ret, frame = cap.read()
        if not ret:
            break
        current_frame += 1

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        pts = cv2.goodFeaturesToTrack(prev_gray, **feat_params)
        disp = 0.0
        if pts is not None and len(pts) >= 4:
            pts_next, status, _ = cv2.calcOpticalFlowPyrLK(prev_gray, gray, pts, None, **lk_params)
            good_old = pts[status.ravel() == 1]
            good_new = pts_next[status.ravel() == 1]
            if len(good_old) >= 4:
                M, _ = cv2.estimateAffinePartial2D(good_old, good_new, method=cv2.RANSAC)
                if M is not None:
                    tx, ty = float(M[0, 2]), float(M[1, 2])
                    drift_x += tx
                    drift_y += ty
                    disp = float(np.hypot(tx, ty))

        valid_indices.append(current_frame)
        displacements.append(disp)
        drift_xs.append(drift_x)
        drift_ys.append(drift_y)
        prev_gray = gray

    cap.release()

    sample_indices = np.array(valid_indices)
    displacements  = np.array(displacements)
    drift_xs       = np.array(drift_xs)
    drift_ys       = np.array(drift_ys)

    mean_disp = float(np.mean(displacements)) if len(displacements) else 0.0
    max_disp  = float(np.max(displacements))  if len(displacements) else 0.0
    margin_x  = int(np.ceil(np.max(np.abs(drift_xs)))) if len(drift_xs) else 0
    margin_y  = int(np.ceil(np.max(np.abs(drift_ys)))) if len(drift_ys) else 0

    print(f"  Frame-to-frame:   mean {mean_disp:.1f} px   max {max_disp:.1f} px")
    print(f"  Cumulative drift: ±{margin_x} px horizontal   ±{margin_y} px vertical  (min safe crop margin per side)")

    return {
        "sample_indices":    sample_indices,
        "displacement_px":   displacements,
        "drift_x_px":        drift_xs,
        "drift_y_px":        drift_ys,
        "mean_displacement": mean_disp,
        "max_displacement":  max_disp,
        "crop_margin_x":     margin_x,
        "crop_margin_y":     margin_y,
    }


# Video anonymization --------------------------------------------------------------------------------------------------

def create_mask(video_path, output_mask, frame_index=0, mode='poly', review_frames=True,
                edit_existing=False):
    """
    Interactively draw a region on a video frame and save it as a binary mask PNG.

    The selected region is filled black (masked out, zeroed to solid black); everything outside
    stays white (kept, unchanged from the original frame).
    Used to create the mask_path argument for mask_video.

    :param video_path     [str or Path]  Source video (provides the reference frame).
    :param output_mask    [str or Path]  Destination PNG path.
    :param frame_index    [int]          Initial frame to draw on. Default 0.
    :param mode           [str]          'poly' — click polygon vertices (default).
                                         'rect' — drag a rectangle.
    :param review_frames  [bool]         Step through other frames (see select_region) to check
                                         the region holds up through the whole video — e.g. a
                                         drone shot drifting or panning — before confirming.
                                         Default True.
    :param edit_existing  [bool]         If output_mask already exists, reopen the editor pre-loaded
                                         with its shapes so you can add to or undo them instead of
                                         starting over. Loaded from the "<output_mask>.json" sidecar
                                         saved alongside it when available (exact vertices); falls
                                         back to re-tracing the PNG's contours (lossy — pixel-level
                                         staircasing on angled edges can add extra vertices) for
                                         masks saved before this sidecar existed. Ignored if
                                         output_mask doesn't exist yet. Default False.

    :return  [Path|None]  Path to the saved mask PNG, or None if aborted.
    """
    output_mask = Path(output_mask)
    shapes_path = output_mask.with_suffix(".json")
    seed_regions = None
    if edit_existing and output_mask.exists():
        if shapes_path.exists():
            saved = json.loads(shapes_path.read_text())
            if saved["mode"] == mode:
                seed_regions = saved["shapes"]
                print(f"  Loaded {len(seed_regions)} existing shape(s) from {shapes_path.name}")
            else:
                print(f"  {shapes_path.name} was drawn in '{saved['mode']}' mode, not '{mode}'"
                      " — falling back to re-tracing the mask PNG.")
        if seed_regions is None:
            existing = cv2.imread(str(output_mask), cv2.IMREAD_GRAYSCALE)
            contours, _ = cv2.findContours(cv2.bitwise_not(existing), cv2.RETR_EXTERNAL,
                                           cv2.CHAIN_APPROX_SIMPLE)
            if mode == 'rect':
                seed_regions = []
                for cnt in contours:
                    x, y, w, h = cv2.boundingRect(cnt)
                    seed_regions.append((x, y, x + w, y + h))
            else:
                seed_regions = []
                for cnt in contours:
                    eps = 0.01 * cv2.arcLength(cnt, True)
                    approx = cv2.approxPolyDP(cnt, eps, True)
                    seed_regions.append([(int(p[0][0]), int(p[0][1])) for p in approx])
            print(f"  Loaded {len(seed_regions)} existing shape(s) from {output_mask.name}"
                  " (approximated from the PNG — no .json sidecar found)")

    region = select_region(video_path, frame_index=frame_index, mode=mode,
                           review_frames=review_frames, seed_regions=seed_regions)
    if region is None:
        return None

    cap, _, vid_w, vid_h, _ = open_video(video_path)
    cap.release()

    shapes: list = region if isinstance(region, list) else [region]
    mask = np.full((vid_h, vid_w), 255, dtype=np.uint8)
    if mode == 'rect':
        for x1, y1, x2, y2 in shapes:
            cv2.rectangle(mask, (x1, y1), (x2, y2), 0, -1)
        desc = f"{len(shapes)} rectangle(s)"
    else:
        cv2.fillPoly(mask, [np.array(poly, dtype=np.int32) for poly in shapes], 0)
        desc = f"{len(shapes)} polygon(s), {sum(len(p) for p in shapes)} vertices"

    output_mask.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(output_mask), mask)
    shapes_path.write_text(json.dumps({"mode": mode, "shapes": shapes}))
    print(f"Saved mask to {output_mask}  ({desc}, {vid_w}x{vid_h})")

    return output_mask


def mask_video(input_video, output_video, mask_path, mode='poly', frame_index=0,
               review_frames=True, edit_mask=False, ffmpeg_path="ffmpeg", crf=18):
    """
    Apply a binary mask to every frame of a video using FFmpeg.

    Pixels where the mask is WHITE (255) are kept unchanged.
    Pixels where the mask is BLACK (0) are set to black.
    Audio is preserved.

    :param input_video    [str or Path]  Input video file path.
    :param output_video   [str or Path]  Output masked video file path.
    :param mask_path      [str or Path]  Binary mask PNG path (grayscale: white=keep, black=zero).
                                         Created interactively if the file does not exist.
    :param mode           [str]          Region shape: 'poly' (click vertices) or 'rect' (drag).
    :param frame_index    [int]          Initial frame shown in the region editor. Default 0.
    :param review_frames  [bool]         When creating a new mask, step through the video's frames
                                         (see select_region) to check the region still holds up
                                         before confirming. Default True.
    :param edit_mask      [bool]         If mask_path already exists, reopen the editor pre-loaded
                                         with its existing shapes so you can add to or undo them,
                                         instead of reusing the PNG as-is. Default False.
    :param ffmpeg_path    [str]          Name or full path of the ffmpeg executable.
    :param crf            [int]          H.264 quality (0=lossless, 51=worst). 18 is visually lossless.

    :return  [Path|None]  Path to the masked output video, or None if the editor was aborted.
    """
    mask_path = Path(mask_path)
    if edit_mask or not mask_path.exists():
        if create_mask(input_video, mask_path, frame_index=frame_index, mode=mode,
                       review_frames=review_frames, edit_existing=edit_mask) is None:
            return None

    cap, fps, _, _, _ = open_video(input_video)
    cap.release()
    total_frames = get_frame_count(input_video)
    cmd = [
        ffmpeg_path, "-y",
        "-i", str(input_video),
        "-loop", "1", "-framerate", str(fps), "-i", str(mask_path),
        "-filter_complex",
        "[0:v]format=rgb24[v0];[1:v]format=rgb24[v1];"
        "[v0][v1]blend=all_mode=multiply,scale=in_range=full:out_range=full,format=yuv420p",
        "-color_range", "2",
        "-frames:v", str(total_frames),
        "-c:v", "libx264", "-crf", str(crf),
        "-c:a", "copy",
        "-shortest",
        "-progress", "pipe:1",
        "-nostats",
        str(output_video),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    assert proc.stderr is not None
    assert proc.stdout is not None
    stderr_stream = proc.stderr
    stdout_stream = proc.stdout
    stderr_buf = []
    def _read_stderr():
        for line in stderr_stream:
            stderr_buf.append(line)
    threading.Thread(target=_read_stderr, daemon=True).start()

    frame = 0
    for line in stdout_stream:
        if line.startswith("frame="):
            try:
                frame = int(line.split("=", 1)[1].strip())
            except ValueError:
                pass
            pct = frame / total_frames * 100 if total_frames else 0
            print(f"\r  {frame}/{total_frames} frames  ({pct:.1f}%)", end="", flush=True)

    proc.wait()
    print()
    if proc.returncode != 0:
        raise RuntimeError(f"FFmpeg failed (exit {proc.returncode}):\n{''.join(stderr_buf)}")

    print(f"Saved masked video to {output_video}")

    return Path(output_video)


def anonymize_video(input_video, output_video=None, threshold=0.2, method="blur",
                    scale=None, debug=False):
    """
    Anonymize faces in a video by detecting and obscuring them.
    Uses deface for face detection (RetinaFace backbone via onnxruntime).

    :param input_video   [str or Path]  Input video file.
    :param output_video  [str or Path]  Output path. Defaults to <input>_anon.mp4.
    :param threshold     [float]        Detection confidence threshold (0–1).
                                        Lower values detect more faces but increase false positives.
    :param method        [str]          How to obscure detected faces:
                                        'blur' (Gaussian), 'pixelate', or 'solid' (black rectangle).
    :param scale         [str]          Resolution for face detection inference (e.g. "640x360").
                                        Output is always written at original resolution.
                                        Lower values are faster; None disables downscaling.
    :param debug         [bool]         Also write a <stem>_anon_debug.mp4 with confidence scores
                                        drawn on each detection. Useful for tuning threshold.

    :return  [Path]  Path to the anonymized video.
    """
    method_map = {"blur": "blur", "pixelate": "mosaic", "solid": "solid"}
    if method not in method_map:
        raise ValueError(f"method must be one of {list(method_map)}; got {method!r}")

    input_video = Path(input_video)
    if output_video is None:
        output_video = input_video.with_name(input_video.stem + "_anon.mp4")
    output_video = Path(output_video)
    output_video.parent.mkdir(parents=True, exist_ok=True)

    deface_exe = Path(sys.executable).parent / "deface"
    ffmpeg_config = '{"codec": "libx264", "macro_block_size": 1, "output_params": ["-crf", "17"]}'

    def run_deface(out_path, draw_scores=False):
        cmd = [
            str(deface_exe), str(input_video),
            "--output", str(out_path),
            "--thresh", str(threshold),
            "--replacewith", method_map[method],
            "--ffmpeg-config", ffmpeg_config,
        ]
        if scale:
            cmd += ["--scale", scale]
        if draw_scores:
            cmd += ["--draw-scores"]
        subprocess.run(cmd, check=True)

    if debug:
        debug_path = output_video.with_name(output_video.stem + "_debug.mp4")
        print(f"Anonymizing {input_video.name} (debug, method={method}, threshold={threshold})...")
        run_deface(debug_path, draw_scores=True)
        print(f"Saved debug video to {debug_path}")
        return debug_path
    else:
        print(f"Anonymizing {input_video.name} (method={method}, threshold={threshold})...")
        run_deface(output_video)
        print(f"Saved anonymized video to {output_video}")
        return output_video