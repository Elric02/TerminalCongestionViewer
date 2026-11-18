# Using Traffic Intelligence

[Traffic Intelligence](https://bitbucket.org/Nicolas/trafficintelligence/src/master/) (TI) is an open-source library
by Nicolas Saunier (Polytechnique Montréal) that provides analysis scripts for road-user trajectories stored in a
SQLite database. This guide covers installation, converting our Parquet results to the TI format, and the available
TI scripts for counting, safety analysis, and performance evaluation.

---

## Installation

**Option A — pip:**
```powershell
pip install trafficintelligence
```

**Option B — from source** (only needed to edit TI scripts or use the latest development version):
```powershell
hg clone http://nicolassaunier@hg.code.sf.net/p/traffic-intelligence/code C:\Users\<user>\trafficintelligence
cd C:\Users\<user>\trafficintelligence
pip install -r python-requirements.txt

# Add to PYTHONPATH (current session only)
$env:PYTHONPATH = "C:\Users\<user>\trafficintelligence;$env:PYTHONPATH"
```

> **NumPy 2.x compatibility:** TI uses `numpy.NaN`, which was removed in NumPy 2.0.
> Apply this patch before importing TI: `import numpy as np; np.NaN = np.nan`

---

## Exporting trajectories to TI format

TI scripts read from a SQLite database. Use `ti_utils.to_sqlite()` to convert a Parquet trajectory file to
the required schema:

```python
import ti_utils as ti

# VisDrone model
ti.to_sqlite("traj.parquet", "traj.sqlite", class_map=ti.visdrone_to_ti)
```
To go the other way (e.g. to import results from `dltrack.py`):

```python
ti.from_sqlite("traj.sqlite", "traj.parquet")
```

### Class ID mappings

TI uses integer `road_user_type`. The two built-in dicts in `ti_utils.py` map YOLO class IDs to TI codes:

#### `coco_to_ti`

| COCO class | Label        | TI code | TI label      |
|------------|--------------|---------|---------------|
| 0          | person       | 2       | pedestrian    |
| 1          | bicycle      | 4       | cyclist       |
| 2          | car          | 1       | car           |
| 3          | motorcycle   | 3       | motorcyclist  |
| 5          | bus          | 5       | bus           |
| 7          | truck        | 6       | truck         |

#### `visdrone_to_ti`

| VisDrone class | Label              | TI code | TI label      |
|----------------|--------------------|---------|---------------|
| 0              | pedestrian         | 2       | pedestrian    |
| 1              | people             | 2       | pedestrian    |
| 2              | bicycle            | 4       | cyclist       |
| 3              | car                | 1       | car           |
| 4              | van                | 1       | car           |
| 5              | truck              | 6       | truck         |
| 6              | tricycle           | 4       | cyclist       |
| 7              | awning-tricycle    | 4       | cyclist       |
| 8              | bus                | 5       | bus           |
| 9              | motor              | 3       | motorcyclist  |

#### TI road user type codes

| Code | Type         |
|------|--------------|
| 0    | Unknown      |
| 1    | Car          |
| 2    | Pedestrian   |
| 3    | Motorcyclist |
| 4    | Cyclist      |
| 5    | Bus          |
| 6    | Truck        |

---

## TI scripts

All scripts below are in `trafficintelligence/scripts/` and require TI to be installed.


## Tracker: `dltrack.py`

`dltrack.py` is TI's own CLI tracker (YOLO + ByteTrack), writing results directly to SQLite.

**Limitations to be aware of:**
- `imgsz` is fixed at 640 — small objects in drone footage will be missed.
- Only COCO-class models are supported.
- Cyclist merging (IoU-based) is built in, similar to `trajectory.merge_cyclists()`.