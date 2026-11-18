"""
Traffic Intelligence utilities — format conversion between trajectory.py and TI.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Any

# Mirrors parquet_dtype in trajectory.py — keep in sync if columns change.
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

# Mappings from model class IDs to TI road_user_type codes.
coco_to_ti = {
    0: 2,   # person       → pedestrian
    1: 4,   # bicycle      → cyclist
    2: 1,   # car          → car
    3: 3,   # motorcycle   → motorcyclist
    5: 5,   # bus          → bus
    7: 6,   # truck        → truck
}

visdrone_to_ti = {
    0: 2,   # pedestrian       → pedestrian
    1: 2,   # people           → pedestrian
    2: 4,   # bicycle          → cyclist
    3: 1,   # car              → car
    4: 1,   # van              → car
    5: 6,   # truck            → truck
    6: 4,   # tricycle         → cyclist
    7: 4,   # awning-tricycle  → cyclist
    8: 5,   # bus              → bus
    9: 3,   # motor            → motorcyclist
}

ti_class_names = {
    0: "unknown",
    1: "car",
    2: "pedestrian",
    3: "motorcyclist",
    4: "cyclist",
    5: "bus",
    6: "truck",
}


def to_sqlite(traj, output_path, class_map=None):
    """Convert a Parquet trajectory file to a TI-compatible SQLite database.

    Writes the three tables expected by Traffic Intelligence analysis scripts:
        objects          (object_id, road_user_type, n_objects)
        positions        (trajectory_id, frame_number, x_coordinate, y_coordinate)
        objects_features (object_id, trajectory_id)

    Only detected frames are written (interpolated gap rows are excluded).
    class_id values are mapped to TI road_user_type codes using class_map.

    :param traj          [str or Path]  Source Parquet file.
    :param output_path   [str or Path]  Output SQLite database (overwritten if exists).
    :param class_map     [dict | None]  Model class ID → TI road_user_type mapping.
                                        None uses coco_to_ti. Pass visdrone_to_ti for
                                        VisDrone-trained models.

    :return  None
    """
    mapping = coco_to_ti if class_map is None else class_map

    df = pd.read_parquet(traj)
    if "detected" in df.columns:
        df = df[df["detected"]]
    df = df[["object_id", "class_id", "frame", "x", "y"]].copy()
    df["road_user_type"] = df["class_id"].map(mapping).fillna(0).astype("int8")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    con = sqlite3.connect(output_path)
    cur = con.cursor()
    # noinspection SqlNoDataSourceInspection
    cur.execute("CREATE TABLE objects (object_id INTEGER, road_user_type INTEGER, "
                "n_objects INTEGER, PRIMARY KEY(object_id))")
    # noinspection SqlNoDataSourceInspection
    cur.execute("CREATE TABLE positions (trajectory_id INTEGER, frame_number INTEGER, "
                "x_coordinate REAL, y_coordinate REAL, "
                "PRIMARY KEY(trajectory_id, frame_number))")
    # noinspection SqlNoDataSourceInspection
    cur.execute("CREATE TABLE objects_features (object_id INTEGER, trajectory_id INTEGER, "
                "PRIMARY KEY(object_id, trajectory_id))")

    objects = df[["object_id", "road_user_type"]].drop_duplicates("object_id")
    obj_ids: list[int] = [int(v) for v in objects["object_id"].tolist()]  # type: ignore[arg-type]
    obj_ruts: list[int] = [int(v) for v in objects["road_user_type"].tolist()]  # type: ignore[arg-type]
    # noinspection SqlNoDataSourceInspection
    cur.executemany("INSERT INTO objects VALUES (?,?,?)",  # type: ignore[arg-type]
                    [(oid, rut, 1) for oid, rut in zip(obj_ids, obj_ruts)])
    # noinspection SqlNoDataSourceInspection
    cur.executemany("INSERT INTO objects_features VALUES (?,?)",  # type: ignore[arg-type]
                    [(oid, oid) for oid in obj_ids])

    # noinspection SqlNoDataSourceInspection
    cur.executemany(
        "INSERT INTO positions VALUES (?,?,?,?)",
        list(zip(df["object_id"].tolist(), df["frame"].tolist(),
                 df["x"].astype(float).tolist(), df["y"].astype(float).tolist())),
    )
    con.commit()
    con.close()
    print(f"  Exported {len(objects)} objects → {Path(output_path).name}")


def from_sqlite(traj, output_path):
    """Convert a TI-compatible SQLite database to the Parquet trajectory format.

    :param traj         [str or Path]  Source SQLite database (TI schema).
    :param output_path  [str or Path]  Output Parquet file (overwritten if exists).

    :return  None
    """
    con = sqlite3.connect(str(traj))
    # noinspection SqlNoDataSourceInspection
    df = pd.read_sql_query(
        "SELECT ofs.object_id, o.road_user_type AS class_id, "
        "p.frame_number AS frame, p.x_coordinate AS x, p.y_coordinate AS y "
        "FROM positions p "
        "JOIN objects_features ofs ON p.trajectory_id = ofs.trajectory_id "
        "JOIN objects o ON ofs.object_id = o.object_id "
        "ORDER BY ofs.object_id, p.frame_number",
        con,
    )
    con.close()

    df["class_name"]  = df["class_id"].map(ti_class_names).fillna("unknown")
    df["width"]       = float("nan")
    df["height"]      = float("nan")
    df["confidence"]  = float("nan")
    df["class_id_raw"] = pd.array([pd.NA] * len(df), dtype="Int8")

    df = df[list(parquet_dtype.keys())].astype(parquet_dtype)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"  Imported {df['object_id'].nunique()} objects → {output_path.name}")
