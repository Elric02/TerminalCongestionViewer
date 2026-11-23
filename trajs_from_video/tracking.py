import csv
import cv2
from ultralytics import YOLO
import numpy as np
from pathlib import Path


TERMINAL_SHORT_NAME = "hudd"
PART = "1"
OUTPUT_FOLDER_PATH = Path("../output/trajs_from_video/tracking")
MODEL_PATH = "models/yolov26l-visdrone-dronefreak.pt"
VIDEO_PATH = Path(f"C:/Users/ElricM/OneDrive - VTI/Videos/Huddinge 260612/DJI_20260612071612_0017_D.MP4")
CONF_THRESHOLD = 0.5
IOU_THRESHOLD = 0.7
NB_FRAMES = 600 # Number of frames to process (set to None to process the entire video)


model = YOLO(MODEL_PATH)

# Open video
print(f"Opening video: {VIDEO_PATH}")
cap = cv2.VideoCapture(str(VIDEO_PATH))
if not cap.isOpened():
    raise ValueError(f"Could not open video: {VIDEO_PATH}")

# Video properties
fps = cap.get(cv2.CAP_PROP_FPS)
if fps == 0:
    fps = 29.97  # fallback

width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

frame_idx = 0
unique_track_ids = set()

# Output video path
output_video_path = OUTPUT_FOLDER_PATH / f"{MODEL_PATH.split('/')[-1].split('.')[0]}_{TERMINAL_SHORT_NAME}_part{PART}.mp4"

# Video writer
fourcc = cv2.VideoWriter_fourcc(*"mp4v")
out = cv2.VideoWriter(str(output_video_path), fourcc, fps, (width, height))

# CSV output
csv_path = OUTPUT_FOLDER_PATH / f"{MODEL_PATH.split('/')[-1].split('.')[0]}_{TERMINAL_SHORT_NAME}_part{PART}.csv"
csv_file = open(str(csv_path), mode="w", newline="", encoding="utf-8")
writer = csv.writer(csv_file)
writer.writerow([
    "frame_idx", "time_sec", "track_id",
    "x1", "y1", "x2", "y2", "x_center", "y_center"
])

while NB_FRAMES is None or frame_idx < NB_FRAMES:
    ret, frame = cap.read()
    if not ret:
        break

    frame_idx += 1

    # Track objects
    results = model.track(
        frame,
        persist=True,
        tracker="botsort.yaml",
        conf=CONF_THRESHOLD,
        iou=IOU_THRESHOLD,
        verbose=False
    )

    annotated_frame = results[0].plot()

    if results[0].boxes is not None and results[0].boxes.id is not None:
        track_ids = results[0].boxes.id.cpu().numpy()
        boxes_xyxy = results[0].boxes.xyxy.cpu().numpy()
        time_sec = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0

        for track_id, box in zip(track_ids, boxes_xyxy):
            x1, y1, x2, y2 = box
            x_center = (x1 + x2) / 2
            y_center = (y1 + y2) / 2

            writer.writerow([
                frame_idx, time_sec, int(track_id),
                float(x1), float(y1), float(x2), float(y2),
                float(x_center), float(y_center)
            ])

            unique_track_ids.add(int(track_id))

    cv2.putText(
        annotated_frame,
        f"Track IDs: {len(unique_track_ids)}",
        (10, 30),
        cv2.FONT_HERSHEY_SIMPLEX,
        1,
        (0, 255, 0),
        2
    )

    out.write(annotated_frame)

    # Optional preview every 100 frames
    if frame_idx % 100 == 0:
        print(f"Processed frame {frame_idx}")
        # Uncomment to preview frames in Colab:
        # cv2_imshow(annotated_frame)

# Cleanup
cap.release()
out.release()
csv_file.close()

print("Processing complete.")
print(f"Output video saved to: {output_video_path}")
print(f"CSV saved to: {csv_path}")