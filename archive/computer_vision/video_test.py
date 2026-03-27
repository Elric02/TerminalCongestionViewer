# CHANGELOG (current version is latest)
# v1
# v2: changed model from yolov8m to yolo11m
# v3: changed model from yolo11m to yolov11l
#
# To refine the model: https://docs.ultralytics.com/modes/track/#tracker-arguments


import cv2
import csv
from ultralytics import YOLO
import numpy as np


VIDEO_PATH = "C:/Users/ElricM/OneDrive - VTI/Videos/Lkpg_bus_terminal.avi_trimmed1.mp4"
OUTPUT_CSV = "output/detections.csv"
OUTPUT_FRAMES = "output/frames"
MODEL_PATH = "yolo11l.pt"
# YOLO COCO class index for "bus"
BUS_CLASS_ID = 5

# GROUND CONTROL POINTS (pixel positions → real world positions)
image_points = np.array([
    [516, 1326],
    [1244, 1883],
    [3258, 746],
    [2535, 437]
], dtype=np.float32)
world_points = np.array([
    [15.623658829077737, 58.41736108378279],
    [15.623336442553528, 58.41719626067784],
    [15.624275457270862, 58.41670044668837],
    [15.624602961041484, 58.4168652721133]
], dtype=np.float32)

# Compute homography matrix
H, _ = cv2.findHomography(image_points, world_points)


# Video and model initialization
model = YOLO(MODEL_PATH)
cap = cv2.VideoCapture(VIDEO_PATH)
frame_idx = 0

# Open CSV file for writing results
csv_file = open(OUTPUT_CSV, mode="w", newline="")
writer = csv.writer(csv_file)
writer.writerow([
    "frame",
    "track_id",
    "world_x_m",
    "world_y_m",
    "pixel_x",
    "pixel_y",
    "x1",
    "y1",
    "x2",
    "y2",
    "confidence"
])


# Main loop
while True:
    success, frame = cap.read()
    if not success:
        break

    # Run detection + tracking
    results = model.track(frame, tracker="bytetrack.yaml", persist=True)
    if results[0].boxes is not None:
        for det in results[0].boxes:
            cls = int(det.cls[0])
            # Only write down something if the detected object is a bus
            if cls != BUS_CLASS_ID:
                continue
            x1, y1, x2, y2 = det.xyxy[0].tolist()
            conf = float(det.conf[0])
            track_id = int(det.id[0]) if det.id is not None else -1

            # Compute bounding box center (pixel coordinates)
            cx = (x1 + x2) / 2.0
            cy = (y1 + y2) / 2.0
            # Convert to homography input shape: (1,1,2)
            pixel_pt = np.array([[[cx, cy]]], dtype=np.float32)

            # Transform pixel center → world (meters)
            lonlat = cv2.perspectiveTransform(pixel_pt, H)[0][0]
            longitude = float(lonlat[0])
            latitude = float(lonlat[1])

            # Write to CSV
            writer.writerow([
                frame_idx,
                track_id,
                longitude,
                latitude,
                cx,
                cy,
                round(x1), round(y1),
                round(x2), round(y2),
                conf
            ])

            # Draw on frame
            cv2.putText(
                frame, f"Bus {track_id} ({longitude:.6f}, {latitude:.6f})",
                (int(x1), int(y1) - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2
            )
            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)

    # Save annotated frame
    cv2.imwrite(f"{OUTPUT_FRAMES}/frame_{frame_idx:05d}.jpg", frame)
    frame_idx += 1


csv_file.close()
cap.release()
print("Processing complete.")
