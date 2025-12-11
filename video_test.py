# CHANGELOG (current version is latest)
# v1
# v2: changed model from yolov8m to yolo11m

# TODO
# Try out other models and options, find out which one is the best
# Convert detections to coordinates


import cv2
import csv
from ultralytics import YOLO


VIDEO_PATH = "C:/Users/ElricM/Desktop/Lkpg_bus_terminal.avi_trimmed1.mp4"
OUTPUT_CSV = "output/detections.csv"
OUTPUT_FRAMES = "output/frames"

# Use a larger model for accuracy (n = nano, s = small, m = medium)
MODEL_PATH = "yolo11m.pt"

# YOLO COCO class index for "bus"
BUS_CLASS_ID = 5

model = YOLO(MODEL_PATH)

cap = cv2.VideoCapture(VIDEO_PATH)
frame_idx = 0

# Open CSV file for writing results
csv_file = open(OUTPUT_CSV, mode="w", newline="")
writer = csv.writer(csv_file)
writer.writerow([
    "frame",
    "track_id",
    "x1",
    "y1",
    "x2",
    "y2",
    "confidence"
])


while True:
    success, frame = cap.read()
    if not success:
        break

    # Run detection + tracking
    results = model.track(frame, tracker="bytetrack.yaml", persist=True)
    if results[0].boxes is not None:
        for det in results[0].boxes:
            cls = int(det.cls[0])
            if cls != BUS_CLASS_ID:
                continue
            x1, y1, x2, y2 = det.xyxy[0].tolist()
            conf = float(det.conf[0])
            track_id = int(det.id[0]) if det.id is not None else -1

            # Write to CSV
            writer.writerow([
                frame_idx,
                track_id,
                round(x1), round(y1),
                round(x2), round(y2),
                conf
            ])

            # Draw on frame
            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)
            cv2.putText(frame, f"Bus {track_id}", (int(x1), int(y1) - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

    # Save annotated frame
    cv2.imwrite(f"{OUTPUT_FRAMES}/frame_{frame_idx:05d}.jpg", frame)
    frame_idx += 1


csv_file.close()
cap.release()

print("Processing complete.")
