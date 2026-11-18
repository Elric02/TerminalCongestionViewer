# Dataset Description

## Study Areas

This project focuses on two bus terminals in Sweden:

1. Bålsta Bus Terminal
2. Linköping Bus Terminal

## Data Collection

Drone videos were collected to capture vehicle movements within the study areas. The videos were later processed to extract vehicle trajectories using computer vision techniques.

### Bålsta Bus Terminal

* Recording date: 12 March 2026
* Video duration: Approximately 20 minutes

### Linköping Bus Terminal

* Recording date: 22 March 2022
* Video duration: Approximately 13 minutes

## Data Preparation

The collected videos were converted into image frames for object detection model development.

The annotation process consisted of:

1. Extracting image frames from the drone videos.
2. Manually annotating bus objects using the RoboFlow annotation platform.
3. Exporting the annotations in YOLO format for model training and evaluation.

## Purpose

The annotated dataset was used to:

* Train YOLO-based object detection models.
* Detect buses within the video recordings.
* Generate vehicle trajectories through multi-object tracking.
* Evaluate extracted trajectories against reference trajectory data.





