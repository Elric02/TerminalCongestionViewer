
# Methodology

## Overview

The objective of this project is to extract bus trajectories from drone videos using computer vision techniques and compare the extracted trajectories with GNSS reference data.

The methodology consists of six main stages:

1. Data Preparation
2. Cross Validation
3. Object Detection
4. Multi-Object Tracking
5. Trajectory Extraction
6. Trajectory Evaluation

---

## 1. Data Preparation

Drone videos collected at Bålsta Bus Terminal and Linköping Bus Terminal were processed to create datasets for object detection.

The data preparation process included:

* Trimming the Linköping video to remove segments affected by camera jitter.
* Extracting image frames from the drone videos.
* Annotating bus objects using the Roboflow annotation platform.
* Splitting the annotated dataset into training, validation, and testing subsets.
* Exporting annotations in YOLO format for model training and evaluation.

---

## 2. Cross Validation

To investigate the impact of dataset size on detection performance, a 5-fold cross-validation procedure was conducted separately for each study area.

The complete annotated dataset of each bus terminal was divided into five folds. During each iteration, four folds were used for training and one fold was used for validation.

The cross-validation results were used to assess the robustness and generalisation capability of the detection models.

---

## 3. Object Detection

Several YOLO-based object detection models were evaluated, including:

* YOLO11 (n, s, and m variants)
* YOLO12 (n, s, and m variants)
* YOLO26 (n, s, and m variants)

In addition, a Mask R-CNN model was implemented for comparison.

The detection models were evaluated using standard performance metrics, including:

* Precision
* Recall
* mAP@50
* mAP@50–95
* AP
* IOU

The best-performing model for each dataset was selected for subsequent trajectory extraction and tracking tasks.

---

## 4. Multi-Object Tracking

The BoT-SORT tracking algorithm was used to track buses across consecutive video frames.

BoT-SORT was selected due to its ability to:

* Maintain object identities during partial occlusions.
* Reduce identity switching.
* Provide stable tracking performance across multiple frames.

The tracker assigned a unique identifier to each detected bus, enabling the reconstruction of vehicle trajectories over time. 

---

## 5. Trajectory Extraction

Tracking outputs were converted into trajectory data.

The centre point of each detected bounding box was used as the representative bus position for each frame.

The generated trajectories were subsequently cleaned through a post-processing procedure, including:

* Removal of false positive detections.
* Removal of tracking outliers.
* Filtering of unrealistic trajectory segments.

The resulting trajectories were used for trajectory evaluation and comparison.

---

## 6. Trajectory Evaluation

The extracted bus trajectories were compared with GNSS reference trajectories.

The comparison was performed to assess the quality and reliability of the computer vision-based trajectory extraction process.

The resulting trajectories were visualised and analysed in QGIS, enabling a spatial comparison between the tracked trajectories and the corresponding GNSS trajectories.

The evaluation results provide insights into the accuracy and applicability of drone-based trajectory extraction for bus operations analysis.
