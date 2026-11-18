# Workflow

The complete trajectory extraction and evaluation pipeline follows the workflow shown below:

1. Data Preparation
2. Cross Validation
3. Object Detection
4. Object Detection Metrics Evaluation
5. Multi-Object Tracking
6. Tracking Metrics Evaluation
7. Fine-Tuning Detection and Tracking Parameters
8. Trajectory Extraction
9. GNSS Data Collection and Processing
10. Trajectory Evaluation

---

## 1. Data Preparation

* Trim and preprocess drone videos.
* Extract image frames from videos.
* Annotate buses using Roboflow.
* Export annotations in YOLO format.
* Split the dataset into training, validation, and testing sets.

---

## 2. Cross Validation

**Notebook:** `CrossVal.ipynb`

* Perform 5-fold cross-validation.
* Evaluate the impact of dataset size on model performance.
* Compare detection model robustness.

---

## 3. Object Detection

**Notebook:** `Detection_with_matrices.ipynb`

* Train and evaluate object detection models.
* Compare YOLO11, YOLO12, YOLO26 models.
* Select the best-performing detection model.

---

## 4. Object Detection Metrics Evaluation

**Notebook:** `Detection_with_matrices.ipynb`

* Analyse detection performance.
* Calculate Precision, Recall, mAP@50 and mAP@50–95, AP.
* Compare detection models and configurations.

---

## 5. Multi-Object Tracking

**Notebook:** `Tracking.ipynb`

* Apply the BoT-SORT tracker.
* Track buses across consecutive frames.
* Assign unique IDs to detected buses.

---

## 6. Tracking Metrics Evaluation

**Notebook:** `Tracking_Matrices.ipynb`

* Evaluate tracking performance.
* Analyse identity switches, tracking consistency and tracking accuracy.
* Compare different tracking configurations.

---

## 7. Fine-Tuning Detection and Tracking Parameters

* Adjust detection confidence thresholds.
* Adjust tracking parameters.
* Optimise the combined detection-tracking pipeline.

---

## 8. Trajectory Extraction
**Notebook:** `Homography.ipynb` `Homography_dynamic.ipynb`

* Convert tracking outputs into trajectory data.
* Use the centre point of bounding boxes as vehicle positions.
* Remove false positives and trajectory outliers.
* Convert those trajectories from pixel coordinate to Real world coordinate thorough Homography
* Static Homography has been used for balsta dataset (`Homography.ipynb`) and dynamic homography for linkoping dataset (`Homography_dynamic.ipynb`)

---

## 9. GNSS Data Collection and Processing


* Collect GNSS reference trajectories.
* Clean and process GNSS data.


---

## 10. Trajectory Evaluation

**Notebook:** `Trajectory_Analysis.ipynb`

* Prepare reference trajectories for comparison.
* Compare extracted trajectories with GNSS trajectories.
* Visualise trajectories in QGIS.


---
## 11. Trajectory Differences

**Notebook:** `Trajectory_Difference_Analysis.ipynb`

* Time-align manual, drone-based, and GNSS trajectories.
* Compare trajectory positions over a common time interval.
* Calculate distance-based errors to evaluate trajectory accuracy.


