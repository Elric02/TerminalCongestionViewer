import vidtraf_tracking_master.video_utils as video_utils
import vidtraf_tracking_master.homography_tools as ht
import pandas as pd
import numpy as np


VIDEO_PATH = "C:/Users/ElricM/OneDrive - VTI/Videos/Lkpg_bus_terminal_stabilized_part1.mp4"
FRAME = 0

video_utils.get_frame(VIDEO_PATH, "C:/Users/ElricM/Work Folders/Documents/GitHub/TerminalCongestionViewer/trajs_from_video/frame_0.png", FRAME)
#video_utils.stabilize_video(VIDEO_PATH, f"output/videos/Lkpg_bus_terminal_stabilized.mp4", FRAME, detector_name="rsift", max_features=6000, downsample_ratio=1.0, filter_ratio=0.75, min_inliers=25)


# Tentatives to use Guillermo's code

PART = 1
TRACKS_PATH = f"C:/Users/ElricM/Work Folders/Documents/GitHub/TerminalCongestionViewer/trajs_from_video/Bus_Trajectory-main/notebooks/modules/Tracking/runs/track_lkpg/lkpg_full_bbox_centers_xyxy_yolo12s_part{PART}.csv"
TIF_PATH = "C:/Users/ElricM/Work Folders/Documents/GitHub/TerminalCongestionViewer/trajs_from_video/test_ortofoto_lkpg/647_53_5050_2023_clipped.tif"
EXPORT_ORTHO_PARAMS_PATH = "C:/Users/ElricM/Work Folders/Documents/GitHub/TerminalCongestionViewer/trajs_from_video/output/ortho_params.txt"
OUTPUT_COORDS_PATH = f"C:/Users/ElricM/Work Folders/Documents/GitHub/TerminalCongestionViewer/trajs_from_video/output/lonlat_part{PART}.csv"

df = pd.read_csv(TRACKS_PATH)
pixels = df[['x_center', 'y_center']].values.astype(np.float32)
#ht.read_ortho_params(TIF_PATH, EXPORT_ORTHO_PARAMS_PATH)

#output = ht.pixel_to_lonlat(pixels, EXPORT_ORTHO_PARAMS_PATH)
#output = [[float(lon), float(lat)] for lon, lat in output]
#pd.DataFrame(output).to_csv(OUTPUT_COORDS_PATH, index=False)