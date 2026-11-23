import video_utils as video_utils



# VIDEO STABILIZATION

TERMINAL_SHORT_NAME = "hudd"
PART = 2
ORIGINAL_VIDEO_PATH = "C:/Users/ElricM/OneDrive - VTI/Videos/Huddinge 260612/DJI_20260612073437_0018_D.MP4"

FRAME = 0
OUTPUT_FRAME = f"../output/trajs_from_video/{TERMINAL_SHORT_NAME}_frame_{FRAME}_part{PART}.png"
OUTPUT_VIDEO_PATH = f"../output/trajs_from_video/videos/{TERMINAL_SHORT_NAME}_stabilized_part{PART}.mp4"


video_utils.get_frame(ORIGINAL_VIDEO_PATH, OUTPUT_FRAME, FRAME)
video_utils.stabilize_video(ORIGINAL_VIDEO_PATH, OUTPUT_VIDEO_PATH, FRAME, detector_name="rsift", max_features=6000, downsample_ratio=1.0, filter_ratio=0.75, min_inliers=25)
