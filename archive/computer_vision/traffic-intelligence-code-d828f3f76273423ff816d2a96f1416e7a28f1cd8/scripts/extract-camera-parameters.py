#! /usr/bin/env python3

import argparse

from trafficintelligence import storage, cvutils

parser = argparse.ArgumentParser(description='The program extracts the intrinsic camera from the tacal camera calibration file used by T-Analyst (http://www.tft.lth.se/en/research/video-analysis/co-operation/software/t-analyst/).')
parser.add_argument('-i', dest = 'filename', help = 'filename of the camera calibration (.tacal)', required = True)
parser.add_argument('-o', dest = 'outputIntrinsicFilename', help = 'filename of the intrinsic camera matrix', default = 'intrinsic-camera.txt')

args = parser.parse_args()

cameraData = storage.loadPinholeCameraModel(args.filename, True)
if cameraData is not None:
    from numpy import savetxt
    intrinsicCameraMatrix = cvutils.getIntrinsicCameraMatrix(cameraData)
    distortionCoefficients = cvutils.getDistortionCoefficients(cameraData)
    savetxt(args.outputIntrinsicFilename, intrinsicCameraMatrix)
    print(distortionCoefficients)
