#! /usr/bin/env python3

import sys, argparse

import numpy as np
import cv2

from trafficintelligence import cvutils
from math import ceil, log10
from pathlib import Path

parser = argparse.ArgumentParser(description='''The program converts a video into a series of images corrected for distortion. One can then use mencoder to generate a movie, eg
$ mencoder 'mf://./*.png' -mf fps=[framerate]:type=png -ovc xvid -xvidencopts bitrate=[bitrate] -nosound -o [output.avi]''')

parser.add_argument('-i', dest = 'videoFilename', help = 'filename of the video sequence')
parser.add_argument('--intrinsic', dest = 'intrinsicCameraMatrixFilename', help = 'name of the intrinsic camera file')
parser.add_argument('--distortion-coefficients', dest = 'distortionCoefficients', help = 'distortion coefficients', nargs = '*', type = float)
parser.add_argument('--undistorted-multiplication', dest = 'undistortedImageMultiplication', help = 'undistorted image multiplication', type = float, default = 1.)
parser.add_argument('-k', dest = 'maskFilename', help = 'name of the mask file, to undistort to see how it covers the undistortion errors')
parser.add_argument('-f', dest = 'firstFrameNum', help = 'number of first frame number to display', type = int, default = 0)
parser.add_argument('-l', dest = 'lastFrameNum', help = 'number of last frame number to save', type = int)
parser.add_argument('-d', dest = 'destinationDirname', help = 'name of the directory where the undistorted frames are saved')
parser.add_argument('--encode', dest = 'encodeVideo', help = 'indicate if video is generated at the end (default Xvid)', action = 'store_true')
parser.add_argument('--fps', dest = 'fps', help = 'frame per second of the output video file if encoding', type = float, default = 30)
parser.add_argument('--bitrate', dest = 'bitrate', help = 'bitrate of the output video file if encoding', type = int, default = 5000)

args = parser.parse_args()

intrinsicCameraMatrix = np.loadtxt(args.intrinsicCameraMatrixFilename)
if args.destinationDirname is None:
    destinationPath = Path('.')
else:
    destinationPath = Path(destinationPath)
    if not destinationPath.exists():
        destinationPath.mkdir()

capture = cv2.VideoCapture(args.videoFilename)
width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
[map1, map2], newCameraMatrix = cvutils.computeUndistortMaps(width, height, args.undistortedImageMultiplication, intrinsicCameraMatrix, args.distortionCoefficients)
if args.maskFilename is not None:
    mask = cv2.imread(args.maskFilename, cv2.IMREAD_GRAYSCALE)
    undistortedMask = cv2.remap(mask, map1, map2, interpolation=cv2.INTER_LINEAR)/255

if capture.isOpened():
    ret = True
    frameNum = args.firstFrameNum
    capture.set(cv2.CAP_PROP_POS_FRAMES, args.firstFrameNum)
    if args.lastFrameNum is None:
        lastFrameNum = float('inf')
    else:
        lastFrameNum = args.lastFrameNum
    nZerosFilename = int(ceil(log10(lastFrameNum)))
    while ret and frameNum < lastFrameNum:
        ret, img = capture.read()
        if ret:
            img = cv2.remap(img, map1, map2, interpolation=cv2.INTER_LINEAR)
            cv2.imwrite(str(destinationPath/Path('undistorted-{{:0{}}}.png'.format(nZerosFilename).format(frameNum))), img)
            if args.maskFilename is not None:
                cv2.imwrite(str(destinationPath/Path('undistorted+mask-{{:0{}}}.png'.format(nZerosFilename).format(frameNum))), cv2.multiply(img, cv2.merge([undistortedMask]*3), dtype = 16))
        frameNum += 1

if args.encodeVideo:
    print('Encoding the images files in video')
    from subprocess import check_call
    from storage import openCheck
    out = openCheck("err.log", "w")
    check_call("mencoder \'mf://"+destinationPath+"*.png\' -mf fps={}:type=png -ovc xvid -xvidencopts bitrate={} -nosound -o ".format(args.fps, args.bitrate)+destinationDirname+"undistort.avi", stderr = out, shell = True)
    out.close()
