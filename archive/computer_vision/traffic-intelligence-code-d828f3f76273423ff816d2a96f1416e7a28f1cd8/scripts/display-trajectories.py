#! /usr/bin/env python3

import sys, argparse
from math import inf

from numpy.linalg import inv
from numpy import loadtxt

from trafficintelligence import storage, cvutils, utils

parser = argparse.ArgumentParser(description='The program displays feature or object trajectories overlaid over the video frames.', epilog = 'Either the configuration filename or the other parameters (at least video and database filenames) need to be provided.')
parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the configuration file')
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file (overrides the configuration file)')
parser.add_argument('-i', dest = 'videoFilename', help = 'name of the video file (overrides the configuration file)')
parser.add_argument('-t', dest = 'trajectoryType', help = 'type of trajectories to display', choices = ['feature', 'object'], default = 'feature')
parser.add_argument('-o', dest = 'homographyFilename', help = 'name of the image to world homography file')
parser.add_argument('--intrinsic', dest = 'intrinsicCameraMatrixFilename', help = 'name of the intrinsic camera file')
parser.add_argument('--distortion-coefficients', dest = 'distortionCoefficients', help = 'distortion coefficients', nargs = '*', type = float)
parser.add_argument('--undistorted-multiplication', dest = 'undistortedImageMultiplication', help = 'undistorted image multiplication', type = float)
parser.add_argument('--undistort', dest = 'undistort', help = 'undistort the video (because features have been extracted that way)', action = 'store_true')
parser.add_argument('-f', dest = 'firstFrameNum', help = 'number of first frame number to display', type = int)
parser.add_argument('-l', dest = 'lastFrameNum', help = 'number of last frame number to save (for image saving, no display is made)', type = int)
parser.add_argument('-r', dest = 'rescale', help = 'rescaling factor for the displayed image', default = 1., type = float)
parser.add_argument('-s', dest = 'nFramesStep', help = 'number of frames between each display', default = 1, type = int)
parser.add_argument('-n', dest = 'nObjects', help = 'number of objects to display', type = int)
parser.add_argument('--save-images', dest = 'saveAllImages', help = 'save all images', action = 'store_true')
parser.add_argument('--nzeros', dest = 'nZerosFilenameArg', help = 'number of digits in filenames', type = int)

args = parser.parse_args()
params, videoFilename, databaseFilename, homography, invHomography, intrinsicCameraMatrix, distortionCoefficients, undistortedImageMultiplication, undistort, firstFrameNum = storage.processVideoArguments(args)

if args.homographyFilename is not None:
    invHomography = inv(loadtxt(args.homographyFilename))
if args.intrinsicCameraMatrixFilename is not None:
    intrinsicCameraMatrix = loadtxt(args.intrinsicCameraMatrixFilename)
if args.distortionCoefficients is not None:
    distortionCoefficients = args.distortionCoefficients
if args.undistortedImageMultiplication is not None:
    undistortedImageMultiplication = args.undistortedImageMultiplication
if args.firstFrameNum is not None:
    firstFrameNum = args.firstFrameNum
if args.lastFrameNum is not None:
    lastFrameNum = args.lastFrameNum
else:
    lastFrameNum = inf
if args.nObjects is not None:
    nObjects = args.nObjects
else:
    nObjects = None

objects = storage.loadTrajectoriesFromSqlite(databaseFilename, args.trajectoryType, nObjects)
boundingBoxes = storage.loadBoundingBoxTableForDisplay(databaseFilename) # not moving.BBMovingObjects, just list of rectangles to display
cvutils.displayTrajectories(videoFilename, objects, boundingBoxes, invHomography, firstFrameNum, lastFrameNum, rescale = args.rescale, nFramesStep = args.nFramesStep, saveAllImages = args.saveAllImages, nZerosFilenameArg = args.nZerosFilenameArg, undistort = (undistort or args.undistort), intrinsicCameraMatrix = intrinsicCameraMatrix, distortionCoefficients = distortionCoefficients, undistortedImageMultiplication = undistortedImageMultiplication)
