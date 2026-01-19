#! /usr/bin/env python3

import sys, argparse

import matplotlib
matplotlib.use('TkAgg')

import matplotlib.pyplot as plt
import numpy as np
import cv2

from trafficintelligence import cvutils, utils, storage

# TODO add option to use RANSAC or other robust homography estimation method?

parser = argparse.ArgumentParser(description='The program computes the homography matrix from at least 4 non-colinear point correspondences inputed in the same order in a video frame and a aerial photo/ground map, or from the list of corresponding points in the two planes.', epilog = '''The point correspondence file contains at least 4 non-colinear point coordinates 
with the following format:
 - the first two lines are the x and y coordinates in the projected space (usually world space)
 - the last two lines are the x and y coordinates in the origin space (usually image space)

Image space is in ideal point space (no camera) if undistorting the camera

If providing video and world images, with a number of points to input
and a ratio to convert pixels to world distance unit (eg meters per pixel), 
the images will be shown in turn and the user should click 
in the same order the corresponding points in world and image spaces.''', formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('-p', dest = 'pointCorrespondencesFilename', help = 'name of the text file containing the point correspondences')
parser.add_argument('--tsai', dest = 'tsaiCameraFilename', help = 'name of the text file containing the camera parameter following the pinhole camera model (Lund format)') # caution, this is Aliaksei's format
parser.add_argument('-i', dest = 'videoFrameFilename', help = 'filename of the video frame')
parser.add_argument('-w', dest = 'worldFilename', help = 'filename of the aerial photo/ground map')
parser.add_argument('-n', dest = 'nPoints', help = 'number of corresponding points to input', default = 4, type = int)
parser.add_argument('-u', dest = 'unitsPerPixel', help = 'number of units per pixel', default = 1., type = float)
parser.add_argument('-o', dest = 'homographyFilename', help = 'filename of the homography matrix', default = 'homography.txt')
parser.add_argument('--display', dest = 'displayPoints', help = 'display original and projected points on both images', action = 'store_true')
parser.add_argument('--intrinsic', dest = 'intrinsicCameraMatrixFilename', help = 'name of the intrinsic camera file')
parser.add_argument('--distortion-coefficients', dest = 'distortionCoefficients', help = 'distortion coefficients', nargs = '*', type = float)
parser.add_argument('--undistorted-multiplication', dest = 'undistortedImageMultiplication', help = 'undistorted image multiplication', type = float, default = 1.)
parser.add_argument('--undistort', dest = 'undistort', help = 'undistort the video (because features have been extracted that way)', action = 'store_true')
parser.add_argument('--correspondences-imagepoints-in-image-space', dest = 'correspondencesImagePointsInImageSpace', help = 'if image points are provided in the actual image space (before undistortion)', action = 'store_true')
parser.add_argument('--save', dest = 'saveImages', help = 'save the undistorted video frame (display option must be chosen)', action = 'store_true')

args = parser.parse_args()

homography = np.array([])
if args.pointCorrespondencesFilename is not None:
    worldPts, videoPts = cvutils.loadPointCorrespondences(args.pointCorrespondencesFilename)
    if args.correspondencesImagePointsInImageSpace:
        videoPts = cv2.undistortPoints(videoPts.T, np.loadtxt(args.intrinsicCameraMatrixFilename), np.array(args.distortionCoefficients)).reshape(-1,2)
    homography, mask = cv2.findHomography(videoPts, worldPts) # method=0, ransacReprojThreshold=3
elif args.tsaiCameraFilename is not None: # hack using PDTV
    from pdtv import TsaiCamera
    cameraData = storage.loadPinholeCameraModel(args.tsaiCameraFilename)
    camera = TsaiCamera(Cx=cameraData['Cx'], Cy=cameraData['Cy'], Sx=cameraData['Sx'], Tx=cameraData['Tx'], Ty=cameraData['Ty'], Tz=cameraData['Tz'], dx=cameraData['dx'], dy=cameraData['dy'], f=cameraData['f'], k=cameraData['k'], r1=cameraData['r1'], r2=cameraData['r2'], r3=cameraData['r3'], r4=cameraData['r4'], r5=cameraData['r5'], r6=cameraData['r6'], r7=cameraData['r7'], r8=cameraData['r8'], r9=cameraData['r9'])
    homography = cvutils.computeHomographyFromPDTV(camera)
elif args.videoFrameFilename is not None and args.worldFilename is not None:
    worldImg = plt.imread(args.worldFilename)
    videoImg = plt.imread(args.videoFrameFilename)
    if args.undistort:        
        [map1, map2], newCameraMatrix = cvutils.computeUndistortMaps(videoImg.shape[1], videoImg.shape[0], args.undistortedImageMultiplication, np.loadtxt(args.intrinsicCameraMatrixFilename), args.distortionCoefficients)
        videoImg = cv2.remap(videoImg, map1, map2, interpolation=cv2.INTER_LINEAR)
    print('Click on {} points in the video frame'.format(args.nPoints))
    plt.ion()
    plt.figure()
    plt.imshow(videoImg)
    plt.tight_layout()
    videoPts = np.array(plt.ginput(args.nPoints, timeout=3000))
    if args.undistort:
        videoPts = cvutils.newCameraProject(videoPts.T, np.linalg.inv(newCameraMatrix)).T
    print('Click on {} points in the world image'.format(args.nPoints))
    plt.figure()
    plt.imshow(worldImg)
    plt.tight_layout()
    worldPts = args.unitsPerPixel*np.array(plt.ginput(args.nPoints, timeout=3000))
    plt.close('all')
    homography, mask = cv2.findHomography(videoPts, worldPts)
    # save the points in file
    f = open('point-correspondences.txt', 'a')
    np.savetxt(f, worldPts.T)
    np.savetxt(f, videoPts.T)
    f.close()

if homography.size>0:
    np.savetxt(args.homographyFilename,homography)

if args.displayPoints and args.videoFrameFilename is not None and args.worldFilename is not None and homography.size>0 and args.tsaiCameraFilename is None:
    worldImg = cv2.imread(args.worldFilename)
    videoImg = cv2.imread(args.videoFrameFilename)
    if args.undistort:
        [map1, map2], newCameraMatrix = cvutils.computeUndistortMaps(videoImg.shape[1], videoImg.shape[0], args.undistortedImageMultiplication, np.loadtxt(args.intrinsicCameraMatrixFilename), args.distortionCoefficients)
        videoImg = cv2.remap(videoImg, map1, map2, interpolation=cv2.INTER_LINEAR)
        if args.saveImages:
            cv2.imwrite(utils.removeExtension(args.videoFrameFilename)+'-undistorted.png', videoImg)
    invHomography = np.linalg.inv(homography)
    projectedWorldPts = cvutils.homographyProject(worldPts.T, invHomography).T
    projectedVideoPts = cvutils.homographyProject(videoPts.T, homography).T
    if args.undistort:
        projectedWorldPts = cvutils.newCameraProject(projectedWorldPts.T, newCameraMatrix).T
        videoPts = cvutils.newCameraProject(videoPts.T, newCameraMatrix).T
    for i in range(worldPts.shape[0]):
        # world image
        cv2.circle(worldImg,tuple(np.int32(np.round(worldPts[i]/args.unitsPerPixel))),2,cvutils.cvBlue['default'])
        cv2.circle(worldImg,tuple(np.int32(np.round(projectedVideoPts[i]/args.unitsPerPixel))),2,cvutils.cvRed['default'])
        cv2.putText(worldImg, str(i+1), tuple(np.int32(np.round(worldPts[i]/args.unitsPerPixel))+5), cv2.FONT_HERSHEY_PLAIN, 2., cvutils.cvBlue['default'], 2)
        # video image
        cv2.circle(videoImg,tuple(np.int32(np.round(videoPts[i]))),2,cvutils.cvBlue['default'])
        cv2.circle(videoImg,tuple(np.int32(np.round(projectedWorldPts[i]))),2,cvutils.cvRed['default'])
        cv2.putText(videoImg, str(i+1), tuple(np.int32(np.round(videoPts[i])+5)), cv2.FONT_HERSHEY_PLAIN, 2., cvutils.cvBlue['default'], 2)
    cv2.imshow('video frame',videoImg)
    cv2.imshow('world image',worldImg)
    cv2.waitKey()
    cv2.destroyAllWindows()
