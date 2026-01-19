#! /usr/bin/env python

import numpy as np, cv2
import argparse, os
from pandas import read_csv
from matplotlib.pyplot import imshow, figure

from trafficintelligence import cvutils, moving, ml, storage

parser = argparse.ArgumentParser(description='The program extracts labeled image patches to train the HoG-SVM classifier, and optionnally speed information')
parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the configuration file', required = True)
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file (overrides the configuration file)')
parser.add_argument('-i', dest = 'videoFilename', help = 'name of the video file (overrides the configuration file)')
parser.add_argument('--gt', dest = 'classificationAnnotationFilename', help = 'name of the file containing the correct classes (user types)', required = True)
parser.add_argument('--delimiter', dest = 'classificationAnnotationFilenameDelimiter', help = 'delimiter for the fields in the correct classification file', default= ' ')
parser.add_argument('-s', dest = 'nFramesStep', help = 'number of frames between each saved patch', default = 50, type = int)
parser.add_argument('-n', dest = 'nObjects', help = 'number of objects to use to extract patches from', type = int, default = None)
parser.add_argument('--start-frame0', dest = 'startFrame0', help = 'starts with first frame for videos with index problem where frames cannot be reached', action = 'store_true')
parser.add_argument('-o', dest = 'overlap', help = 'maximum intersection over union of the features nFramesStep apart to save image', type = float, default = 0.2)
parser.add_argument('--extract-all', dest = 'extractAllObjectImages', help = 'extracts the images for all objects, well classified or not (otherwise, extracts only for the misclassified)', action = 'store_true')
parser.add_argument('--prefix', dest = 'imagePrefix', help = 'image prefix', default = 'img')
parser.add_argument('--ouput', dest = 'directoryName', help = 'parent directory name for the directories containing the samples for the different road users', default = '.')
parser.add_argument('--compute-speed-distributions', dest = 'computeSpeedDistribution', help = 'computes the distribution of the road users of each type and fits parameters to each', action = 'store_true')

args = parser.parse_args()
params, videoFilename, databaseFilename, homography, invHomography, intrinsicCameraMatrix, distortionCoefficients, undistortedImageMultiplication, undistort, firstFrameNum = storage.processVideoArguments(args)
classifierParams = storage.ClassifierParameters(params.classifierFilename)

classificationAnnotations = read_csv(args.classificationAnnotationFilename, index_col=0, delimiter = args.classificationAnnotationFilenameDelimiter, names = ["object_num", "road_user_type"])
annotatedObjectNumbers = classificationAnnotations.index.tolist()

# objects has the objects for which we want to extract labeled images
if args.extractAllObjectImages:
    objects = storage.loadTrajectoriesFromSqlite(databaseFilename, 'object', args.nObjects, withFeatures = True)
else:
    if len(annotatedObjectNumbers) > args.nObjects:
        classificationAnnotations = classificationAnnotations[:args.nObjects]
        annotatedObjectNumbers = classificationAnnotations.index.tolist()
    objects = storage.loadTrajectoriesFromSqlite(databaseFilename, 'object', annotatedObjectNumbers, withFeatures = True)
for obj in objects:
    if obj.getNum() in annotatedObjectNumbers:
        obj.setUserType(classificationAnnotations.loc[obj.getNum(), 'road_user_type'])
timeInterval = moving.TimeInterval.unionIntervals([obj.getTimeInterval() for obj in objects])
if args.startFrame0:
    timeInterval.first = 0

for userType in classificationAnnotations['road_user_type'].unique():
    if not os.path.exists(args.directoryName+os.sep+moving.userTypeNames[userType]):
        os.mkdir(args.directoryName+os.sep+moving.userTypeNames[userType])

capture = cv2.VideoCapture(videoFilename)
width = int(capture.get(cv2.cv.CV_CAP_PROP_FRAME_WIDTH))
height = int(capture.get(cv2.cv.CV_CAP_PROP_FRAME_HEIGHT))

if undistort: # setup undistortion
    [map1, map2] = cvutils.computeUndistortMaps(width, height, undistortedImageMultiplication, intrinsicCameraMatrix, distortionCoefficients)
    height, width = map1.shape

if capture.isOpened():
    ret = True
    frameNum = timeInterval.first
    if not args.startFrame0:
        capture.set(cv2.cv.CV_CAP_PROP_POS_FRAMES, frameNum)
    lastFrameNum = timeInterval.last
    while ret and frameNum <= timeInterval.last:
        ret, img = capture.read()
        distorted = True
        if ret:
            if frameNum%50 == 0:
                print('frame number: {}'.format(frameNum))
            for obj in objects[:]:
                if obj.existsAtInstant(frameNum):
                    if (10+frameNum-obj.getFirstInstant())%args.nFramesStep == 0:
                        currentImageFeatures = set([f.num for f in obj.getFeatures() if f.existsAtInstant(frameNum)])
                        if not hasattr(obj, 'lastImageFeatures') or len(currentImageFeatures.intersection(obj.lastImageFeatures))/len(currentImageFeatures.union(obj.lastImageFeatures)) < args.overlap:
                            obj.lastImageFeatures = currentImageFeatures
                            if undistort and distorted: # undistort only if necessary
                                img = cv2.remap(img, map1, map2, interpolation=cv2.INTER_LINEAR)
                                distorted = False
                            croppedImg = cvutils.imageBox(img, obj, frameNum, invHomography, width, height, classifierParams.percentIncreaseCrop, classifierParams.percentIncreaseCrop, classifierParams.minNPixels)
                            if croppedImg is not None:
                                cv2.imwrite(args.directoryName+os.sep+moving.userTypeNames[obj.getUserType()]+os.sep+args.imagePrefix+'-{}-{}.png'.format(obj.getNum(), frameNum), croppedImg)
                    elif obj.getLastInstant() == frameNum:
                        objects.remove(obj)
        frameNum += 1

# todo speed info: distributions AND min speed equiprobable

