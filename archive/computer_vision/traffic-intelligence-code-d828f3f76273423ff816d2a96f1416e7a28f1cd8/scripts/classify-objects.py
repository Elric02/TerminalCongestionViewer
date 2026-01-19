#! /usr/bin/env python3

import sys, argparse

import numpy as np
import cv2
from scipy.stats import norm, lognorm
from pathlib import Path

try:
    from ultralytics import YOLO
    ultralyticsAvailable = True
except ImportError:
    print('Ultralytics library could not be loaded') # TODO change to logging module
    ultralyticsAvailable = False


from trafficintelligence import cvutils, moving, ml, storage, utils

# TODO add mode detection live, add choice of kernel and svm type (to be saved in future classifier format)

parser = argparse.ArgumentParser(description='The program processes indicators for all pairs of road users in the scene', epilog='The integer ids for the categories are stored in the moving module:\n{}'.format(moving.userType2Num))
parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the configuration file', required = True)
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file (overrides the configuration file)')
parser.add_argument('-i', dest = 'videoFilename', help = 'name of the video file (overrides the configuration file)')
parser.add_argument('-n', dest = 'nObjects', help = 'number of objects to classify', type = int, default = None)
parser.add_argument('--plot-speed-distributions', dest = 'plotSpeedDistribution', help = 'simply plots the distributions used for each user type', action = 'store_true')
parser.add_argument('--max-speed-distribution-plot', dest = 'maxSpeedDistributionPlot', help = 'if plotting the user distributions, the maximum speed to display (km/h)', type = float, default = 50.)
parser.add_argument('--verbose', dest = 'verbose', help = 'verbose information', action = 'store_true')

args = parser.parse_args()
params, videoFilename, databaseFilename, homography, invHomography, intrinsicCameraMatrix, distortionCoefficients, undistortedImageMultiplication, undistort, firstFrameNum = storage.processVideoArguments(args)
classifierParams = storage.ClassifierParameters(params.classifierFilename)
classifierParams.convertToFrames(params.videoFrameRate, 3.6) # conversion from km/h to m/frame

speedAggregationFunc = utils.aggregationFunction(classifierParams.speedAggregationMethod, classifierParams.speedAggregationCentile)
if speedAggregationFunc is None:
    sys.exit()

if ultralyticsAvailable and Path(classifierParams.dlFilename).is_file(): # use Yolo
    pedBikeCarSVM = None
    bikeCarSVM = None
    yolo = YOLO(classifierParams.dlFilename, task='detect')
    useYolo = True
    print('Using Yolov8 model '+classifierParams.dlFilename)
else:
    useYolo = False
    pedBikeCarSVM = ml.SVM_load(classifierParams.pedBikeCarSVMFilename)
    bikeCarSVM = ml.SVM_load(classifierParams.bikeCarSVMFilename)

# log logistic for ped and bik otherwise ((pedBeta/pedAlfa)*((sMean/pedAlfa)**(pedBeta-1)))/((1+(sMean/pedAlfa)**pedBeta)**2.)
carNorm = norm(classifierParams.meanVehicleSpeed, classifierParams.stdVehicleSpeed)
pedNorm = norm(classifierParams.meanPedestrianSpeed, classifierParams.stdPedestrianSpeed)
# numpy lognorm shape, loc, scale: shape for numpy is scale (std of the normal) and scale for numpy is exp(location) (loc=mean of the normal)
cycLogNorm = lognorm(classifierParams.scaleCyclistSpeed, loc = 0., scale = np.exp(classifierParams.locationCyclistSpeed))
speedProbabilities = {moving.userType2Num['car']: lambda s: carNorm.pdf(s),
                      moving.userType2Num['pedestrian']: lambda s: pedNorm.pdf(s), 
                      moving.userType2Num['cyclist']: lambda s: cycLogNorm.pdf(s)}
if useYolo: # consider other user types
    for i in [3, 5, 6, 7]:
        speedProbabilities[i] = speedProbabilities[moving.userType2Num['car']]

if args.plotSpeedDistribution:
    import matplotlib.pyplot as plt
    plt.figure()
    speeds = np.arange(0.1, args.maxSpeedDistributionPlot, 0.1)
    for k in speedProbabilities:
        plt.plot(speeds, [speedProbabilities[k](s/(3.6*params.videoFrameRate)) for s in speeds], label = k) # the distribution parameters are in video intrinsic units, unit of distance per frame
    maxProb = -1.
    for k in speedProbabilities:
        maxProb = max(maxProb, np.max([speedProbabilities[k](s/(3.6*params.videoFrameRate)) for s in speeds]))
    plt.plot([classifierParams.minSpeedEquiprobable*3.6*params.videoFrameRate]*2, [0., maxProb], 'k-')
    plt.text(classifierParams.minSpeedEquiprobable*3.6*params.videoFrameRate, maxProb, 'threshold for equiprobable class')
    plt.xlabel('Speed (km/h)')
    plt.ylabel('Probability density function')
    plt.legend()
    #plt.title('Probability Density Function')
    plt.show()
    sys.exit()

objects = storage.loadTrajectoriesFromSqlite(databaseFilename, 'object', args.nObjects, withFeatures = True)
timeInterval = moving.TimeInterval.unionIntervals([obj.getTimeInterval() for obj in objects])

capture = cv2.VideoCapture(videoFilename)
width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

#if undistort: # setup undistortion
#     [map1, map2] = cvutils.computeUndistortMaps(width, height, undistortedImageMultiplication, intrinsicCameraMatrix, distortionCoefficients)
#     height, width = map1.shape
#    newImgSize = (int(round(width*undistortedImageMultiplication)), int(round(height*undistortedImageMultiplication)))
#    newCameraMatrix = cv2.getDefaultNewCameraMatrix(intrinsicCameraMatrix, newImgSize, True)
#else:
#    newCameraMatrix = None

pastObjects = []
currentObjects = []
if capture.isOpened():
    ret = True
    frameNum = timeInterval.first
    capture.set(cv2.CAP_PROP_POS_FRAMES, frameNum)
    lastFrameNum = timeInterval.last

    while ret and frameNum <= lastFrameNum:
        ret, img = capture.read()
        if ret:
            if frameNum%50 == 0:
                print('frame number: {}'.format(frameNum))
            #if undistort:
            #    img = cv2.remap(img, map1, map2, interpolation=cv2.INTER_LINEAR)
            if useYolo:
                results = yolo.predict(img, conf = classifierParams.confidence, classes=list(moving.cocoTypeNames.keys()), verbose=False)

            for obj in objects[:]:
                if obj.getFirstInstant() <= frameNum: # if images are skipped
                    obj.initClassifyUserTypeHoGSVM(speedAggregationFunc, pedBikeCarSVM, bikeCarSVM, classifierParams.maxPedestrianSpeed, classifierParams.maxCyclistSpeed, classifierParams.nFramesIgnoreAtEnds, invHomography, intrinsicCameraMatrix, distortionCoefficients)
                    currentObjects.append(obj)
                    objects.remove(obj)

            for obj in currentObjects[:]:
                if obj.getLastInstant() <= frameNum:
                    obj.classifyUserTypeHoGSVM(classifierParams.minSpeedEquiprobable, speedProbabilities, classifierParams.maxPercentUnknown)
                    pastObjects.append(obj)
                    currentObjects.remove(obj)
                else:
                    if useYolo:
                        # if one feature falls in bike, it's a bike
                        # could one count all hits in various objects, or one takes majority at the instant?
                        #print('yolo', len(results[0].boxes))
                        obj.classifyUserTypeYoloAtInstant(frameNum, results[0].boxes)
                    else:
                        obj.classifyUserTypeHoGSVMAtInstant(img, frameNum, width, height, classifierParams.percentIncreaseCrop, classifierParams.percentIncreaseCrop, classifierParams.minNPixels, classifierParams.hogRescaleSize, classifierParams.hogNOrientations, classifierParams.hogNPixelsPerCell, classifierParams.hogNCellsPerBlock, classifierParams.hogBlockNorm)
                    if args.verbose:
                        print('obj {}@{}: {}'.format(obj.getNum(), frameNum, moving.userTypeNames[obj.userTypes[frameNum]]))
        frameNum += 1
    
    for obj in currentObjects:
        obj.classifyUserTypeHoGSVM(classifierParams.minSpeedEquiprobable, speedProbabilities, classifierParams.maxPercentUnknown)
        pastObjects.append(obj)
    print('Saving user types')
    storage.setRoadUserTypes(databaseFilename, pastObjects)
