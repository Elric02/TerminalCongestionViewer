#! /usr/bin/env python3
# from https://docs.ultralytics.com/modes/track/
import sys, argparse
from math import inf
from copy import copy
from collections import Counter
import numpy as np
from scipy.optimize import linear_sum_assignment
from ultralytics import YOLO
from torch import cat
from torchvision.ops import box_iou
import cv2

from trafficintelligence import cvutils, moving, storage, utils

parser = argparse.ArgumentParser(description='The program tracks objects using the ultralytics models and trackers.',
                                 epilog= '''The models can be found in the Ultralytics model zoo, 
                                 eg YOLOv8 (https://docs.ultralytics.com/models/yolov8/).
                                 The tracking models can be found also online 
                                 (https://github.com/ultralytics/ultralytics/tree/main/ultralytics/cfg/trackers).
                                 The choice is to project the middle of the bottom line for persons, 
                                 and the bounding box center otherwise.''')
parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the configuration file')
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file (overrides the configuration file)')
parser.add_argument('-i', dest = 'videoFilename', help = 'name of the video file (overrides the configuration file)')
parser.add_argument('-m', dest = 'detectorFilename', help = 'name of the detection model file', required = True)
parser.add_argument('-t', dest = 'trackerFilename', help = 'name of the tracker file', required = True)
parser.add_argument('-o', dest = 'homographyFilename', help = 'filename of the homography matrix')
parser.add_argument('-k', dest = 'maskFilename', help = 'name of the mask file')
parser.add_argument('--undistort', dest = 'undistort', help = 'undistort the video', action = 'store_true')
parser.add_argument('--intrinsic', dest = 'intrinsicCameraMatrixFilename', help = 'name of the intrinsic camera file')
parser.add_argument('--distortion-coefficients', dest = 'distortionCoefficients', help = 'distortion coefficients', nargs = '*', type = float)
parser.add_argument('--display', dest = 'display', help = 'show the raw detection and tracking results', action = 'store_true')
#parser.add_argument('--no-image-coordinates', dest = 'notSavingImageCoordinates', help = 'not saving the raw detection and tracking results', action = 'store_true')
parser.add_argument('-f', dest = 'firstFrameNum', help = 'number of first frame number to process', type = int)
parser.add_argument('-l', dest = 'lastFrameNum', help = 'number of last frame number to process', type = int, default = inf)
parser.add_argument('--conf', dest = 'confidence', help = 'object confidence threshold for detection', type = float, default = 0.25)
parser.add_argument('--bike-prop', dest = 'bikeProportion', help = 'minimum proportion of time a person classified as bike or motorbike to be classified as cyclist', type = float, default = 0.2)
parser.add_argument('--cyclist-iou', dest = 'cyclistIou', help = 'IoU threshold to associate a bike and ped bounding box', type = float, default = 0.15)
parser.add_argument('--cyclist-match-prop', dest = 'cyclistMatchingProportion', help = 'minimum proportion of time a bike exists and is associated with a pedestrian to be merged as cyclist', type = float, default = 0.3)
#parser.add_argument('--max-temp-overal', dest = 'maxTemporalOverlap', help = 'maximum proportion of time to merge 2 bikes associated with same pedestrian', type = float, default = 0.05)

args = parser.parse_args()
params, videoFilename, databaseFilename, homography, invHomography, intrinsicCameraMatrix, distortionCoefficients, undistortedImageMultiplication, undistort, firstFrameNum = storage.processVideoArguments(args)

if args.homographyFilename is not None:
    homography = np.loadtxt(args.homographyFilename)
if args.intrinsicCameraMatrixFilename is not None:
    intrinsicCameraMatrix = np.loadtxt(args.intrinsicCameraMatrixFilename)
if args.distortionCoefficients is not None:
    distortionCoefficients = np.array(args.distortionCoefficients)
if args.firstFrameNum is not None:
    firstFrameNum = args.firstFrameNum
if args.lastFrameNum is not None:
    lastFrameNum = args.lastFrameNum
elif args.configFilename is not None:
    lastFrameNum = params.lastFrameNum
else:
    lastFrameNum = np.inf
if args.maskFilename is not None:
    mask = cv2.imread(args.maskFilename, cv2.IMREAD_GRAYSCALE)
elif params is not None and params.maskFilename is not None:
    mask = cv2.imread(params.maskFilename, cv2.IMREAD_GRAYSCALE)
else:
    mask = None
if params is not None:
    smoothingHalfWidth = params.smoothingHalfWidth
    minObjectDuration = params.minFeatureTime
else:
    smoothingHalfWidth = None
    minObjectDuration = 3

# TODO use mask, remove short objects, smooth

# TODO add option to refine position with mask for vehicles, to save different positions
# TODO work with optical flow (farneback or RAFT) https://pytorch.org/vision/main/models/raft.html

# use 2 x bytetrack track buffer to remove objects from existing ones

# Load a model
model = YOLO(args.detectorFilename) # seg yolov8x-seg.pt
# seg could be used on cropped image... if can be loaded and kept in memory
# model = YOLOX('/home/nicolas/Research/Data/classification-models/yolo_nas_l.pt ') # AttributeError: 'YoloNAS_L' object has no attribute 'get'

# Track with the model
if args.display:
    windowName = 'frame'
    cv2.namedWindow(windowName, cv2.WINDOW_NORMAL)

capture = cv2.VideoCapture(videoFilename)
objects = {}
featureNum = 1
frameNum = firstFrameNum
capture.set(cv2.CAP_PROP_POS_FRAMES, frameNum)

success, frame = capture.read()
if not success:
    print('Input {} could not be read. Exiting'.format(videoFilename))
    import sys; sys.exit()

results = model.track(source=frame, tracker=args.trackerFilename, classes=list(moving.cocoTypeNames.keys()), conf=args.confidence, persist=True, verbose=False)
while capture.isOpened() and success and frameNum <= lastFrameNum:
    result = results[0]
    if frameNum %10 == 0:
        print(frameNum, len(result.boxes), 'objects')
    for box in result.boxes:
        if box.id is not None:# None are objects with low confidence
            xyxy = copy(box.xyxy)
            minPoint = moving.Point(xyxy[0,0].item(), xyxy[0,1].item())
            maxPoint = moving.Point(xyxy[0,2].item(), xyxy[0,3].item())
            center = (minPoint+maxPoint).divide(2.).asint()
            if mask is None or mask[center.y, center.x] > 0:
                num = int(box.id.item())
                if num in objects:
                    objects[num].timeInterval.last = frameNum
                    objects[num].features[0].timeInterval.last = frameNum
                    objects[num].features[1].timeInterval.last = frameNum
                    objects[num].bboxes[frameNum] = xyxy
                    objects[num].userTypes.append(moving.coco2Types[int(box.cls.item())])
                    objects[num].features[0].tmpPositions[frameNum] = minPoint # min
                    objects[num].features[1].tmpPositions[frameNum] = maxPoint # max
                else:
                    inter = moving.TimeInterval(frameNum, frameNum)
                    objects[num] = moving.MovingObject(num, inter)
                    objects[num].bboxes = {frameNum: copy(xyxy)}
                    objects[num].userTypes = [moving.coco2Types[int(box.cls.item())]]
                    objects[num].features = [moving.MovingObject(featureNum, copy(inter)), moving.MovingObject(featureNum+1, copy(inter))]
                    objects[num].featureNumbers = [featureNum, featureNum+1]
                    objects[num].features[0].tmpPositions = {frameNum: minPoint}
                    objects[num].features[1].tmpPositions = {frameNum: maxPoint}
                    featureNum += 2
    if args.display:
        cvutils.cvImshow(windowName, result.plot()) # original image in orig_img
        key = cv2.waitKey()
        if cvutils.quitKey(key):
            break
    frameNum += 1
    success, frame = capture.read()
    if success:
        results = model.track(source=frame, persist=True)
capture.release()
cv2.destroyAllWindows()

# classification
shortObjectNumbers = []
for num, obj in objects.items():
    if obj.length() < minObjectDuration:
        shortObjectNumbers.append(num)
    else:
        obj.setUserType(utils.mostCommon(obj.userTypes)) # improve? mix with speed?
for num in shortObjectNumbers:
    del objects[num]
# TODO add quality control: avoid U-turns
    
# merge bikes and people
twowheels = [num for num, obj in objects.items() if obj.getUserType() in (moving.userType2Num['motorcyclist'],moving.userType2Num['cyclist'])]
pedestrians = [num for num, obj in objects.items() if obj.getUserType() == moving.userType2Num['pedestrian']]

def mergeObjects(obj1, obj2):
    obj1.features = obj1.features+obj2.features
    obj1.featureNumbers = obj1.featureNumbers+obj2.featureNumbers
    obj1.timeInterval = moving.TimeInterval(min(obj1.getFirstInstant(), obj2.getFirstInstant()), max(obj1.getLastInstant(), obj2.getLastInstant()))    
    
costs = []
for twInd in twowheels:
    tw = objects[twInd]
    tw.nBBoxes = len(tw.bboxes)
    twCost = []
    for pedInd in pedestrians:
        ped = objects[pedInd]
        nmatches = 0
        for t in tw.bboxes:
            if t in ped.bboxes:
                #print(tw.num, ped.num, t, box_iou(tw.bboxes[t], ped.bboxes[t]))
                if not tw.commonTimeInterval(ped).empty() and box_iou(tw.bboxes[t], ped.bboxes[t]).item() > args.cyclistIou:
                    nmatches += 1
        twCost.append(nmatches/tw.nBBoxes)
    costs.append(twCost)

costs = -np.array(costs)

if costs.size > 0:
# before matching, scan for pedestrians with good non-overlapping temporal match with different bikes
    for pedInd in range(costs.shape[1]):
        nMatchedBikes = (costs[:,pedInd] < -args.cyclistMatchingProportion).sum()
        if nMatchedBikes == 0: # peds that have no bike matching: see if they have been classified as bikes sometimes (more than args.bikeProportion)
            userTypeStats = Counter(obj.userTypes)
            if (moving.userType2Num['cyclist'] in userTypeStats or (moving.userType2Num['motorcyclist'] in userTypeStats and moving.userType2Num['cyclist'] in userTypeStats and userTypeStats[moving.userType2Num['motorcyclist']]<=userTypeStats[moving.userType2Num['cyclist']])) and userTypeStats[moving.userType2Num['motorcyclist']]+userTypeStats[moving.userType2Num['cyclist']] > args.bikeProportion*userTypeStats.total(): # verif if not turning all motorbike into cyclists
                obj.setUserType(moving.userType2Num['cyclist'])
        # elif nMatchedBikes > 1: # try to merge bikes first
        #     twIndices = np.nonzero(costs[:,pedInd] < -args.cyclistMatchingProportion)[0]
        #     # we have to compute temporal overlaps of all 2 wheels among themselves, then remove the ones with the most overlap (sum over column) one by one until there is little left
        #     nTwoWheels = len(twIndices)
        #     twTemporalOverlaps = np.zeros((nTwoWheels,nTwoWheels))
        #     for i in range(nTwoWheels):
        #         for j in range(i):
        #             twi = objects[twowheels[twIndices[i]]]
        #             twj = objects[twowheels[twIndices[j]]]
        #             twTemporalOverlaps[i,j] = len(set(twi.bboxes).intersection(set(twj.bboxes)))/max(len(twi.bboxes), len(twj.bboxes))
        #             #twTemporalOverlaps[j,i] = twTemporalOverlaps[i,j]
        #     tw2merge = list(range(nTwoWheels))
        #     while len(tw2merge)>0 and (twTemporalOverlaps[np.ix_(tw2merge, tw2merge)] > args.maxTemporalOverlap).sum(0).max() >= 2:
        #         i = (twTemporalOverlaps[np.ix_(tw2merge, tw2merge)] > args.maxTemporalOverlap).sum(0).argmax()
        #         del tw2merge[i]
        #     twIndices = [twIndices[i] for i in tw2merge]
        #     tw1 = objects[twowheels[twIndices[0]]]
        #     twCost = costs[twIndices[0],:]*tw1.nBBoxes
        #     nBBoxes = tw1.nBBoxes
        #     for twInd in twIndices[1:]:
        #         mergeObjects(tw1, objects[twowheels[twInd]])
        #         twCost = twCost + costs[twInd,:]*objects[twowheels[twInd]].nBBoxes
        #         nBBoxes += objects[twowheels[twInd]].nBBoxes
        #     twIndicesToKeep = list(range(costs.shape[0]))
        #     for twInd in twIndices[1:]:
        #         twIndicesToKeep.remove(twInd)
        #         del objects[twowheels[twInd]]
        #     twowheels = [twowheels[i] for i in twIndicesToKeep]
        #     costs = costs[twIndicesToKeep,:]

    twIndices, matchingPedIndices = linear_sum_assignment(costs)
    for twInd, pedInd in zip(twIndices, matchingPedIndices): # caution indices in the cost matrix
        if -costs[twInd, pedInd] >= args.cyclistMatchingProportion:
            tw = objects[twowheels[twInd]]
            ped = objects[pedestrians[pedInd]]
            mergeObjects(tw, ped)
            del objects[pedestrians[pedInd]]
            # link ped to each assigned bike, remove bike from cost (and ped is temporal match is high)
            
            #TODO Verif overlap piéton vélo : si long hors overlap, changement mode (trouver exemples)
    # TODO continue assigning if leftover bikes (if non temporally overlapping with existing bikes assigned to ped)
            
# interpolate and save image coordinates
for num, obj in objects.items():
    for f in obj.getFeatures():
        if f.length() != len(f.tmpPositions): # interpolate
            f.positions = moving.Trajectory.fromPointDict(f.tmpPositions)
        else:
            f.positions = moving.Trajectory.fromPointList(list(f.tmpPositions.values()))
#if not args.notSavingImageCoordinates: storage.saveTrajectoriesToSqlite(utils.removeExtension(databaseFilename)+'-bb.sqlite', list(objects.values()), 'object') # to be changed as bbox will be saved by default
# project and smooth
for num, obj in objects.items():
    features = obj.getFeatures()
    # possible to save bottom pedestrians? not consistent with other users
    # if moving.userTypeNames[obj.getUserType()] == 'pedestrian':
    #     assert len(features) == 2
    #     t1 = features[0].getPositions()
    #     t2 = features[1].getPositions()
    #     t = [[(p1.x+p2.x)/2., max(p1.y, p2.y)] for p1, p2 in zip(t1, t2)]
    # else:
    t = []
    for instant in obj.getTimeInterval():
        points = [f.getPositionAtInstant(instant) for f in features if f.existsAtInstant(instant)]
        t.append(moving.Point.agg(points, np.mean).aslist())
    #t = sum([f.getPositions().asArray() for f in features])/len(features)
    #t = (moving.Trajectory.add(t1, t2)*0.5).asArray()
    projected = cvutils.imageToWorldProject(np.array(t).T, intrinsicCameraMatrix, distortionCoefficients, homography)
    featureNum = features[0].getNum()
    feature = moving.MovingObject(featureNum, obj.getTimeInterval(), moving.Trajectory(projected.tolist()))
    if smoothingHalfWidth is not None: # smoothing
        feature.smoothPositions(smoothingHalfWidth, replace = True)#f.positions = f.getPositions().filterMovingWindow(smoothingHalfWidth)
    feature.computeVelocities()
    obj.features=[feature]
    obj.featureNumbers = [featureNum]
#saving
storage.saveTrajectoriesToSqlite(databaseFilename, list(objects.values()), 'object')



# todo save bbox and mask to study localization / representation
# apply quality checks deviation and acceleration bounds?

# def mergeBBoxes(tw, ped):
#     'merges ped into tw (2nd obj into first obj)'
#     timeInstants = set(tw.bboxes).union(set(ped.bboxes))
#     for t in timeInstants:
#         if t in tw.bboxes and t in ped.bboxes:
#             tw.features[0].tmpPositions[t] = moving.Point(min(tw.features[0].tmpPositions[t].x, ped.features[0].tmpPositions[t].x),
#                                                           min(tw.features[0].tmpPositions[t].y, ped.features[0].tmpPositions[t].y))
#             tw.features[1].tmpPositions[t] = moving.Point(max(tw.features[1].tmpPositions[t].x, ped.features[1].tmpPositions[t].x),
#                                                           max(tw.features[1].tmpPositions[t].y, ped.features[1].tmpPositions[t].y))
#         elif t in ped.bboxes:
#             tw.features[0].tmpPositions[t] = ped.features[0].tmpPositions[t]
#             tw.features[1].tmpPositions[t] = ped.features[1].tmpPositions[t]
#     tw.timeInterval = moving.TimeInterval(min(tw.getFirstInstant(), ped.getFirstInstant()), max(tw.getLastInstant(), ped.getLastInstant()))
