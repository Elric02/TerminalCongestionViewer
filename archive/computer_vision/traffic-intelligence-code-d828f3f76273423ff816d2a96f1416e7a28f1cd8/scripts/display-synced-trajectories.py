#! /usr/bin/env python3

import sys, argparse, os.path
from datetime import datetime, timedelta

import numpy as np
import cv2

from trafficintelligence import cvutils, utils, storage
from trafficintelligence.metadata import connectDatabase, Site, CameraView, VideoSequence

parser = argparse.ArgumentParser(description='The program displays several views of the same site synchronously.')
parser.add_argument('--db', dest = 'metadataFilename', help = 'name of the metadata file', required = True)
#parser.add_argument('-n', dest = 'siteId', help = 'site id or site name', required = True)
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file', required = True)
parser.add_argument('-f', dest = 'startTime', help = 'time to start playing (format %%Y-%%m-%%d %%H:%%M:%%S, eg 2011-06-22 10:00:39)', required = True)
parser.add_argument('-t', dest = 'trajectoryType', help = 'type of trajectories to display', choices = ['feature', 'object'], default = 'object')
parser.add_argument('-r', dest = 'rescale', help = 'rescaling factor for the displayed image', default = 1., type = float)
parser.add_argument('-s', dest = 'step', help = 'display every s image', default = 1, type = int)
parser.add_argument('--undistort', dest = 'undistort', help = 'undistort the video (because features have been extracted that way)', action = 'store_true')

args = parser.parse_args()

session = connectDatabase(args.metadataFilename)

mergedSequence = session.query(VideoSequence).filter(VideoSequence.databaseFilename == args.databaseFilename).first()
if mergedSequence is None:
    print('Video sequence {} was not found in {}. Exiting'.format(args.databaseFilename, args.metadataFilename))
    sys.exit()

dirname = os.path.split(args.metadataFilename)[0]

frameRate = mergedSequence.cameraView.cameraType.frameRate
startTime = datetime.strptime(args.startTime, utils.datetimeFormat)
mergedFirstFrameNum = utils.deltaFrames(mergedSequence.startTime, startTime, frameRate)

cameraViews = session.query(CameraView).filter(CameraView.site == mergedSequence.cameraView.site).filter(CameraView.virtual == False).all()
videoSequences = session.query(VideoSequence).filter(VideoSequence.virtual == False).all()
#videoSequences.remove(mergedSequence)
videoSequences = [v for v in videoSequences if v.cameraView in cameraViews and (v.containsInstant(startTime) or v.startTime > startTime)]
filenames = [dirname+os.path.sep+v.getVideoSequenceFilename() for v in videoSequences]
firstFrameNums = [utils.deltaFrames(v.startTime, startTime, frameRate) for v in videoSequences] # use pos/neg first frame nums
windowNames = [v.cameraView.description for v in videoSequences]

# homography and undistort
homographies = [np.linalg.inv(np.loadtxt(dirname+os.path.sep+v.cameraView.getHomographyFilename())) for v in videoSequences]
if args.undistort:
    cameraTypes = set([cv.cameraType for cv in cameraViews])
    for cameraType in cameraTypes:
        cameraType.computeUndistortMaps()

objects = storage.loadTrajectoriesFromSqlite(dirname+os.path.sep+mergedSequence.getDatabaseFilename(), args.trajectoryType)
for obj in objects:
    obj.projectedPositions = {}

#def playVideo(filenames, windowNames = None, firstFrameNums = None, frameRate = -1, interactive = False, printFrames = True, text = None, rescale = 1., step = 1):
if len(filenames) == 0:
    print('Empty filename list')
    sys.exit()

if windowNames is None:
    windowNames = ['frame{}'.format(i) for i in range(len(filenames))]
#wait = 5
#if rescale == 1.:
for windowName in windowNames:
    cv2.namedWindow(windowName, cv2.WINDOW_NORMAL)
#if frameRate > 0:
#    wait = int(round(1000./frameRate))
#if interactive:
wait = 0
rescale = 1.
captures = [cv2.VideoCapture(fn) for fn in filenames]
if np.array([cap.isOpened() for cap in captures]).all():
    key = -1
    ret = True
    nFramesShown = 0
    for i in range(len(captures)):
        if firstFrameNums[i] > 0:
            captures[i].set(cv2.cv.CV_CAP_PROP_POS_FRAMES, firstFrameNums[i])
    while ret and not cvutils.quitKey(key):
        rets = []
        images = []
        for i in range(len(captures)):
            if firstFrameNums[i]+nFramesShown>=0:
                ret, img = captures[i].read()
                if ret and args.undistort:
                    img = cv2.remap(img, videoSequences[i].cameraView.cameraType.map1, videoSequences[i].cameraView.cameraType.map2, interpolation=cv2.INTER_LINEAR)
                rets.append(ret)
                images.append(img)
            else:
                rets.append(False)
                images.append(None)                
        if np.array(rets).any():
            #if printFrames:
            print('frame shown {0}'.format(nFramesShown))
            for i in range(len(filenames)):
                if rets[i]:#firstFrameNums[i]+nFramesShown>=0:
                    for obj in objects:
                        if obj.existsAtInstant(mergedFirstFrameNum+nFramesShown):
                            #print obj.num, obj.timeInterval, mergedFirstFrameNum, nFramesShown
                            if i not in obj.projectedPositions:
                                if homographies[i] is not None:
                                    obj.projectedPositions[i] = obj.positions.homographyProject(homographies[i])
                                else:
                                    obj.projectedPositions[i] = obj.positions
                            cvutils.cvPlot(images[i], obj.projectedPositions[i], cvutils.cvColors['default'][obj.getNum()], int(mergedFirstFrameNum+nFramesShown)-obj.getFirstInstant())

                    #if text is not None:
                    #    cv2.putText(images[i], text, (10,50), cv2.FONT_HERSHEY_PLAIN, 1, cvRed['default'])
                    cvutils.cvImshow(windowNames[i], images[i], rescale) # cv2.imshow('frame', img)
            key = cv2.waitKey(wait)
            #if cvutils.saveKey(key):
            #    cv2.imwrite('image-{}.png'.format(frameNum), img)
        nFramesShown += args.step
        if args.step > 1:
            for i in range(len(captures)):
                if firstFrameNums[i]+nFramesShown >= 0:
                    captures[i].set(cv2.cv.CV_CAP_PROP_POS_FRAMES, firstFrameNums[i]+nFramesShown)
    cv2.destroyAllWindows()
else:
    print('Video captures for {} failed'.format(filenames))
