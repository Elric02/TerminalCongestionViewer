#! /usr/bin/env python3

import sys, argparse, os.path, sqlite3
from datetime import datetime, timedelta

from trafficintelligence import cvutils, utils, moving, storage
from trafficintelligence.metadata import connectDatabase, Site, VideoSequence, CameraView, getSite

timeConverter = utils.TimeConverter()

parser = argparse.ArgumentParser(description='The program merges feature trajectories recorded from the same site synchronously between start and end time.')
parser.add_argument('--db', dest = 'metadataFilename', help = 'name of the metadata file', required = True)
parser.add_argument('-n', dest = 'siteId', help = 'site id or site name', required = True)
parser.add_argument('-f', dest = 'startTime', help = 'time to start merging features (format %%Y-%%m-%%d %%H:%%M:%%S, eg 2011-06-22 10:00:39)', type = timeConverter.convert) # if not provided, take common time interval
parser.add_argument('-l', dest = 'endTime', help = 'time to stop merging features (format %%Y-%%m-%%d %%H:%%M:%%S, eg 2011-06-22 10:00:39)', type = timeConverter.convert)
parser.add_argument('-o', dest = 'outputDBFilename', help = 'name of the output SQLite file', required = True)

args = parser.parse_args()

session = connectDatabase(args.metadataFilename)

site = getSite(session, args.siteId)
if site is None:
    print('Site {} was not found in {}. Exiting'.format(args.siteId, args.metadataFilename))
    sys.exit()
else:
    site = site[0]

startTime = datetime.strptime(args.startTime, utils.datetimeFormat)
endTime = datetime.strptime(args.endTime, utils.datetimeFormat)
processInterval = moving.TimeInterval(startTime, endTime)
cameraViews = session.query(CameraView).filter(CameraView.site == site).filter(CameraView.virtual == False)
videoSequences = session.query(VideoSequence).filter(VideoSequence.virtual == False).order_by(VideoSequence.startTime.asc()).all() #.order_by(VideoSequence.cameraViewIdx) .filter(VideoSequence.startTime <= startTime)
videoSequences = [vs for vs in videoSequences if vs.cameraView in cameraViews]
#timeIntervals = [v.intersection(startTime, endTime) for v in videoSequences]
#cameraViews = set([v.cameraView for v in videoSequences])

videoSequences = {cv: [v for v in videoSequences if v.cameraView == cv] for cv in cameraViews}
timeIntervals = {}
for cv in videoSequences:
    timeIntervals[cv] = moving.TimeInterval.unionIntervals([v.getTimeInterval() for v in videoSequences[cv]])

# intersection of the time interval (union) for each camera view
commonTimeInterval = list(timeIntervals.values())[0]
for inter in timeIntervals.values()[1:]:
    commonTimeInterval = moving.TimeInterval.intersection(commonTimeInterval, inter)
commonTimeInterval = moving.TimeInterval.intersection(commonTimeInterval, processInterval)

if commonTimeInterval.empty():
    print('Empty time interval. Exiting')
    sys.exit()

if len(set([cv.cameraType.frameRate for cv in cameraViews])) > 1:
    print('Different framerates of the cameras ({}) are not handled yet. Exiting'.format([cv.cameraType.frameRate for cv in cameraViews]))
else:
    frameRate = cv.cameraType.frameRate

try:
    outConnection = sqlite3.connect(args.outputDBFilename)
    outCursor = outConnection.cursor()
    storage.createTrajectoryTable(outCursor, 'positions')
    storage.createTrajectoryTable(outCursor, 'velocities')
    storage.createFeatureCorrespondenceTable(outCursor)
    outConnection.commit()
except sqlite3.OperationalError as error:
    storage.printDBError(error)
    sys.exit()

dirname = os.path.split(args.metadataFilename)[0]
if len(dirname) == 0:
    dirname = '.'

newTrajectoryId = -1
# first frame num is commonTimeInterval
for cv, vs in videoSequences.items():
    print(cv.idx, cv.description)
    for videoSequence in vs:
        try:
            vsConnection = sqlite3.connect(dirname+os.path.sep+videoSequence.getDatabaseFilename())
            vsCursor = vsConnection.cursor()
            firstFrameNum = utils.deltaFrames(videoSequence.startTime, commonTimeInterval.first, frameRate)
            lastFrameNum = (commonTimeInterval.last-videoSequence.startTime).seconds*frameRate
            # positions table
            vsCursor.execute('SELECT * FROM positions WHERE frame_number BETWEEN {} AND {} ORDER BY trajectory_id'.format(firstFrameNum, lastFrameNum))
            featureIdCorrespondences = {}
            currentTrajectoryId = -1
            for row in vsCursor:
                if row[0] != currentTrajectoryId:
                    currentTrajectoryId = row[0]
                    newTrajectoryId += 1
                    featureIdCorrespondences[currentTrajectoryId] = newTrajectoryId
                outCursor.execute(storage.insertTrajectoryQuery('positions'), (newTrajectoryId, row[1]-firstFrameNum, row[2], row[3]))
            # velocities table
            for row in vsCursor:
                outCursor.execute(storage.insertTrajectoryQuery('velocities'), (featureIdCorrespondences[row[0]], row[1]-firstFrameNum, row[2], row[3]))
            # saving the id correspondences
            for oldId, newId in featureIdCorrespondences.items():
                outCursor.execute("INSERT INTO feature_correspondences (trajectory_id, source_dbname, db_trajectory_id) VALUES ({},\"{}\",{})".format(newId, videoSequence.name, oldId))
            outConnection.commit()
        except sqlite3.OperationalError as error:
            storage.printDBError(error)
            
# save the information of the new virtual sequence and camera view in the metadata
mergedCameraView = CameraView('merged', None, site, cv.cameraType, None, None, virtual = True)
session.add(mergedCameraView)
session.add(VideoSequence('merged', commonTimeInterval.first, commonTimeInterval.last-commonTimeInterval.first, mergedCameraView, virtual = True))
session.commit()
