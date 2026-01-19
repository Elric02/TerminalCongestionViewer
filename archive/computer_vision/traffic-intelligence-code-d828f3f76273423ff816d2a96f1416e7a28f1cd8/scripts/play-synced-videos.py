#! /usr/bin/env python3

import sys, argparse, os.path
from datetime import datetime, timedelta

from trafficintelligence import cvutils, utils
from trafficintelligence.metadata import connectDatabase, Site, CameraView, VideoSequence, getSite

timeConverter = utils.TimeConverter()

parser = argparse.ArgumentParser(description='The program displays several views of the same site synchronously.')
parser.add_argument('--db', dest = 'metadataFilename', help = 'name of the metadata file', required = True)
parser.add_argument('-n', dest = 'siteId', help = 'site id or site name', required = True)
parser.add_argument('-f', dest = 'startTime', help = 'time to start playing (format %%Y-%%m-%%d %%H:%%M:%%S, eg 2011-06-22 10:00:39)', required = True, type = timeConverter.convert)
parser.add_argument('--fps', dest = 'frameRate', help = 'approximate frame rate to replay', default = -1, type = float)
parser.add_argument('-r', dest = 'rescale', help = 'rescaling factor for the displayed image', default = 1., type = float)
parser.add_argument('-s', dest = 'step', help = 'display every s image', default = 1, type = int)

args = parser.parse_args()

session = connectDatabase(args.metadataFilename)

site = getSite(session, args.siteId)
if site is None:
    print('Site {} was not found in {}. Exiting'.format(args.siteId, args.metadataFilename))
    sys.exit()
else:
    site = site[0]

dirname = os.path.split(args.metadataFilename)[0]

startTime = datetime.strptime(args.startTime, utils.datetimeFormat)
cameraViews = session.query(CameraView).filter(CameraView.site == site)
videoSequences = session.query(VideoSequence).filter(VideoSequence.name != None).filter(VideoSequence.startTime <= startTime).all()
#videoSequences = session.query(VideoSequence).filter(VideoSequence.site == site).filter(VideoSequence.startTime <= startTime).all()
videoSequences = [v for v in videoSequences if v.containsInstant(startTime) and v.cameraView in cameraViews]
filenames = [dirname+os.path.sep+v.getVideoSequenceFilename() for v in videoSequences]
firstFrameNums = [v.getFrameNum(startTime) for v in videoSequences]

cvutils.playVideo(filenames, [v.cameraView.description for v in videoSequences], firstFrameNums, args.frameRate, rescale = args.rescale, step = args.step)
