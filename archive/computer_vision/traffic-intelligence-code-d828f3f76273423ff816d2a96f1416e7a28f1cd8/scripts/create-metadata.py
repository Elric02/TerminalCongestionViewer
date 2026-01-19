#! /usr/bin/env python3

import argparse
from datetime import datetime

from trafficintelligence import metadata, utils

timeConverter = utils.TimeConverter()

parser = argparse.ArgumentParser(description='The program add camera views (metadata.CameraView) for a site or video sequences (metadata.VideoSequence) for a site and a view.')
parser.add_argument('-i', dest = 'databaseFilename', help = 'name of the metadata filename', required = True)
parser.add_argument('-d', dest = 'dirname', help = 'directory name containing sites or video sequences for a given view')
#parser.add_argument('-s', dest = 'siteId', help = 'site id (if provided, the program adds video sequences for the camera view)')
parser.add_argument('-v', dest = 'viewId', help = 'camera view id')
parser.add_argument('--nviews', dest = 'nViewsPerSite', help = 'default number of camera views', type = int, default = 1)
parser.add_argument('-s', dest = 'startTime', help = 'starting time of the first video (format %%Y-%%m-%%d %%H:%%M:%%S, eg 2011-06-22 10:00:39)', type = timeConverter.convert)
parser.add_argument('--timeformat', dest = 'timeFormat', help = 'time format of the video filenames (optional) (eg %%Y_%%m%%d_%%H%%M%%S, eg 2017_0627_163231)')
args = parser.parse_args()

session = metadata.connectDatabase(args.databaseFilename)
if args.viewId is not None:
    # sites = metadata.getSite(session, args.siteId)
    # if len(sites) > 1:
    #     print('{} sites found matching {}, using the first {}'.format(len(sites), args.siteId, sites[0].name))
    # site = sites[0]
    cameraView = metadata.getCameraView(session, args.viewId)
    metadata.initializeVideos(session, cameraView, args.dirname, args.startTime, args.timeFormat)
else:
    metadata.initializeSites(session, args.dirname, args.nViewsPerSite)

