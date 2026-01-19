#! /usr/bin/env python3

import sys, argparse, os.path
from shutil import copy

from trafficintelligence.cvutils import getImagesFromVideo
from matplotlib.pyplot import imsave

from trafficintelligence import storage, utils

# could try to guess the video
# check if there is already a tracking.cfg file

parser = argparse.ArgumentParser(description='The program initilizes the files for tracking: copy tracking.cfg, sets up with the video filename, generates a frame image (frame.png) and prints the next commands')

parser.add_argument('-i', dest = 'videoFilename', help = 'filename of the video sequence', required = True)
parser.add_argument('-n', dest = 'nFrames', help = 'number of frames to extract', type = int)

args = parser.parse_args()

# assumes tracking.cfg is in the parent directory to the directory of the traffic intelligence python modules
matchingPaths = [s for s in sys.path if 'traffic-intelligence' in s]
#if len(matchingPaths) > 1:
#    print('Too many matching paths for Traffic Intelligence modules: {}'.format(matchingPaths))
if len(matchingPaths) == 0:
    print('No environment path to Traffic Intelligence modules.\nExiting')
    sys.exit()
else:
    directoryName = matchingPaths[0]
    if directoryName.endswith('/'):
        directoryName = directoryName[:-1]
    if os.path.exists(directoryName+'/tracking.cfg') and not os.path.exists('./tracking.cfg'):
        f = utils.openCheck(directoryName+'/tracking.cfg')
        out = utils.openCheck('./tracking.cfg', 'w')
        for l in f:
            if 'video-filename' in l:
                tmp = l.split('=')
                out.write(tmp[0]+'= '+args.videoFilename+'\n')
            elif 'database-filename' in l:
                tmp = l.split('=')
                out.write(tmp[0]+'= '+utils.removeExtension(args.videoFilename)+'.sqlite\n')                
            else:
                out.write(l)
        f.close()
        out.close()
        print('Configuration file tracking.cfg successfully copied to the current directory with video and database filename adapted')
    if os.path.exists(directoryName+'/classifier.cfg') and not os.path.exists('./classifier.cfg'):
        copy(directoryName+'/classifier.cfg', 'classifier.cfg')
        print('Configuration file classifier.cfg successfully copied to the current directory')
        
# extract image from video
if args.nFrames is not None:
    image = getImagesFromVideo(args.videoFilename, 0, args.nFrames, saveImage = True, outputPrefix = 'frame')
else:
    image = getImagesFromVideo(args.videoFilename, saveImage = True, outputPrefix = 'frame')
print('first video frame successfully copied to the current directory')

# next commands
print('''--------------------------------------
Here are a sample of the next command to compute the homography,
track features, group them in objects and display object trajectories
--------------------------------------''')
print('compute-homography -i [frame.png] -w [world_image] -n [npoints] -u [unit_per_pixel]')
print('(beware of camera distortion)')
print('feature-based-tracking tracking.cfg --tf')
print('feature-based-tracking tracking.cfg --gf')
print('display-trajectories --cfg tracking.cfg -t object')
