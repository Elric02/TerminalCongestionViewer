#! /usr/bin/env python3

import sys, argparse
from numpy import loadtxt
from numpy.linalg import inv

from trafficintelligence import moving, storage, cvutils

# TODO: need to trim objects to same mask ?

# Warning, not working with changed intrinsic and homography processing

parser = argparse.ArgumentParser(description='The program computes the CLEAR MOT metrics between ground truth and tracker output (in Polytrack format)', epilog='''CLEAR MOT metrics information:
Keni, Bernardin, and Stiefelhagen Rainer. "Evaluating multiple object tracking performance: the CLEAR MOT metrics." EURASIP Journal on Image and Video Processing 2008 (2008)

Polytrack format: 
JP. Jodoin\'s MSc thesis (in french)
see examples on http://www.jpjodoin.com/urbantracker/dataset.html''', formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('-d', dest = 'trackerDatabaseFilename', help = 'name of the Sqlite database containing the tracker output', required = True)
parser.add_argument('-g', dest = 'groundTruthDatabaseFilename', help = 'name of the Sqlite database containing the ground truth', required = True)
parser.add_argument('-o', dest = 'homographyFilename', help = 'name of the filename for the homography (if tracking was done using the homography)')
parser.add_argument('-m', dest = 'matchingDistance', help = 'matching distance between tracker and ground truth trajectories', required = True, type = float)
parser.add_argument('-k', dest = 'maskFilename', help = 'filename of the mask file used to define the where objects were tracked')
parser.add_argument('-f', dest = 'firstInstant', help = 'first instant for measurement', required = True, type = int)
parser.add_argument('-l', dest = 'lastInstant', help = 'last instant for measurement', required = True, type = int)
parser.add_argument('--offset', dest = 'nFramesOffsetAnnotations', help = 'number of frames to offset the ground truth annotations', type = int)
parser.add_argument('--displayOffset', dest = 'nFramesOffsetDisplay', help = 'number of frames to offset annotations and objects for display', type = int)
parser.add_argument('--display', dest = 'display', help = 'display the ground truth to object matches (graphically)', action = 'store_true')
parser.add_argument('--undistort', dest = 'undistort', help = 'undistort the video (because features have been extracted that way)', action = 'store_true')
parser.add_argument('--intrinsic', dest = 'intrinsicCameraMatrixFilename', help = 'name of the intrinsic camera file')
parser.add_argument('--distortion-coefficients', dest = 'distortionCoefficients', help = 'distortion coefficients', nargs = '*', type = float)
parser.add_argument('--undistorted-multiplication', dest = 'undistortedImageMultiplication', help = 'undistorted image multiplication', type = float)

parser.add_argument('-i', dest = 'videoFilename', help = 'name of the video file (for display)')
parser.add_argument('--csv', dest = 'csvOutput', help = 'output comma-separated metrics', action = 'store_true')
args = parser.parse_args()

if args.homographyFilename is not None:
    invHomography = inv(loadtxt(args.homographyFilename))
else:
    invHomography = None

objects = storage.loadTrajectoriesFromSqlite(args.trackerDatabaseFilename, 'object')

if args.maskFilename is not None:
    maskObjects = []
    from matplotlib.pyplot import imread
    mask = imread(args.maskFilename, cv2.IMREAD_GRAYSCALE)
    #if len(mask) > 1: if loaded as RGB
    #    mask = mask[:,:,0]
    for obj in objects:
        maskObjects += obj.getObjectsInMask(mask, invHomography, 10) # TODO add option to keep object if at least one feature in mask
    objects = maskObjects

annotations = storage.loadBBMovingObjectsFromSqlite(args.groundTruthDatabaseFilename)
for a in annotations:
    a.computeCentroidTrajectory(invHomography)

if args.nFramesOffsetAnnotations is not None:
    for a in annotations:
        a.shiftTimeInterval(args.nFramesOffsetAnnotations)

motp, mota, mt, mme, fpt, gt, gtMatches, toMatches = moving.computeClearMOT(annotations, objects, args.matchingDistance, args.firstInstant, args.lastInstant, args.display)

if args.csvOutput:
    print('{},{},{},{},{}'.format(motp, mota, mt, mme, fpt))
else:
    print 'MOTP: {}'.format(motp)
    print 'MOTA: {}'.format(mota)
    print 'Number of missed objects.frames: {}'.format(mt)
    print 'Number of mismatches: {}'.format(mme)
    print 'Number of false alarms.frames: {}'.format(fpt)

def shiftMatches(matches, offset):
    shifted = {}
    for k in matches:
        shifted[k] = {t+offset:v for t, v in matches[k].items()}
    return shifted

if args.display:
    if args.undistort and args.intrinsicCameraMatrixFilename is not None:
        intrinsicCameraMatrix = loadtxt(args.intrinsicCameraMatrixFilename)
    else:
        intrinsicCameraMatrix = None
    firstInstant = args.firstInstant
    lastInstant = args.lastInstant
    cvutils.displayTrajectories(args.videoFilename, objects, {}, invHomography, firstInstant, lastInstant, annotations = annotations, undistort = args.undistort, intrinsicCameraMatrix = intrinsicCameraMatrix, distortionCoefficients = args.distortionCoefficients, undistortedImageMultiplication = args.undistortedImageMultiplication, gtMatches = gtMatches, toMatches = toMatches)
    #print('Ground truth matches')
    #print(gtMatches)
    #print('Object matches')
    #rint toMatches
