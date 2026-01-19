#! /usr/bin/env python3
import os
import sys
import glob
from trafficintelligence import storage, moving
import subprocess
import numpy as np


def loadParameters(filename):
    # load initial parameters from x.txt
    f = open(filename, 'r+')
    l = f.readline()
    x = [s for s in l.strip().split(" ")]
    f.close()
    
    # create para-value list
    return paraValueList(x)
    
def paraValueList(x):
    #create para-value list
    #list of the parameters and their values
    p = ['--feature-quality',             #]0.-0.4]
         '--min-feature-distanceklt',     #]0.-10]
         '--block-size',                  #[1-10]integer
         '--window-size',                 #[1-10]integer
         '--min-feature-displacement',    #[0.0001-0.1] 0.05
         '--acceleration-bound',          #[1.-5.] 3
         '--deviation-bound',             #[-1, 1] 0.6
         #p[3] = '--min-tracking-error'          #[0.01-0.3]
         '--min-feature-time',            #[2-100] integer
         '--mm-connection-distance',      #[0.5-100]
         '--mm-segmentation-distance',    #[1-100] ~mm-connection-distance / 2.5
         '--min-nfeatures-group']         #[2-4]
    integerParameters = [2, 3, 7]
    para = []
    if len(x) == 4:
        for n in range(4):
            if n+7 in integerParameters:
                value = x[n].split('.')[0] #int(np.floor(float(x[n]))) recast to str
            else:
                value = x[n]
            para = para + [p[-4+n],value]
    else:
        for n in range(len(x)):
            if n in integerParameters:
                value = x[n].split('.')[0]
            else:
                value = x[n]
            para = para + [p[n], value]
    
    return para

def process(para, intersections, recursive):
    Mota = []
    gtDatabaseaAbsPaths = []
    configFileAbsPaths = []

    cwd = os.getcwd()
    # move to the location of the intersection
    for intersectionPath in intersections:
        intersectionAbsPath = os.path.abspath(intersectionPath)
        os.chdir(intersectionAbsPath)
        # iterate through all the subdirectories to find ground truth sqlite files
        newPaths = [os.path.abspath(fn) for fn in glob.glob(intersectionAbsPath+'/*_gt.sqlite', recursive=recursive)]
        gtDatabaseaAbsPaths.extend(newPaths)
        configFilename = os.path.abspath(glob.glob(intersectionAbsPath+'/*.cfg', recursive=recursive)[0])
        configFileAbsPaths.extend([configFilename]*len(newPaths))
        os.chdir(cwd)
    for gtDatabaseAbsPath, configFileAbsPath in zip(gtDatabaseaAbsPaths, configFileAbsPaths):
        gtDatabaseBasename = gtDatabaseAbsPath[:-10]
        videoFilename = gtDatabaseBasename + ".avi" # careful, it may be necessary to check video type / extension
        if not os.path.exists(videoFilename):
            print('Video file {} does not exist'.format(videoFilename))
        databaseFilename = gtDatabaseBasename + ".sqlite"
        gtDatabaseDirname = os.path.dirname(gtDatabaseAbsPath)
        homographyFilename = gtDatabaseDirname + "/homography.txt"
        maskFilename = gtDatabaseDirname + "/mask.png"
        # Skip feature tracking if the user specified to optimize only grouping parameters
        if len(para) > 8:
            # Track features
            trackingFeature(para, configFileAbsPath, videoFilename, databaseFilename, homographyFilename, maskFilename)
        # Group features
        groupFeature(para, configFileAbsPath, videoFilename, databaseFilename, homographyFilename, maskFilename)
        #load trajectory
        objects = storage.loadTrajectoriesFromSqlite(databaseFilename, 'object')
        #load ground truth
        annotations = storage.loadTrajectoriesFromSqlite(gtDatabaseAbsPath, 'object')
        # Appending negative mota because nomad minimizes the output
        matchingDistance = 5
        inter = moving.TimeInterval.unionIntervals([a.getTimeInterval() for a in annotations])
        motp, mota, mt, mme, fpt, gt, gtMatches, toMatches = moving.computeClearMOT(annotations, objects, matchingDistance, inter.first, inter.last)
        Mota.append(-mota)
    
    # Change to the previous directory
    os.chdir(cwd)

    return np.mean(Mota)

def trackingFeature(para, config, video, db, homo, mask):
    # remove previous tracking
    if os.path.exists(db):
        os.remove(db)
    # trackingfeature command parameters
    tf = ['feature-based-tracking', config, '--tf', '--video-filename', video, '--database-filename', db, '--homography-filename', homo, '--mask-filename', mask]
    # run in command line and print directly
    subprocess.check_output(tf + para)

def groupFeature(para, config, video, db, homo, mask):
    #remove previous grouping
    storage.deleteFromSqlite(db, 'object')
    #groupfeature command parameters
    gf = ['feature-based-tracking', config, '--gf', '--video-filename', video, '--database-filename', db, '--homography-filename', homo, '--mask-filename', mask]
    #run in command line and print directly
    subprocess.check_output(gf + para)  # ['--min-feature-time', 'x', '--mm-connection-distance', 'x', '--mm-segmentation-distance', 'x', '--min-nfeatures-group', 'x']


if __name__ == "__main__":
    # Load args that were given with select-arguments.py
    # with open('arguments.txt', 'r') as f:
    #     args = f.read().split('\n')
    #     intersections = args[0]
    #     optimizeGroupingOnly = eval(args[1])
    #     intersections = eval(intersections)
    
    # Just write the intersections to optimize here: do not use '~' for home directory
    intersections = ['/home/nicolas/Research/Data3/test-optimization/12-laurier']
    recursive = False

    # first and only argument should be the
    if len(sys.argv) == 2:
        para = loadParameters(sys.argv[1])
        # run process including trackingfeature, groupfeature, load groundtruth, compute mota
        print(process(para, intersections, recursive))
    else:
        print('Usage: site-parameters-optimization.py [initial-parameters text file]')
    sys.exit(0)
