#! /usr/bin/env python3

import sys, argparse

import numpy as np
import matplotlib.pyplot as plt

from trafficintelligence import ml, utils, storage, moving, processing

parser = argparse.ArgumentParser(description='''The program clusters trajectories, each cluster being represented by a trajectory. It can either work on the same dataset (database) or different ones, but only does learning or assignment at a time to avoid issues''') #, epilog = ''
#parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the configuration file')
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file', required = True)
parser.add_argument('-o', dest = 'outputPrototypeDatabaseFilename', help = 'name of the Sqlite database file to save prototypes')
parser.add_argument('-i', dest = 'inputPrototypeDatabaseFilename', help = 'name of the Sqlite database file for prototypes to start the algorithm with')
parser.add_argument('-t', dest = 'trajectoryType', help = 'type of trajectories to process', choices = ['feature', 'object'], default = 'feature')
parser.add_argument('--nfeatures-per-object', dest = 'nLongestFeaturesPerObject', help = 'maximum number of features per object to load', type = int)
parser.add_argument('-n', dest = 'nObjects', help = 'number of the object or feature trajectories to load', type = int, default = None)
parser.add_argument('-e', dest = 'epsilon', help = 'distance for the similarity of trajectory points', type = float, required = True)
parser.add_argument('--metric', dest = 'metric', help = 'metric for the similarity of trajectory points', default = 'cityblock') # default is manhattan distance
parser.add_argument('-s', dest = 'minSimilarity', help = 'minimum similarity to put a trajectory in a cluster', type = float, required = True)
#parser.add_argument('-c', dest = 'minClusterSize', help = 'minimum cluster size', type = int, default = 0)
parser.add_argument('--learn', dest = 'learn', help = 'learn', action = 'store_true')
parser.add_argument('--optimize', dest = 'optimizeCentroid', help = 'recompute centroid at each assignment', action = 'store_true')
parser.add_argument('--random', dest = 'randomInitialization', help = 'random initialization of clustering algorithm', action = 'store_true')
parser.add_argument('--subsample', dest = 'positionSubsamplingRate', help = 'rate of position subsampling (1 every n positions)', type = int)
parser.add_argument('--display', dest = 'display', help = 'display trajectories', action = 'store_true')
parser.add_argument('--similarities-filename', dest = 'similaritiesFilename', help = 'filename of the similarities')
parser.add_argument('--save-similarities', dest = 'saveSimilarities', help = 'save computed similarities (in addition to prototypes)', action = 'store_true')
parser.add_argument('--save-assignments', dest = 'saveAssignments', help = 'saves the assignments of the objects to the prototypes', action = 'store_true')
parser.add_argument('--assign', dest = 'assign', help = 'assigns the objects to the prototypes and saves the assignments', action = 'store_true')

args = parser.parse_args()

# use cases
# 1. learn proto from one file, save in same or another
# 2. load proto, load objects (from same or other db), update proto matchings, save proto
# TODO 3. on same dataset, learn and assign trajectories (could be done with min cluster size)
# TODO? 4. when assigning, allow min cluster size only to avoid assigning to small clusters (but prototypes are not removed even if in small clusters, can be done after assignment with nmatchings)

# TODO add possibility to cluster with velocities
# TODO add possibility to load all trajectories and use minclustersize

if args.learn and args.assign:
    print('Cannot learn and assign simultaneously')
    sys.exit(0)

objects = storage.loadTrajectoriesFromSqlite(args.databaseFilename, args.trajectoryType, args.nObjects, timeStep = args.positionSubsamplingRate, nLongestFeaturesPerObject = args.nLongestFeaturesPerObject)
if args.trajectoryType == 'object' and args.nLongestFeaturesPerObject is not None:
    objectsWithFeatures = objects
    objects = [f for o in objectsWithFeatures for f in o.getFeatures()]
    prototypeType = 'feature'
else:
    prototypeType = args.trajectoryType

# load initial prototypes, if any    
if args.inputPrototypeDatabaseFilename is not None:
    initialPrototypes = storage.loadPrototypesFromSqlite(args.inputPrototypeDatabaseFilename, True)
else:
    initialPrototypes = []

lcss = utils.LCSS(metric = args.metric, epsilon = args.epsilon)
similarityFunc = lambda x,y : lcss.computeNormalized(x, y)
nTrajectories = len(initialPrototypes)+len(objects)
if args.similaritiesFilename is not None:
    similarities = np.loadtxt(args.similaritiesFilename)
if args.similaritiesFilename is None or similarities.shape[0] != nTrajectories or similarities.shape[1] != nTrajectories:
    similarities = -np.ones((nTrajectories, nTrajectories))

prototypeIndices, labels = processing.learnAssignMotionPatterns(args.learn, args.assign, objects, similarities, args.minSimilarity, similarityFunc, 0, args.optimizeCentroid, args.randomInitialization, False, initialPrototypes)

if args.learn:# and not args.assign:
    prototypes = []
    for i in prototypeIndices:
        if i<len(initialPrototypes):
            prototypes.append(initialPrototypes[i])
        else:
            prototypes.append(moving.Prototype(args.databaseFilename, objects[i-len(initialPrototypes)].getNum(), prototypeType))

    if args.outputPrototypeDatabaseFilename is None:
        outputPrototypeDatabaseFilename = args.databaseFilename
    else:
        outputPrototypeDatabaseFilename = args.outputPrototypeDatabaseFilename
        if args.inputPrototypeDatabaseFilename == args.outputPrototypeDatabaseFilename:
            storage.deleteFromSqlite(args.outputPrototypeDatabaseFilename, 'prototype')
    storage.savePrototypesToSqlite(outputPrototypeDatabaseFilename, prototypes)
    if args.display:
        plt.figure()
        for p in prototypes:
            p.getMovingObject().plot()
        plt.axis('equal')
        plt.show()

if args.assign: # not args.learn and  no modification to prototypes, can work with initialPrototypes
    clusterSizes = ml.computeClusterSizes(labels, prototypeIndices, -1)
    for i in prototypeIndices:
        nMatchings = clusterSizes[i]-1 # external prototypes
        if initialPrototypes[i].nMatchings is None:
            initialPrototypes[i].nMatchings = nMatchings
        else:
            initialPrototypes[i].nMatchings += nMatchings
    if args.outputPrototypeDatabaseFilename is None:
        outputPrototypeDatabaseFilename = args.databaseFilename
    else:
        outputPrototypeDatabaseFilename = args.outputPrototypeDatabaseFilename
    storage.setPrototypeMatchingsInSqlite(outputPrototypeDatabaseFilename, initialPrototypes)
    if args.saveAssignments:
        if args.trajectoryType == 'object' and args.nLongestFeaturesPerObject is not None:
            # consider that the object is assigned through its longest features
            # issues are inconsistencies in the number of matchings per prototype and display (will display features, not objects)
            objectNumbers = []
            objectLabels = []
            i = 0
            for obj in objectsWithFeatures:
                objLabels = []
                for f in obj.getFeatures():
                    if f == objects[i]:
                        objLabels.append(labels[i+len(initialPrototypes)])
                        i += 1
                    else:
                        print('Issue with obj {} and feature {} (trajectory {})'.format(obj.getNum(), f.getNum(), i))
                objectLabels.append(utils.mostCommon(objLabels))
                objectNumbers.append(obj.getNum())
            storage.savePrototypeAssignmentsToSqlite(args.databaseFilename, objectNumbers, 'object', objectLabels, initialPrototypes)
        else:
            storage.savePrototypeAssignmentsToSqlite(args.databaseFilename, [obj.getNum() for obj in objects], args.trajectoryType, labels[len(initialPrototypes):], initialPrototypes)
    if args.display:
        plt.figure()
        for i,o in enumerate(objects):
            if labels[i+len(initialPrototypes)] < 0:
                o.plot('kx-')
            else:
                o.plot(utils.colors[labels[i+len(initialPrototypes)]])
        for i,p in enumerate(initialPrototypes):
            p.getMovingObject().plot(utils.colors[i]+'o')
        plt.axis('equal')
        plt.show()

if (args.learn or args.assign) and args.saveSimilarities:
    if args.similaritiesFilename is not None:
        np.savetxt(args.similaritiesFilename, similarities, '%.4f')
    else:
        np.savetxt(utils.removeExtension(args.databaseFilename)+'-prototype-similarities.txt.gz', similarities, '%.4f')
