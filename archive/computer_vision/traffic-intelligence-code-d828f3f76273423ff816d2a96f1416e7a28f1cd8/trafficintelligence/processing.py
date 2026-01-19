#! /usr/bin/env python
'''Algorithms to process trajectories and moving objects'''

import numpy as np

from trafficintelligence import ml, storage, utils

def extractSpeeds(objects, zone):
    speeds = {}
    objectsNotInZone = []
    import matplotlib.nxutils as nx        
    for o in objects:
        inPolygon = nx.points_inside_poly(o.getPositions().asArray().T, zone.T)
        if inPolygon.any():
            objspeeds = [o.getVelocityAt(i).norm2() for i in range(int(o.length()-1)) if inPolygon[i]]
            speeds[o.num] = np.mean(objspeeds) # km/h
        else:
            objectsNotInZone.append(o)
    return speeds, objectsNotInZone

def extractVideoSequenceSpeeds(dbFilename, siteName, nObjects, startTime, frameRate, minDuration, aggMethods, aggCentiles):
    data = []
    d = startTime.date()
    t1 = startTime.time()
    print('Extracting speed from '+dbFilename)
    aggFunctions, tmpheaders = utils.aggregationMethods(aggMethods, aggCentiles)
    objects = storage.loadTrajectoriesFromSqlite(dbFilename, 'object', nObjects)
    for o in objects:
        if o.length() > minDuration:
            row = [siteName, d, utils.framesToTime(o.getFirstInstant(), frameRate, t1), o.getUserType()]
            tmp = o.getSpeeds()
            for method,func in aggFunctions.items():
                aggSpeeds = frameRate*3.6*func(tmp)
                if method == 'centile':
                    row.extend(aggSpeeds.tolist())
                else:
                    row.append(aggSpeeds)
        data.append(row)
    return data

def learnAssignMotionPatterns(learn, assign, objects, similarities, minSimilarity, similarityFunc, minClusterSize = 0, optimizeCentroid = False, randomInitialization = False, removePrototypesAfterAssignment = False, initialPrototypes = []):
    '''Learns motion patterns

    During assignments, if using minClusterSize > 0, prototypes can change (be removed)
    The argument removePrototypesAfterAssignment indicates whether the prototypes are removed or not'''
    if len(initialPrototypes) > 0:
        initialPrototypeIndices = list(range(len(initialPrototypes)))
        trajectories = [p.getMovingObject().getPositions().asArray().T for p in initialPrototypes]
    else:
        initialPrototypeIndices = None
        trajectories = []
    trajectories.extend([o.getPositions().asArray().T for o in objects])

    if learn:
        prototypeIndices = ml.prototypeCluster(trajectories, similarities, minSimilarity, similarityFunc, optimizeCentroid, randomInitialization, initialPrototypeIndices)
    else:
        prototypeIndices = initialPrototypeIndices

    if assign:
        assignedPrototypeIndices, labels = ml.assignToPrototypeClusters(trajectories, prototypeIndices, similarities, minSimilarity, similarityFunc, minClusterSize)
        if minClusterSize > 0 and removePrototypesAfterAssignment: # use prototypeIndices anyway
            prototypeIndices = assignedPrototypeIndices
    else:
        labels = None

    return prototypeIndices, labels
    
