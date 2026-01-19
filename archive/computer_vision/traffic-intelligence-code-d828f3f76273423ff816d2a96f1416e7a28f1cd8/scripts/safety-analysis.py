#! /usr/bin/env python3

import sys, argparse, random
from multiprocessing import Pool

import matplotlib.pyplot as plt
import numpy as np

from trafficintelligence import storage, prediction, events, moving

# todo: very slow if too many predicted trajectories
# add computation of probality of unsucessful evasive action

parser = argparse.ArgumentParser(description='The program processes indicators for all pairs of road users in the scene')
parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the configuration file', required = True)
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file (overrides the configuration file)')
parser.add_argument('-n', dest = 'nObjects', help = 'number of objects to analyse', type = int)
# TODO analyze only 
parser.add_argument('--prediction-method', dest = 'predictionMethod', help = 'prediction method (constant velocity (cvd: vector computation (approximate); cve: equation solving; cv: discrete time (approximate)), normal adaptation, point set prediction)', choices = ['cvd', 'cve', 'cv', 'na', 'ps', 'mp'])
parser.add_argument('-p', dest = 'prototypeDatabaseFilename', help = 'name of the database containing the prototypes')
parser.add_argument('-c', dest = 'minPrototypeNMatchings', help = 'minimum number of matchings per prototype', type = int, default = 1)
# parser.add_argument('--categorize', dest = 'categorize', help = 'computes interaction categories', action = 'store_true') TODO, add angle parameters in tracking.cfg - the safety analysis parameters should probably be spun off tracking.cfg
parser.add_argument('--no-motion-prediction', dest = 'noMotionPrediction', help = 'does not compute indicators like TTC depending on motion prediction', action = 'store_true')
parser.add_argument('--pet', dest = 'computePET', help = 'computes PET', action = 'store_true')
parser.add_argument('--display-cp', dest = 'displayCollisionPoints', help = 'display collision points', action = 'store_true')
parser.add_argument('--nthreads', dest = 'nProcesses', help = 'number of processes to run in parallel', type = int, default = 1)
args = parser.parse_args()

params = storage.ProcessParameters(args.configFilename)

# selected database to overide the configuration file
if args.databaseFilename is not None:
    params.databaseFilename = args.databaseFilename

# parameters for prediction methods
if args.predictionMethod is not None:
    predictionMethod = args.predictionMethod
else:
    predictionMethod = params.predictionMethod

def accelerationDistribution(): 
    return random.triangular(-params.maxNormalAcceleration, params.maxNormalAcceleration, 0.)
def steeringDistribution():
    return random.triangular(-params.maxNormalSteering, params.maxNormalSteering, 0.)

if predictionMethod == 'cvd':
    predictionParameters = prediction.CVDirectPredictionParameters()
if predictionMethod == 'cve':
    predictionParameters = prediction.CVExactPredictionParameters()
elif predictionMethod == 'cv':
    predictionParameters = prediction.ConstantPredictionParameters(params.maxPredictedSpeed)
elif predictionMethod == 'na':
    predictionParameters = prediction.NormalAdaptationPredictionParameters(params.maxPredictedSpeed,
                                                                           params.nPredictedTrajectories, 
                                                                           accelerationDistribution,
                                                                           steeringDistribution,
                                                                           params.useFeaturesForPrediction)
elif predictionMethod == 'ps':
    predictionParameters = prediction.PointSetPredictionParameters(params.maxPredictedSpeed)
elif predictionMethod == 'mp':
    if args.prototypeDatabaseFilename is None:
        prototypes = storage.loadPrototypesFromSqlite(params.databaseFilename)
    else:
        prototypes = storage.loadPrototypesFromSqlite(args.prototypeDatabaseFilename)
    if args.minPrototypeNMatchings > 0:
        prototypes = [p for p in prototypes if p.getNMatchings() >= args.minPrototypeNMatchings]
    else:
        nProto0Matching = 0
        for p in prototypes:
            if p.getNMatchings() == 0:
                nProto0Matching += 1
                print("Prototype {} has 0 matchings".format(p))
        if len(prototypes) == 0 or nProto0Matching > 0:
            print('Database has {} prototypes without any matching. Exiting'.format(nProto0Matching))
            sys.exit()
    for p in prototypes:
        p.getMovingObject().computeCumulativeDistances()
    predictionParameters = prediction.PrototypePredictionParameters(prototypes, params.nPredictedTrajectories, params.maxLcssDistance, params.minLcssSimilarity, params.lcssMetric, params.minFeatureTime, params.constantSpeedPrototypePrediction, params.useFeaturesForPrediction)
# else:
# no else required, since parameters is required as argument

# evasiveActionPredictionParameters = prediction.EvasiveActionPredictionParameters(params.maxPredictedSpeed, 
#                                                                                  params.nPredictedTrajectories, 
#                                                                                  params.minExtremeAcceleration,
#                                                                                  params.maxExtremeAcceleration,
#                                                                                  params.maxExtremeSteering,
#                                                                                  params.useFeaturesForPrediction)

objects = storage.loadTrajectoriesFromSqlite(params.databaseFilename, 'object', args.nObjects, withFeatures = (params.useFeaturesForPrediction or predictionMethod == 'ps' or predictionMethod == 'mp'))

interactions = events.createInteractions(objects)
if args.nProcesses == 1:
    processed = events.computeIndicators(interactions, not args.noMotionPrediction, args.computePET, predictionParameters, params.collisionDistance, False, params.predictionTimeHorizon, params.crossingZones, False, None)
else:
    pool = Pool(processes = args.nProcesses)
    nInteractionPerProcess = int(np.ceil(len(interactions)/float(args.nProcesses)))
    jobs = [pool.apply_async(events.computeIndicators, args = (interactions[i*nInteractionPerProcess:(i+1)*nInteractionPerProcess], not args.noMotionPrediction, args.computePET, predictionParameters, params.collisionDistance, False, params.predictionTimeHorizon, params.crossingZones, False, None)) for i in range(args.nProcesses)]
    processed = []
    for job in jobs:
        processed += job.get()
    pool.close()
storage.saveIndicatorsToSqlite(params.databaseFilename, processed)

if args.displayCollisionPoints:
    plt.figure()
    allCollisionPoints = []
    for inter in processed:
        for collisionPoints in inter.collisionPoints.values():
            allCollisionPoints += collisionPoints
    moving.Point.plotAll(allCollisionPoints)
    plt.axis('equal')
    
