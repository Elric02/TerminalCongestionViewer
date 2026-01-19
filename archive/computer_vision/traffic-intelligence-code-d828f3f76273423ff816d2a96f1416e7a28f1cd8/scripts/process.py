#! /usr/bin/env python3

import sys, argparse
from pathlib import Path
from multiprocessing.pool import Pool

#import matplotlib
#atplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from trafficintelligence import storage, events, prediction, cvutils, utils, moving, processing, ml
from trafficintelligence.metadata import *

parser = argparse.ArgumentParser(description='This program manages the processing of several files based on a description of the sites and video data in an SQLite database following the metadata module.')
# input
parser.add_argument('--db', dest = 'metadataFilename', help = 'name of the metadata file', required = True)
parser.add_argument('--videos', dest = 'videoIds', help = 'indices of the video sequences', nargs = '*')
parser.add_argument('--sites', dest = 'siteIds', help = 'indices of the video sequences', nargs = '*')

# main function
parser.add_argument('--delete', dest = 'delete', help = 'data to delete', choices = ['feature', 'object', 'classification', 'interaction'])
parser.add_argument('--process', dest = 'process', help = 'data to process', choices = ['feature', 'object', 'classification', 'prototype', 'interaction'])
parser.add_argument('--display', dest = 'display', help = 'data to display (replay over video)', choices = ['feature', 'object', 'classification', 'interaction'])
parser.add_argument('--progress', dest = 'progress', help = 'information about the progress of processing', action = 'store_true')
parser.add_argument('--analyze', dest = 'analyze', help = 'data to analyze (results)', choices = ['feature', 'object', 'classification', 'interaction', 'collision-map', 'event-speed', 'event-interaction'])

# common options
parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the configuration file')
parser.add_argument('-n', dest = 'nObjects', help = 'number of objects/interactions to process', type = int)
parser.add_argument('-t', dest = 'trajectoryType', help = 'type of trajectories', choices = ['feature', 'object'], default = 'feature')
parser.add_argument('--dry', dest = 'dryRun', help = 'dry run of processing', action = 'store_true')
parser.add_argument('--nthreads', dest = 'nProcesses', help = 'number of processes to run in parallel', type = int, default = 1)
parser.add_argument('--subsample', dest = 'positionSubsamplingRate', help = 'rate of position subsampling (1 every n positions)', type = int)

### process options
# motion pattern learning and assignment
parser.add_argument('--prototype-filename', dest = 'outputPrototypeDatabaseFilename', help = 'name of the Sqlite database file to save prototypes', default = 'prototypes.sqlite')
#parser.add_argument('-i', dest = 'inputPrototypeDatabaseFilename', help = 'name of the Sqlite database file for prototypes to start the algorithm with')
parser.add_argument('--nobjects-mp', dest = 'nMPObjects', help = 'number of objects/interactions to process', type = int)
parser.add_argument('--nfeatures-per-object', dest = 'nLongestFeaturesPerObject', help = 'maximum number of features per object to load', type = int)
parser.add_argument('--epsilon', dest = 'epsilon', help = 'distance for the similarity of trajectory points', type = float)
parser.add_argument('--metric', dest = 'metric', help = 'metric for the similarity of trajectory points', default = 'cityblock') # default is manhattan distance
parser.add_argument('--minsimil', dest = 'minSimilarity', help = 'minimum similarity to put a trajectory in a cluster', type = float)
parser.add_argument('--min-cluster-size', dest = 'minClusterSize', help = 'minimum cluster size', type = int, default = 0)
#parser.add_argument('--learn', dest = 'learn', help = 'learn', action = 'store_true')
parser.add_argument('--optimize', dest = 'optimizeCentroid', help = 'recompute centroid at each assignment', action = 'store_true')
parser.add_argument('--random', dest = 'randomInitialization', help = 'random initialization of clustering algorithm', action = 'store_true')
#parser.add_argument('--similarities-filename', dest = 'similaritiesFilename', help = 'filename of the similarities')
parser.add_argument('--save-similarities', dest = 'saveSimilarities', help = 'save computed similarities (in addition to prototypes)', action = 'store_true')
parser.add_argument('--save-assignments', dest = 'saveAssignments', help = 'saves the assignments of the objects to the prototypes', action = 'store_true')
parser.add_argument('--assign', dest = 'assign', help = 'assigns the objects to the prototypes and saves the assignments', action = 'store_true')

# safety analysis
parser.add_argument('--prediction-method', dest = 'predictionMethod', help = 'prediction method (constant velocity (cvd: vector computation (approximate); cve: equation solving; cv: discrete time (approximate)), normal adaptation, point set prediction)', choices = ['cvd', 'cve', 'cv', 'na', 'ps', 'mp'])
parser.add_argument('--pet', dest = 'computePET', help = 'computes PET', action = 'store_true')
# override other tracking config, erase sqlite?


# analysis options
parser.add_argument('--output', dest = 'output', help = 'kind of output to produce (interval means)', choices = ['figure', 'interval', 'event'])
parser.add_argument('--min-duration', dest = 'minDuration', help = 'mininum duration we have to see the user or interaction to take into account in the analysis (s)', type = float)
parser.add_argument('--max-time-indicator-value', dest = 'maxTimeIndicatorValue', help = 'maximum indicator value for time indicators like PET and TTC (s)', type = float)
parser.add_argument('--max-speed-indicator-value', dest = 'maxSpeedIndicatorValue', help = 'maximum indicator value for speed indicators like individual speed statistics and speed differential (km/h)', type = float)
parser.add_argument('--interval-duration', dest = 'intervalDuration', help = 'length of time interval to aggregate data (min)', type = int, default = 15)
parser.add_argument('--aggregation', dest = 'aggMethods', help = 'aggregation method per user/interaction and per interval', choices = ['mean', 'median', 'centile'], nargs = '*', default = ['median'])
parser.add_argument('--aggregation-centiles', dest = 'aggCentiles', help = 'centile(s) to compute from the observations', nargs = '*', type = int)
parser.add_argument('--event-thresholds', dest = 'eventThresholds', help = 'threshold to count severe situations', nargs = '*', type = float)
parser.add_argument('--event-filename', dest = 'eventFilename', help = 'filename of the event data')
dpi = 150
# unit of analysis: site - camera-view

# need way of selecting sites as similar as possible to sql alchemy syntax
# override tracking.cfg from db
# manage cfg files, overwrite them (or a subset of parameters)
# delete sqlite files
# info of metadata

args = parser.parse_args()

#################################
# Data preparation
#################################
session = connectDatabase(args.metadataFilename)
parentPath = Path(args.metadataFilename).parent # files are relative to metadata location
videoSequences = []
sites = []
if args.videoIds is not None:
    for videoId in args.videoIds:
        if '-' in videoId:
            videoSequences.extend([session.query(VideoSequence).get(i) for i in moving.TimeInterval.parse(videoId)])
        else:
            videoSequences.append(session.query(VideoSequence).get(int(videoId)))
    videoSequences = [vs for vs in videoSequences if vs is not None]
    sites = set([vs.cameraView.site for vs in videoSequences])
elif args.siteIds is not None:
    for siteId in args.siteIds:
        if '-' in siteId:
            sites.extend([session.query(Site).get(i) for i in moving.TimeInterval.parse(siteId)])
        else:
            sites.append(session.query(Site).get(int(siteId)))
    sites = [s for s in sites if s is not None]
    for site in sites:
        videoSequences.extend(getSiteVideoSequences(site))
else:
    print('No video/site to process')

if args.nProcesses > 1:
    pool = Pool(args.nProcesses)

#################################
# Report progress in the processing
#################################
if args.progress: # TODO find video sequences that have null camera view, to work with them
    print('Providing information on progress of data processing')
    headers = ['site', 'vs', 'features', 'objects', 'interactions'] # todo add prototypes and object classification
    data = []
    for site in sites:
        unprocessedVideoSequences = []
        for vs in getSiteVideoSequences(site):
            if (parentPath/vs.getDatabaseFilename()).is_file(): # TODO check time of file?
                tableNames = storage.tableNames(str(parentPath.absolute()/vs.getDatabaseFilename()))
                data.append([site.name, vs.idx, 'positions' in tableNames, 'objects' in tableNames, 'interactions' in tableNames])
            else:
                unprocessedVideoSequences.append(vs)
                data.append([site.name, vs.idx, False, False, False])
        #if len(unprocessedVideoSequences):
        #    print('Site {} ({}) has {} completely unprocessed video sequences'.format (site.name, site.idx, len(unprocessedVideoSequences)))
    videoSequences = session.query(VideoSequence).filter(VideoSequence.cameraViewIdx.is_(None)).all()
    data = pd.DataFrame(data, columns = headers)
    print('-'*80)
    print('\t'+' '.join(headers[2:]))
    print('-'*80)
    for name, group in data.groupby(['site']): #.agg({'vs': 'count'}))
        n = group.vs.count()
        print('{}: {} % / {} % / {} % ({})'.format(name, 100*group.features.sum()/float(n), 100*group.objects.sum()/float(n), 100*group.interactions.sum()/float(n), n))
    print('-'*80)
    if len(videoSequences) > 0:
        print('{} video sequences without a camera view:'.format(len(videoSequences)))
        print([vs.idx for vs in videoSequences])
        print('-'*80)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(data)

#################################
# Delete
#################################
if args.delete is not None:
    if args.delete == 'feature':
        response = input('Are you sure you want to delete the tracking results (SQLite files) of all these sites (y/n)?')
        if response == 'y':
            for vs in videoSequences:
                p = parentPath.absolute()/vs.getDatabaseFilename()
                p.unlink()
    elif args.delete in ['object', 'interaction']:
        #parser.add_argument('-t', dest = 'dataType', help = 'type of the data to remove', required = True, choices = ['object','interaction', 'bb', 'pois', 'prototype'])
        for vs in videoSequences:
            storage.deleteFromSqlite(str(parentPath/vs.getDatabaseFilename()), args.delete)

#################################
# Process
#################################
if args.process in ['feature', 'object']: # tracking
    if args.nProcesses == 1:
        for vs in videoSequences:
            if not (parentPath/vs.getDatabaseFilename()).is_file() or args.process == 'object':
                if args.configFilename is None:
                    configFilename = str(parentPath/vs.cameraView.getTrackingConfigurationFilename())
                else:
                    configFilename = args.configFilename
                if vs.cameraView.cameraType is None:
                    cvutils.tracking(configFilename, args.process == 'object', str(parentPath.absolute()/vs.getVideoSequenceFilename()), str(parentPath.absolute()/vs.getDatabaseFilename()), str(parentPath.absolute()/vs.cameraView.getHomographyFilename()), str(parentPath.absolute()/vs.cameraView.getMaskFilename()), False, None, None, args.dryRun)
                else: #caution: cameratype can be not none, but without parameters for undistortion
                    cvutils.tracking(configFilename, args.process == 'object', str(parentPath.absolute()/vs.getVideoSequenceFilename()), str(parentPath.absolute()/vs.getDatabaseFilename()), str(parentPath.absolute()/vs.cameraView.getHomographyFilename()), str(parentPath.absolute()/vs.cameraView.getMaskFilename()), True, vs.cameraView.cameraType.intrinsicCameraMatrix, vs.cameraView.cameraType.distortionCoefficients, args.dryRun)
            else:
                print('SQLite already exists: {}'.format(parentPath/vs.getDatabaseFilename()))
    else:
        for vs in videoSequences:
            if not (parentPath/vs.getDatabaseFilename()).is_file() or args.process == 'object':
                if args.configFilename is None:
                    configFilename = str(parentPath/vs.cameraView.getTrackingConfigurationFilename())
                else:
                    configFilename = args.configFilename
                if vs.cameraView.cameraType is None:
                    pool.apply_async(cvutils.tracking, args = (configFilename, args.process == 'object', str(parentPath.absolute()/vs.getVideoSequenceFilename()), str(parentPath.absolute()/vs.getDatabaseFilename()), str(parentPath.absolute()/vs.cameraView.getHomographyFilename()), str(parentPath.absolute()/vs.cameraView.getMaskFilename()), False, None, None, args.dryRun))
                else:
                    pool.apply_async(cvutils.tracking, args = (configFilename, args.process == 'object', str(parentPath.absolute()/vs.getVideoSequenceFilename()), str(parentPath.absolute()/vs.getDatabaseFilename()), str(parentPath.absolute()/vs.cameraView.getHomographyFilename()), str(parentPath.absolute()/vs.cameraView.getMaskFilename()), True, vs.cameraView.cameraType.intrinsicCameraMatrix, vs.cameraView.cameraType.distortionCoefficients, args.dryRun))
            else:
                print('SQLite already exists: {}'.format(parentPath/vs.getDatabaseFilename()))
        pool.close()
        pool.join()

elif args.process == 'prototype': # motion pattern learning
    # learn by site by default -> group videos by camera view TODO
    # by default, load all objects, learn and then assign (BUT not save the assignments)
    for site in sites:
        print('Learning motion patterns for site {} ({})'.format(site.idx, site.name))
        objects = {}
        object2VideoSequences = {}
        for cv in site.cameraViews:
            for vs in cv.videoSequences:
                print('Loading '+vs.getDatabaseFilename())
                objects[vs.idx] = storage.loadTrajectoriesFromSqlite(str(parentPath/vs.getDatabaseFilename()), args.trajectoryType, args.nObjects, timeStep = args.positionSubsamplingRate, nLongestFeaturesPerObject = args.nLongestFeaturesPerObject)
                if args.trajectoryType == 'object' and args.nLongestFeaturesPerObject is not None:
                    objectsWithFeatures = objects[vs.idx]
                    objects[vs.idx] = [f for o in objectsWithFeatures for f in o.getFeatures()]
                    prototypeType = 'feature'
                else:
                    prototypeType = args.trajectoryType
                for obj in objects[vs.idx]:
                    object2VideoSequences[obj] = vs
        lcss = utils.LCSS(metric = args.metric, epsilon = args.epsilon)
        similarityFunc = lambda x,y : lcss.computeNormalized(x, y)
        trainingObjects = [o for tmpobjects in objects.values() for o in tmpobjects]
        if args.nMPObjects is not None and args.nMPObjects < len(trainingObjects):
            m = int(np.floor(float(len(trainingObjects))/args.nMPObjects))
            trainingObjects = trainingObjects[::m]
        similarities = -np.ones((len(trainingObjects), len(trainingObjects)))
        prototypeIndices, labels = processing.learnAssignMotionPatterns(True, True, trainingObjects, similarities, args.minSimilarity, similarityFunc, args.minClusterSize, args.optimizeCentroid, args.randomInitialization, True, [])
        if args.outputPrototypeDatabaseFilename is None:
            outputPrototypeDatabaseFilename = args.databaseFilename
        else:
            outputPrototypeDatabaseFilename = args.outputPrototypeDatabaseFilename
        clusterSizes = ml.computeClusterSizes(labels, prototypeIndices, -1)
        storage.savePrototypesToSqlite(str(parentPath/site.getPath()/outputPrototypeDatabaseFilename), [moving.Prototype(object2VideoSequences[trainingObjects[i]].getDatabaseFilename(False), trainingObjects[i].getNum(), prototypeType, clusterSizes[i]) for i in prototypeIndices])

elif args.process == 'interaction':
    # safety analysis TODO make function in safety analysis script
    if args.predictionMethod == 'cvd':
        predictionParameters = prediction.CVDirectPredictionParameters()
    elif args.predictionMethod == 'cve':
        predictionParameters = prediction.CVExactPredictionParameters()
    for vs in videoSequences:
        print('Processing '+vs.getDatabaseFilename())
        if args.configFilename is None:
            params = storage.ProcessParameters(str(parentPath/vs.cameraView.getTrackingConfigurationFilename()))
        else:
            params = storage.ProcessParameters(args.configFilename)  
        objects = storage.loadTrajectoriesFromSqlite(str(parentPath/vs.getDatabaseFilename()), 'object')#, args.nObjects, withFeatures = (params.useFeaturesForPrediction or predictionMethod == 'ps' or predictionMethod == 'mp'))
        interactions = events.createInteractions(objects)
        if args.nProcesses == 1:
            #print(len(interactions), args.computePET, predictionParameters, params.collisionDistance, params.predictionTimeHorizon, params.crossingZones)
            processed = events.computeIndicators(interactions, True, args.computePET, predictionParameters, params.collisionDistance, params.predictionTimeHorizon, False, False, None) # params.crossingZones
        else:
            #pool = Pool(processes = args.nProcesses)
            nInteractionPerProcess = int(np.ceil(len(interactions)/float(args.nProcesses)))
            jobs = [pool.apply_async(events.computeIndicators, args = (interactions[i*nInteractionPerProcess:(i+1)*nInteractionPerProcess], True, args.computePET, predictionParameters, params.collisionDistance, params.predictionTimeHorizon, False, False, None)) for i in range(args.nProcesses)] # params.crossingZones
            processed = []
            for job in jobs:
                processed += job.get()
            #pool.close()
        storage.saveIndicatorsToSqlite(str(parentPath/vs.getDatabaseFilename()), processed)
            
#################################
# Analyze
#################################
if args.analyze == 'object':
    # user speeds, accelerations
    # aggregation per site
    if args.eventFilename is None:
        print('Missing output filename (event-filename). Exiting')
        sys.exit(0)
    data = [] # list of observation per site-user with time
    headers = ['site', 'date', 'time', 'user_type']
    aggFunctions, tmpheaders = utils.aggregationMethods(args.aggMethods, args.aggCentiles)
    headers.extend(tmpheaders)
    if args.nProcesses == 1:
        for vs in videoSequences:
            data.extend(processing.extractVideoSequenceSpeeds(str(parentPath/vs.getDatabaseFilename()), vs.cameraView.site.name, args.nObjects, vs.startTime, vs.cameraView.cameraType.frameRate, vs.cameraView.cameraType.frameRate*args.minDuration, args.aggMethods, args.aggCentiles))
    else:
        jobs = [pool.apply_async(processing.extractVideoSequenceSpeeds, args = (str(parentPath/vs.getDatabaseFilename()), vs.cameraView.site.name, args.nObjects, vs.startTime, vs.cameraView.cameraType.frameRate, vs.cameraView.cameraType.frameRate*args.minDuration, args.aggMethods, args.aggCentiles)) for vs in videoSequences]
        for job in jobs:
            data.extend(job.get())
        pool.close()
    data = pd.DataFrame(data, columns = headers)
    if args.output == 'figure':
        for name in headers[4:]:
            plt.ioff()
            plt.figure()
            plt.boxplot([data.loc[data['site']==site.name, name] for site in sites], labels = [site.name for site in sites])
            plt.ylabel(name+' Speeds (km/h)')
            plt.savefig(name.lower()+'-speeds.png', dpi=dpi)
            plt.close()
    elif args.output == 'event':
        data.to_csv(args.eventFilename, index = False)

if args.analyze == 'interaction': # redo as for object, export in dataframe all interaction data
    indicatorIds = [2,5,7,10]
    #maxIndicatorValue = {2: float('inf'), 5: float('inf'), 7:10., 10:10.}
    data = [] # list of observation per site-user with time
    headers = ['site', 'date', 'time', events.Interaction.indicatorNames[10].replace(' ','-')] # user types?
    aggFunctions, tmpheaders = utils.aggregationMethods(args.aggMethods, args.aggCentiles)
    nAggFunctions = len(tmpheaders)
    indicatorUnits = [events.Interaction.indicatorUnits[10]] # for PET above
    for i in indicatorIds[:3]:
        for h in tmpheaders:
            headers.append(events.Interaction.indicatorNames[i].replace(' ','-')+'-'+h)
            indicatorUnits.append(events.Interaction.indicatorUnits[i])
    for vs in videoSequences:
        print('Extracting SMoS from '+vs.getDatabaseFilename())
        interactions = storage.loadInteractionsFromSqlite(str(parentPath/vs.getDatabaseFilename()))
        minDuration = vs.cameraView.cameraType.frameRate*args.minDuration
        conversionFactors = {2: 1., 5: 3.6*vs.cameraView.cameraType.frameRate, 7:1./vs.cameraView.cameraType.frameRate, 10:1./vs.cameraView.cameraType.frameRate}
        for inter in interactions:
            if inter.length() > minDuration:
                d = vs.startTime.date()
                t = vs.startTime.time()
                row = [vs.cameraView.site.name, d, utils.framesToTime(inter.getFirstInstant(), vs.cameraView.cameraType.frameRate, t)]
                pet = inter.getIndicator(events.Interaction.indicatorNames[10])
                if pet is None:
                    row.append(None)
                else:
                    row.append(conversionFactors[10]*pet.getValues()[0])
                for i in indicatorIds[:3]:
                    indic = inter.getIndicator(events.Interaction.indicatorNames[i])
                    if indic is not None:
                        #v = indic.getMostSevereValue()*
                        tmp = list(indic.values.values())
                        for method,func in aggFunctions.items():
                            agg = conversionFactors[i]*func(tmp)
                            if method == 'centile':
                                row.extend(agg.tolist())
                            else:
                                row.append(agg)
                    else:
                        row.extend([None]*nAggFunctions)
                data.append(row)
    data = pd.DataFrame(data, columns = headers)
    if args.output == 'figure':
        plt.ioff()
        for i, indic in enumerate(headers[3:]):
            if 'Time' in indic and args.maxTimeIndicatorValue is not None:
                tmp = data.loc[data[indic] < args.maxTimeIndicatorValue, ['site', indic]]
            elif 'Speed' in indic and args.maxSpeedIndicatorValue is not None:
                tmp = data.loc[data[indic] < args.maxSpeedIndicatorValue, ['site', indic]]
            else:
                tmp = data[['site', indic]]
            plt.figure()
            tmp.boxplot(indic, 'site')
            # plt.boxplot(tmp, labels = [session.query(Site).get(siteId).name for siteId in indicators])
            plt.ylabel(indic+' ('+indicatorUnits[i]+')')
            plt.savefig('boxplot-sites-'+indic+'.pdf')#, dpi=150)
            plt.close()
            plt.figure()
            for site in sorted(tmp.site.unique()):
                x = sorted(tmp.loc[tmp.site == site, indic])
                plt.plot(x, np.arange(1,len(x)+1)/len(x), label=site)
            plt.legend()
            plt.title('Cumulative Distribution Function by Site')
            plt.xlabel(indic+' ('+indicatorUnits[i]+')')
            plt.savefig('cdf-sites-'+indic+'.pdf')
            plt.close()
    elif args.output == 'event':
        data.to_csv(args.eventFilename, index = False)

if args.analyze == 'collision-map':
    predictionParameters = prediction.CVExactPredictionParameters()
    data = []
    for vs in videoSequences:
        print('Extracting potential collision points from '+vs.getDatabaseFilename())
        interactions = storage.loadInteractionsFromSqlite(str(parentPath/vs.getDatabaseFilename()))
        objects = storage.loadTrajectoriesFromSqlite(str(parentPath/vs.getDatabaseFilename()), 'object')
        params = storage.ProcessParameters(str(parentPath/vs.cameraView.getTrackingConfigurationFilename()))
        minDuration = vs.cameraView.cameraType.frameRate*args.minDuration
        maxTimeIndicatorValue = vs.cameraView.cameraType.frameRate*args.maxTimeIndicatorValue
        for inter in interactions:
            if inter.length() > minDuration:
                ttc = inter.getIndicator(events.Interaction.indicatorNames[7])
                if ttc is not None:
                    t = min(ttc.values, key = ttc.values.get)
                    if args.maxTimeIndicatorValue is None or ttc.values[t] < maxTimeIndicatorValue:
                        inter.setRoadUsers(objects)
                        cps, _ = predictionParameters.computeCrossingsCollisionsAtInstant(t, inter.roadUser1, inter.roadUser2, params.collisionDistance, params.predictionTimeHorizon)
                        data.append([vs.cameraView.site.name, cps[0].x, cps[0].y, cps[0].indicator])
    data = pd.DataFrame(data, columns = ['site', 'x', 'y', 'ttc'])
    margin = 0.1
    for site in data.site.unique():
        s = session.query(Site).filter(Site.name.like('%'+site+'%')).first()
        img = plt.imread(str(parentPath/s.getMapImageFilename()))
        tmp = data[data.site == site].copy()
        tmp.x = tmp.x/s.nUnitsPerPixel
        tmp.y = tmp.y/s.nUnitsPerPixel
        h, w, _ = img.shape
        tmp = tmp[(tmp.x>-margin*w) & (tmp.x < (1+margin)*w) & (tmp.y > -margin*h) & (tmp.y < (1+margin)*h)]
        plt.figure()
        plt.imshow(img)
        plt.hexbin(tmp.x, tmp.y, alpha = 0.5, edgecolors = 'face', mincnt=1, gridsize=50)
        plt.title('Density of Potential Collision Points at Site '+site)
        plt.colorbar()
        plt.axis('equal')
        plt.savefig('collision-map-'+site+'.pdf')
        #plt.close()
        
if args.analyze == 'event-speed': # aggregate event data by 15 min interval (args.intervalDuration), count events with thresholds
    data = pd.read_csv(args.eventFilename, parse_dates = [2])
    #data = pd.read_csv('./speeds.csv', converters = {'time': lambda s: datetime.datetime.strptime(s, "%H:%M:%S").time()}, nrows = 5000)
    # create time for end of each 15 min, then group by, using the agg method for each data column
    headers = ['site', 'date', 'intervalend15', 'duration', 'count']
    aggFunctions, tmpheaders = utils.aggregationMethods(args.aggMethods, args.aggCentiles)
    dataColumns = list(data.columns[4:])
    print(dataColumns)
    for h in dataColumns:
        for h2 in tmpheaders:
            headers.append(h+'-'+h2)
    if args.eventThresholds is not None:
        for h in dataColumns:
            for t in args.eventThresholds:
                headers.append('n-{}-{}'.format(h, t))
    data['intervalend15'] = data.time.apply(lambda t: (pd.Timestamp(year = t.year, month = t.month, day = t.day,hour = t.hour, minute = (t.minute // args.intervalDuration)*args.intervalDuration)+pd.Timedelta(minutes = 15)).time())
    outputData = []
    for name, group in data.groupby(['site', 'date', 'intervalend15']):
        row = []
        row.extend(name)
        groupStartTime = group.time.min()
        groupEndTime = group.time.max()
        row.append((groupEndTime.minute+1-groupStartTime.minute) % 60)#(name[2].minute*60+name[2].second-groupStartTime.minute*60+groupStartTime.second) % 3600)
        row.append(len(group))
        for h in dataColumns:
            for method,func in aggFunctions.items():
                aggregated = func(group[h])
                if method == 'centile':
                    row.extend(aggregated)
                else:
                    row.append(aggregated)
        if args.eventThresholds is not None:
            for h in dataColumns:
                for t in args.eventThresholds:
                    row.append((group[h] > t).sum())
        outputData.append(row)
    pd.DataFrame(outputData, columns = headers).to_csv(utils.removeExtension(args.eventFilename)+'-aggregated.csv', index = False)

elif args.analyze == 'event-interaction': # aggregate event data by 15 min interval (args.intervalDuration), count events with thresholds
    data = pd.read_csv(args.eventFilename, parse_dates = [2])
    headers = ['site', 'date', 'intervalend15', 'duration', 'count']
    aggFunctions, tmpheaders = utils.aggregationMethods(args.aggMethods, args.aggCentiles)
    nAggFunctions = len(tmpheaders)
    dataColumns = list(data.columns[3:])
    for h in dataColumns:
        if not 'speed' in h.lower(): # proximity indicators are reversed, taking 85th centile of this column will yield the 15th centile (which we have to take the opposite again)
            data[h] = -data[h]
    for h in dataColumns:
        for h2 in tmpheaders:
            headers.append(h+'-'+h2)
    for h,t in zip(dataColumns, args.eventThresholds): # each threshold in this case applies to one indicator
        headers.append('n-{}-{}'.format(h, t))
    data['intervalend15'] = data.time.apply(lambda t: (pd.Timestamp(year = t.year, month = t.month, day = t.day,hour = t.hour, minute = (t.minute // args.intervalDuration)*args.intervalDuration)+pd.Timedelta(minutes = 15)).time())
    outputData = []
    for name, group in data.groupby(['site', 'date', 'intervalend15']):
        row = []
        row.extend(name)
        groupStartTime = group.time.min()
        groupEndTime = group.time.max()
        row.append((groupEndTime.minute+1-groupStartTime.minute) % 60)#(name[2].minute*60+name[2].second-groupStartTime.minute*60+groupStartTime.second) % 3600)
        row.append(len(group))
        for h in dataColumns:
            for method,func in aggFunctions.items():
                tmp = group.loc[~group[h].isna(), h]
                if len(tmp)>0:
                    aggregated = func(tmp) # todo invert if the resulting stat is negative
                    if method == 'centile':
                        row.extend(np.abs(aggregated))
                    else:
                        row.append(np.abs(aggregated))
                else:
                    row.extend([None]*nAggFunctions)
        for h,t in zip(dataColumns, args.eventThresholds): # each threshold in this case applies to one indicator
            if 'speed' in h.lower():
                row.append((group[h] > t).sum())
            else:
                row.append((group[h] > -t).sum()) # take larger than than negative threshold for proximity indicators
        outputData.append(row)
    pd.DataFrame(outputData, columns = headers).to_csv(utils.removeExtension(args.eventFilename)+'-aggregated.csv', index = False)
