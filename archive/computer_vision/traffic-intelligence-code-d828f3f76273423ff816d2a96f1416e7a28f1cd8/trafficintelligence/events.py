#! /usr/bin/env python
'''Libraries for events
Interactions, pedestrian crossing...'''

from trafficintelligence import moving, prediction, indicators, utils, cvutils, ml
from trafficintelligence.base import VideoFilenameAddable

import numpy as np
from matplotlib.pyplot import subplot, figure, ylabel

import multiprocessing
import itertools, logging


def findRoute(prototypes,objects,i,j,noiseEntryNums,noiseExitNums,minSimilarity= 0.3, spatialThreshold=1.0, delta=180):
    if i[0] not in noiseEntryNums: 
        prototypesRoutes= [ x for x in sorted(prototypes.keys()) if i[0]==x[0]]
    elif i[1] not in noiseExitNums:
        prototypesRoutes=[ x for x in sorted(prototypes.keys()) if i[1]==x[1]]
    else:
        prototypesRoutes=[x for x in sorted(prototypes.keys())]
    routeSim={}
    lcss = utils.LCSS(similarityFunc=lambda x,y: (distanceForLCSS(x,y) <= spatialThreshold),delta=delta)
    for y in prototypesRoutes: 
        if y in prototypes:
            prototypesIDs=prototypes[y]
            similarity=[]
            for x in prototypesIDs:
                s=lcss.computeNormalized(objects[j].positions, objects[x].positions)
                similarity.append(s)
            routeSim[y]=max(similarity)
    route=max(routeSim, key=routeSim.get)
    if routeSim[route]>=minSimilarity:
        return route
    else:
        return i

def getRoute(obj,prototypes,objects,noiseEntryNums,noiseExitNums,useDestination=True):
    route=(obj.startRouteID,obj.endRouteID)
    if useDestination:
        if route not in prototypes:
            route= findRoute(prototypes,objects,route,obj.getNum(),noiseEntryNums,noiseExitNums)
    return route

class Interaction(moving.STObject, VideoFilenameAddable):
    '''Class for an interaction between two road users 
    or a road user and an obstacle
    
    link to the moving objects
    contains the indicators in a dictionary with the names as keys
    '''

    categories = {'headon': 0,
                  'rearend': 1,
                  'side': 2,
                  'parallel': 3,
                  'stationary': 4}

    indicatorNames = ['Collision Course Dot Product',
                      'Collision Course Angle',
                      'Distance',
                      'Minimum Distance',
                      'Velocity Angle',
                      'Speed Differential',
                      'Collision Probability',
                      'Time to Collision', # 7
                      'Probability of Successful Evasive Action',
                      'predicted Post Encroachment Time',
                      'Post Encroachment Time']

    indicatorNameToIndices = utils.inverseEnumeration(indicatorNames)

    indicatorShortNames = ['CCDP',
                           'CCA',
                           'Dist',
                           'MinDist',
                           'VA',
                           'SD',
                           'PoC',
                           'TTC',
                           'P(SEA)',
                           'pPET',
                           'PET']

    indicatorUnits = ['',
                      'rad',
                      'm',
                      'm',
                      'rad',
                      'km/h',
                      '',
                      's',
                      '',
                      's',
                      's']

    mostSevereIsMinIndicators = ['Distance', 'Time to Collision', 'predicted Post Encroachment Time']

    def __init__(self, num = None, timeInterval = None, roaduserNum1 = None, roaduserNum2 = None, roadUser1 = None, roadUser2 = None):
        moving.STObject.__init__(self, num, timeInterval)
        if timeInterval is None and roadUser1 is not None and roadUser2 is not None:
            self.timeInterval = roadUser1.commonTimeInterval(roadUser2)
        self.roadUser1 = roadUser1
        self.roadUser2 = roadUser2
        if roaduserNum1 is not None and roaduserNum2 is not None:
            self.roadUserNumbers = set([roaduserNum1, roaduserNum2])
        elif roadUser1 is not None and roadUser2 is not None:
            self.roadUserNumbers = set([roadUser1.getNum(), roadUser2.getNum()])
        else:
            self.roadUserNumbers = None
        self.indicators = {}
         # list for collison points and crossing zones
        self.collisionPoints = None
        self.crossingZones = None

    def getRoadUserNumbers(self):
        return self.roadUserNumbers

    def setRoadUsers(self, objects):
        tmpobjects = {o.getNum():o for o in objects}
        i, j = self.roadUserNumbers
        if i in tmpobjects:
            self.roadUser1 = tmpobjects[i]
        if j in tmpobjects:
            self.roadUser2 = tmpobjects[j]

    def getIndicator(self, indicatorName):
        return self.indicators.get(indicatorName, None)

    def resetIndicator(self, indicatorName):
        if indicatorName in self.indicators:
            del self.indicators[indicatorName]

    def addIndicator(self, indicator):
        if indicator is not None:
            self.indicators[indicator.name] = indicator

    def getIndicatorValueAtInstant(self, indicatorName, instant):
        indicator = self.getIndicator(indicatorName)
        if indicator is not None:
            return indicator[instant]
        else:
            return None

    def getIndicatorValuesAtInstant(self, instant):
        '''Returns list of indicator values at instant
        as dict (with keys from indicators dict)'''
        values = {}
        for k, indicator in self.indicators.items():
            values[k] = indicator[instant]
        return values
        
    def plot(self, options = '', withOrigin = False, timeStep = 1, withFeatures = False, restricted = True, **kwargs):
        if restricted:
            self.roadUser1.getObjectInTimeInterval(self.timeInterval).plot(options, withOrigin, timeStep, withFeatures, **kwargs)
            self.roadUser2.getObjectInTimeInterval(self.timeInterval).plot(options, withOrigin, timeStep, withFeatures, **kwargs)
        else:
            self.roadUser1.plot(options, withOrigin, timeStep, withFeatures, **kwargs)
            self.roadUser2.plot(options, withOrigin, timeStep, withFeatures, **kwargs)

    def plotOnWorldImage(self, nPixelsPerUnitDistance, options = '', withOrigin = False, timeStep = 1, **kwargs):
        self.roadUser1.plotOnWorldImage(nPixelsPerUnitDistance, options, withOrigin, timeStep, **kwargs)
        self.roadUser2.plotOnWorldImage(nPixelsPerUnitDistance, options, withOrigin, timeStep, **kwargs)

    def plotIndicators(self, _indicatorNames = indicatorNames):
        nrows = int(np.ceil(len(_indicatorNames)/2))
        ncols = 2
        #subplot(nrows, 2)
        for i, indicatorName in enumerate(_indicatorNames):
            if i==0:
                ax = subplot(nrows, ncols, i+1)
            else:
                subplot(nrows, ncols, i+1, sharex = ax)
            ind = self.getIndicator(indicatorName)
            if ind is not None:
                ind.plot()
                ylabel(indicatorName)
        
    def play(self, videoFilename, homography = None, undistort = False, intrinsicCameraMatrix = None, distortionCoefficients = None, undistortedImageMultiplication = 1., allUserInstants = False):
        if self.roadUser1 is not None and self.roadUser2 is not None:
            if allUserInstants:
                firstFrameNum = min(self.roadUser1.getFirstInstant(), self.roadUser2.getFirstInstant())
                lastFrameNum = max(self.roadUser1.getLastInstant(), self.roadUser2.getLastInstant())
            else:
                firstFrameNum = self.getFirstInstant()
                lastFrameNum = self.getLastInstant()
            cvutils.displayTrajectories(videoFilename, [self.roadUser1, self.roadUser2], homography = homography, firstFrameNum = firstFrameNum, lastFrameNumArg = lastFrameNum, undistort = undistort, intrinsicCameraMatrix = intrinsicCameraMatrix, distortionCoefficients = distortionCoefficients, undistortedImageMultiplication = undistortedImageMultiplication)
        else:
            print('Please set the interaction road user attributes roadUser1 and roadUser1 through the method setRoadUsers')

    def computeIndicators(self):
        '''Computes all cinematic indicators but the expensive safety indicators (TTC, PET)'''
        collisionCourseDotProducts = {}
        collisionCourseAngles = {}
        velocityAngles = {}
        distances = {}
        speedDifferentials = {}
        for instant in self.timeInterval:
            deltap = self.roadUser1.getPositionAtInstant(instant)-self.roadUser2.getPositionAtInstant(instant)
            v1 = self.roadUser1.getVelocityAtInstant(instant)
            v2 = self.roadUser2.getVelocityAtInstant(instant)
            deltav = v2-v1
            v1Norm = v1.norm2()
            v2Norm = v2.norm2()
            if v1Norm != 0. and v2Norm != 0.:
                velocityAngles[instant] = np.arccos(max(-1, min(1, moving.Point.dot(v1, v2)/(v1Norm*v2Norm))))
            collisionCourseDotProducts[instant] = moving.Point.dot(deltap, deltav)
            distances[instant] = deltap.norm2()
            speedDifferentials[instant] = deltav.norm2()
            if distances[instant] != 0 and speedDifferentials[instant] != 0:
                collisionCourseAngles[instant] = np.arccos(max(-1, min(1, collisionCourseDotProducts[instant]/(distances[instant]*speedDifferentials[instant])))) # avoid values slightly higher than 1.0

        self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[0], collisionCourseDotProducts))
        self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[1], collisionCourseAngles))
        self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[2], distances, mostSevereIsMax = False))
        self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[4], velocityAngles))
        self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[5], speedDifferentials))

        # if we have features, compute other indicators
        if self.roadUser1.hasFeatures() and self.roadUser2.hasFeatures() and len(self.roadUser1.getFeatures())>1 and len(self.roadUser2.getFeatures())>1:
            minDistances={}
            for instant in self.timeInterval:
                minDistances[instant] = moving.MovingObject.minDistance(self.roadUser1, self.roadUser2, instant)
            self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[3], minDistances, mostSevereIsMax = False))

    def categorize(self, velocityAngleTolerance, parallelAngleTolerance, headonCollisionCourseAngleTolerance = None, speedThreshold = 0.):
        '''Computes the interaction category by instant
        all 3 angle arguments in radian
        velocityAngleTolerance: indicates the angle threshold for rear and head on (180-velocityAngleTolerance),
        as well as the maximum collision course angle for head on (if headonCollisionCourseAngleTolerance is None)
        parallelAngleTolerance: indicates the tolerance on the expected 90 deg angle
        between velocity vector (average for parallel) and position vector for a parallel interaction
        speedThreshold defines stationary users: a stationary interaction is between one moving and
        one stationary user, and their distance decreases

        an instant may not be categorized if it matches the side definition (angle)
        but the distance is growing (at least one user is probably past the point of trajectory crossing)'''
        minParallelAngleCosine = np.cos(np.pi/2+parallelAngleTolerance)
        maxParallelAngleCosine = np.cos(np.pi/2-parallelAngleTolerance)
        if headonCollisionCourseAngleTolerance is None:
            headonCollisionCourseAngleTolerance = velocityAngleTolerance
        speedThreshold2 = speedThreshold**2
            
        self.categories = {}
        collisionCourseDotProducts = self.getIndicator(Interaction.indicatorNames[0])
        collisionCourseAngles = self.getIndicator(Interaction.indicatorNames[1])
        distances = self.getIndicator(Interaction.indicatorNames[2])
        velocityAngles = self.getIndicator(Interaction.indicatorNames[4])
        for instant in self.timeInterval:
            stationaryUser1 = self.roadUser1.getVelocityAtInstant(instant).norm2Squared() <= speedThreshold2
            stationaryUser2 = self.roadUser2.getVelocityAtInstant(instant).norm2Squared() <= speedThreshold2
            if stationaryUser1 != stationaryUser2 and collisionCourseDotProducts[instant] > 0: # only one is not moving and is getting closer
                self.categories[instant] = Interaction.categories["stationary"]
                # true stationary would be all the times (parked, difficult without semantic knowledge of the scene
                # alternatively, one could get the previous or next non zero velocity to identify user orientation
            elif velocityAngles.existsAtInstant(instant):
                if velocityAngles[instant] < velocityAngleTolerance: # parallel or rear end
                    midVelocity = self.roadUser1.getVelocityAtInstant(instant) + self.roadUser2.getVelocityAtInstant(instant)
                    deltap = self.roadUser1.getPositionAtInstant(instant)-self.roadUser2.getPositionAtInstant(instant)
                    if minParallelAngleCosine < abs(moving.Point.dot(midVelocity, deltap)/(midVelocity.norm2()*distances[instant])) < maxParallelAngleCosine:
                        self.categories[instant] = Interaction.categories["parallel"]
                    else:
                        self.categories[instant] = Interaction.categories["rearend"]
                elif velocityAngles[instant] > np.pi - velocityAngleTolerance and collisionCourseAngles[instant] < headonCollisionCourseAngleTolerance: # head on
                    self.categories[instant] = Interaction.categories["headon"]
                elif collisionCourseDotProducts[instant] > 0:
                    self.categories[instant] = Interaction.categories["side"]
            # true stationary is when object does not move for the whole period of the interaction, otherwise get last (or next) velocity vector for user orientation
        # leaving is not a good interaction category (issue in Etienne's 2022 paper):
        # means we are past the situation in which users are approaching
        # could try to predict what happened before, but it's not observed
        

    def computeCrossingsCollisions(self, predictionParameters, collisionDistanceThreshold, timeHorizon, computeCZ = False, debug = False, timeInterval = None, speedThreshold = 0.):
        '''Computes all crossing and collision points at each common instant for two road users.

        speedThreshold defines when users are stationary: TTC is not computed when both users are stationary'''
        TTCs = {}
        collisionProbabilities = {}
        if timeInterval is not None:
            commonTimeInterval = timeInterval
        else:
            commonTimeInterval = self.timeInterval
        self.collisionPoints, crossingZones = predictionParameters.computeCrossingsCollisions(self.roadUser1, self.roadUser2, collisionDistanceThreshold, timeHorizon, computeCZ, debug, commonTimeInterval, speedThreshold)
        for i, cps in self.collisionPoints.items():
            TTCs[i] = prediction.SafetyPoint.computeExpectedIndicator(cps)
            collisionProbabilities[i] = sum([p.probability for p in cps])
        if len(TTCs) > 0:
            self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[7], TTCs, mostSevereIsMax=False))
            self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[6], collisionProbabilities))
        else:
            for i in [6,7]:
                self.resetIndicator(Interaction.indicatorNames[i])
        
        # crossing zones and pPET
        if computeCZ:
            self.crossingZones = crossingZones
            pPETs = {}
            for i, cz in self.crossingZones.items():
                pPETs[i] = prediction.SafetyPoint.computeExpectedIndicator(cz)
            self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[9], pPETs, mostSevereIsMax=False))
        # TODO add probability of collision, and probability of successful evasive action

    def computePET(self, collisionDistanceThreshold, computePetWithBoundingPoly):
        'Warning: when computing PET from interactions, there could be PETs between objects that do not coexist and therefore are not considered interactions'
        pet, t1, t2=  moving.MovingObject.computePET(self.roadUser1, self.roadUser2, collisionDistanceThreshold, computePetWithBoundingPoly)
        if pet is not None:
            self.addIndicator(indicators.SeverityIndicator(Interaction.indicatorNames[10], {min(t1, t2): pet, max(t1, t2): pet}, mostSevereIsMax = False))
        else:
            self.resetIndicator(Interaction.indicatorNames[10])

    def setCollision(self, collision):
        '''indicates if it is a collision: argument should be boolean'''
        self.collision = collision

    def isCollision(self):
        if hasattr(self, 'collision'):
            return self.collision
        else:
            return None

    def getCollisionPoints(self):
        return self.collisionPoints

    def getCrossingZones(self):
        return self.crossingZones

def createInteractions(objects, _others = None, maxDurationApart = 0):
    '''Create all interactions of two co-existing road users'''
    if _others is not None:
        others = _others

    interactions = []
    num = 0
    for i in range(len(objects)):
        if _others is None:
            others = objects[:i]
        for j in range(len(others)):
            commonTimeInterval = objects[i].commonTimeInterval(others[j])
            if not commonTimeInterval.empty() or (maxDurationApart > 0 and objects[i].getTimeInterval().distance(objects[j].getTimeInterval()) < maxDurationApart):
                interactions.append(Interaction(num, commonTimeInterval, objects[i].num, others[j].num, objects[i], others[j]))
                num += 1
    return interactions

def findInteraction(interactions, roadUserNum1, roadUserNum2):
    'Returns the right interaction in the set'
    i=0
    while i<len(interactions) and set([roadUserNum1, roadUserNum2]) != interactions[i].getRoadUserNumbers():
        i+=1
    if i<len(interactions):
        return interactions[i]
    else:
        return None

def computeIndicators(interactions, computeMotionPrediction, computePET, predictionParameters, collisionDistanceThreshold, computePetWithBoundingPoly, timeHorizon, computeCZ = False, debug = False, timeInterval = None):
    for inter in interactions:
        print('processing interaction {}'.format(inter.getNum())) # logging.debug('processing interaction {}'.format(inter.getNum()))
        inter.computeIndicators()
        if computeMotionPrediction:
            inter.computeCrossingsCollisions(predictionParameters, collisionDistanceThreshold, timeHorizon, computeCZ, debug, timeInterval)
        if computePET:
            inter.computePET(collisionDistanceThreshold, computePetWithBoundingPoly)
    return interactions
    
def aggregateSafetyPoints(interactions, pointType = 'collision'):
    '''Put all collision points or crossing zones in a list for display'''
    allPoints = []
    if pointType == 'collision':
        for i in interactions:
            for points in i.collisionPoints.values():
                allPoints += points
    elif pointType == 'crossing':
        for i in interactions:
            for points in i.crossingZones.values():
                allPoints += points
    else:
        print('unknown type of point: '+pointType)
    return allPoints

def prototypeCluster(interactions, similarities, indicatorName, minSimilarity, similarityFunc = None, minClusterSize = None, randomInitialization = False):
    return ml.prototypeCluster([inter.getIndicator(indicatorName) for inter in interactions], similarities, minSimilarity, similarityFunc, minClusterSize, randomInitialization)

class Crossing(moving.STObject):
    '''Class for the event of a street crossing

    TODO: detecter passage sur la chaussee
    identifier origines et destination (ou uniquement chaussee dans FOV)
    carac traversee
    detecter proximite veh (retirer si trop similaire simultanement
    carac interaction'''
    
    def __init__(self, roaduserNum = None, num = None, timeInterval = None):
        moving.STObject.__init__(self, num, timeInterval)
        self.roaduserNum = roaduserNum

    

if __name__ == "__main__":
    import doctest
    import unittest
    suite = doctest.DocFileSuite('tests/events.txt')
    #suite = doctest.DocTestSuite()
    unittest.TextTestRunner().run(suite)
    
