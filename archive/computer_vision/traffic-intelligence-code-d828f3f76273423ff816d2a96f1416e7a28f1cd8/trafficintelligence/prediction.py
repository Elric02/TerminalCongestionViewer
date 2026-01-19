#! /usr/bin/env python
'''Library for motion prediction methods'''

import math, random
from copy import copy

import numpy as np

from trafficintelligence import moving
from trafficintelligence.utils import LCSS

class PredictedTrajectory(object):
    '''Class for predicted trajectories with lazy evaluation
    if the predicted position has not been already computed, compute it

    it should also have a probability'''

    def __init__(self):
        self.probability = 0.
        self.predictedPositions = {}
        self.predictedSpeedOrientations = {}
        #self.collisionPoints = {}
        #self.crossingZones = {}

    def predictPosition(self, nTimeSteps):
        if nTimeSteps > 0 and not nTimeSteps in self.predictedPositions:
            self.predictPosition(nTimeSteps-1)
            self.predictedPositions[nTimeSteps], self.predictedSpeedOrientations[nTimeSteps] = moving.predictPosition(self.predictedPositions[nTimeSteps-1], self.predictedSpeedOrientations[nTimeSteps-1], self.getControl(), self.maxSpeed)
        return self.predictedPositions[nTimeSteps]

    def getPredictedTrajectory(self):
        return moving.Trajectory.fromPointList(list(self.predictedPositions.values()))

    def getPredictedSpeeds(self):
        return [so.norm for so in self.predictedSpeedOrientations.values()]

    def hasCollisionWith(self, other, t, collisionDistanceThreshold):
        p1 = self.predictPosition(t)
        p2 = other.predictPosition(t)
        return (p1-p2).norm2() <= collisionDistanceThreshold, p1, p2

    def plot(self, options = '', withOrigin = False, timeStep = 1, **kwargs):
        self.getPredictedTrajectory().plot(options, withOrigin, timeStep, **kwargs)

class PredictedTrajectoryConstant(PredictedTrajectory):
    '''Predicted trajectory at constant speed or acceleration
    TODO generalize by passing a series of velocities/accelerations'''

    def __init__(self, initialPosition, initialVelocity, control = moving.NormAngle(0,0), probability = 1., maxSpeed = None):
        self.control = control
        self.maxSpeed = maxSpeed
        self.probability = probability
        self.predictedPositions = {0: initialPosition}
        self.predictedSpeedOrientations = {0: moving.NormAngle.fromPoint(initialVelocity)}
    
    def getControl(self):
        return self.control

class PredictedTrajectoryConstantVelocity(PredictedTrajectory):
    '''Predicted trajectory at constant speed or acceleration
    TODO generalize by passing a series of velocities/accelerations'''

    def __init__(self, initialPosition, initialVelocity, probability = 1.):
        self.probability = probability
        self.predictedPositions = {0: initialPosition}
        self.initialVelocity = initialVelocity
        #self.predictedSpeedOrientations = {0: moving.NormAngle.fromPoint(initialVelocity)}

    def predictPosition(self, nTimeSteps):
        if nTimeSteps > 0 and not nTimeSteps in self.predictedPositions:
            self.predictPosition(nTimeSteps-1)
            self.predictedPositions[nTimeSteps] = self.predictedPositions[nTimeSteps-1]+self.initialVelocity
        return self.predictedPositions[nTimeSteps]

class PredictedPolyTrajectoryConstantVelocity(PredictedTrajectory):
    '''Predicted trajectory of an object with an outline represented by a polygon
    Simple method that just translates the polygon corners (set of moving.Point)'''
    def __init__(self, polyCorners, initialVelocity, probability = 1.):
        self.probability = probability
        self.predictedPositions = {0: moving.pointsToShapely(polyCorners)}
        self.initialVelocity = initialVelocity
        self.predictedTrajectories = [PredictedTrajectoryConstantVelocity(p, initialVelocity) for p in polyCorners]

    def predictPosition(self, nTimeSteps):
        if nTimeSteps > 0 and not nTimeSteps in self.predictedPositions:
            self.predictedPositions[nTimeSteps] = moving.pointsToShapely([t.predictPosition(nTimeSteps) for t in self.predictedTrajectories])
        return self.predictedPositions[nTimeSteps]

    def hasCollisionWith(self, other, t, collisionDistanceThreshold):
        poly1 = self.predictPosition(t)
        poly2 = other.predictPosition(t)
        return poly1.overlaps(poly2), poly1, poly2

class PredictedTrajectoryPrototype(PredictedTrajectory):
    '''Predicted trajectory that follows a prototype trajectory
    The prototype is in the format of a moving.Trajectory: it could be
    1. an observed trajectory (extracted from video)
    2. a generic polyline (eg the road centerline) that a vehicle is supposed to follow

    Prediction can be done
    1. at constant speed (the instantaneous user speed)
    2. following the trajectory path, at the speed of the user
    (applying a constant ratio equal 
    to the ratio of the user instantaneous speed and the trajectory closest speed)'''

    def __init__(self, initialPosition, initialVelocity, prototype, constantSpeed = False, nFramesIgnore = 3, probability = 1.):
        ''' prototype is a MovingObject

        Prediction at constant speed will not work for unrealistic trajectories 
        that do not follow a slowly changing velocity (eg moving object trajectories, 
        but is good for realistic motion (eg features)'''
        self.valid = True
        self.prototype = prototype
        self.constantSpeed = constantSpeed
        self.nFramesIgnore = nFramesIgnore
        self.probability = probability
        self.predictedPositions = {0: initialPosition}
        self.closestPointIdx = prototype.getPositions().getClosestPoint(initialPosition)
        self.deltaPosition = initialPosition-prototype.getPositionAt(self.closestPointIdx) #should be computed in relative coordinates to position
        self.theta = prototype.getVelocityAt(self.closestPointIdx).angle()
        self.initialSpeed = initialVelocity.norm2()
        if not constantSpeed:
            while prototype.getVelocityAt(self.closestPointIdx).norm2() == 0. and self.closestPointIdx < prototype.length():
                self.closestPointIdx += 1
            if self.closestPointIdx < prototype.length():
                self.ratio = self.initialSpeed/prototype.getVelocityAt(self.closestPointIdx).norm2()
            else:
                self.valid = False
    
    def predictPosition(self, nTimeSteps):
        if nTimeSteps > 0 and not nTimeSteps in self.predictedPositions:
            deltaPosition = copy(self.deltaPosition)
            if self.constantSpeed:
                traj = self.prototype.getPositions()
                trajLength = traj.length()
                traveledDistance = nTimeSteps*self.initialSpeed + traj.getCumulativeDistance(self.closestPointIdx)
                i = self.closestPointIdx
                while i < trajLength and traj.getCumulativeDistance(i) < traveledDistance:
                    i += 1
                if i == trajLength:
                    v = self.prototype.getVelocityAt(-1-self.nFramesIgnore)
                    self.predictedPositions[nTimeSteps] = deltaPosition.rotate(v.angle()-self.theta)+traj[i-1]+v*((traveledDistance-traj.getCumulativeDistance(i-1))/v.norm2())
                else:
                    v = self.prototype.getVelocityAt(min(i-1, int(self.prototype.length())-1-self.nFramesIgnore))
                    self.predictedPositions[nTimeSteps] = deltaPosition.rotate(v.angle()-self.theta)+traj[i-1]+(traj[i]-traj[i-1])*((traveledDistance-traj.getCumulativeDistance(i-1))/traj.getDistance(i-1))
            else:
                traj = self.prototype.getPositions()
                trajLength = traj.length()
                nSteps = self.ratio*nTimeSteps+self.closestPointIdx
                i = int(np.floor(nSteps))
                if nSteps < trajLength-1:
                    v = self.prototype.getVelocityAt(min(i, int(self.prototype.length())-1-self.nFramesIgnore))
                    self.predictedPositions[nTimeSteps] = deltaPosition.rotate(v.angle()-self.theta)+traj[i]+(traj[i+1]-traj[i])*(nSteps-i)
                else:
                    v = self.prototype.getVelocityAt(-1-self.nFramesIgnore)
                    self.predictedPositions[nTimeSteps] = deltaPosition.rotate(v.angle()-self.theta)+traj[-1]+v*(nSteps-trajLength+1)
        return self.predictedPositions[nTimeSteps]

class PredictedTrajectoryRandomControl(PredictedTrajectory):
    '''Random vehicle control: suitable for normal adaptation'''
    def __init__(self, initialPosition, initialVelocity, accelerationDistribution, steeringDistribution, probability = 1., maxSpeed = None):
        '''Constructor
        accelerationDistribution and steeringDistribution are distributions 
        that return random numbers drawn from them'''
        self.accelerationDistribution = accelerationDistribution
        self.steeringDistribution = steeringDistribution
        self.maxSpeed = maxSpeed
        self.probability = probability
        self.predictedPositions = {0: initialPosition}
        self.predictedSpeedOrientations = {0: moving.NormAngle.fromPoint(initialVelocity)}

    def getControl(self):
        return moving.NormAngle(self.accelerationDistribution(),self.steeringDistribution())

class SafetyPoint(moving.Point):
    '''Can represent a collision point or crossing zone 
    with respective safety indicator, TTC or pPET'''
    def __init__(self, p, probability = 1., indicator = -1):
        self.x = p.x
        self.y = p.y
        self.probability = probability
        self.indicator = indicator

    def __str__(self):
        return '{0} {1} {2} {3}'.format(self.x, self.y, self.probability, self.indicator)

    @staticmethod
    def save(out, points, predictionInstant, objNum1, objNum2):
        for p in points:
            out.write('{0} {1} {2} {3}\n'.format(objNum1, objNum2, predictionInstant, p))

    @staticmethod
    def computeExpectedIndicator(points):
        return np.sum([p.indicator*p.probability for p in points])/sum([p.probability for p in points])

def computeCollisionTime(predictedTrajectory1, predictedTrajectory2, collisionDistanceThreshold, timeHorizon):
    '''Computes the first instant 
    at which two predicted trajectories are colliding (depending on the trajectory format)
    Computes all the times including timeHorizon
    
    User has to check the first variable collision to know about a collision'''
    t = 1
    collision, p1, p2 = predictedTrajectory1.hasCollisionWith(predictedTrajectory2, t, collisionDistanceThreshold)
    while t < timeHorizon and not collision:
        t += 1
        collision, p1, p2 = predictedTrajectory1.hasCollisionWith(predictedTrajectory2, t, collisionDistanceThreshold)
    return collision, t, p1, p2

def savePredictedTrajectoriesFigure(currentInstant, obj1, obj2, predictedTrajectories1, predictedTrajectories2, timeHorizon, printFigure = True):
    from matplotlib.pyplot import figure, axis, title, clf, savefig
    if printFigure:
        clf()
    else:
        figure()
    for et in predictedTrajectories1:
        for t in range(int(np.round(timeHorizon))):
            et.predictPosition(t)
            et.plot('rx')
    for et in predictedTrajectories2:
        for t in range(int(np.round(timeHorizon))):
            et.predictPosition(t)
            et.plot('bx')
    obj1.plot('r', withOrigin = True)
    obj2.plot('b', withOrigin = True)
    title('instant {0}'.format(currentInstant))
    axis('equal')
    if printFigure:
        savefig('predicted-trajectories-t-{0}.png'.format(currentInstant))

def calculateProbability(nMatching,similarity,objects):
    sumFrequencies=sum([nMatching[p] for p in similarity])
    prototypeProbability={}
    for i in similarity:
        prototypeProbability[i]= similarity[i] * float(nMatching[i])/sumFrequencies
    sumProbabilities= sum([prototypeProbability[p] for p in prototypeProbability])
    probabilities={}
    for i in prototypeProbability:
        probabilities[objects[i]]= float(prototypeProbability[i])/sumProbabilities
    return probabilities

def findPrototypes(prototypes,nMatching,objects,route,partialObjPositions,noiseEntryNums,noiseExitNums,minSimilarity=0.1,mostMatched=None,spatialThreshold=1.0, delta=180):
    ''' behaviour prediction first step'''
    if route[0] not in noiseEntryNums: 
        prototypesRoutes= [ x for x in sorted(prototypes.keys()) if route[0]==x[0]]
    elif route[1] not in noiseExitNums:
        prototypesRoutes=[ x for x in sorted(prototypes.keys()) if route[1]==x[1]]
    else:
        prototypesRoutes=[x for x in sorted(prototypes.keys())]
    lcss = LCSS(similarityFunc=lambda x,y: (distanceForLCSS(x,y) <= spatialThreshold),delta=delta)
    similarity={}
    for y in prototypesRoutes: 
        if y in prototypes:
            prototypesIDs=prototypes[y]            
            for x in prototypesIDs:
                s=lcss.computeNormalized(partialObjPositions, objects[x].positions)
                if s >= minSimilarity:
                    similarity[x]=s
    
    if mostMatched==None:
        probabilities= calculateProbability(nMatching,similarity,objects)        
        return probabilities
    else:
        mostMatchedValues=sorted(similarity.values(),reverse=True)[:mostMatched]
        keys=[k for k in similarity if similarity[k] in mostMatchedValues]
        newSimilarity={}
        for i in keys:
            newSimilarity[i]=similarity[i]
        probabilities= calculateProbability(nMatching,newSimilarity,objects)        
        return probabilities        
        
def findPrototypesSpeed(prototypes,secondStepPrototypes,nMatching,objects,route,partialObjPositions,noiseEntryNums,noiseExitNums,minSimilarity=0.1,mostMatched=None,useDestination=True,spatialThreshold=1.0, delta=180):
    if useDestination:
        prototypesRoutes=[route]
    else:
        if route[0] not in noiseEntryNums: 
            prototypesRoutes= [ x for x in sorted(prototypes.keys()) if route[0]==x[0]]
        elif route[1] not in noiseExitNums:
            prototypesRoutes=[ x for x in sorted(prototypes.keys()) if route[1]==x[1]]
        else:
            prototypesRoutes=[x for x in sorted(prototypes.keys())]
    lcss = LCSS(similarityFunc=lambda x,y: (distanceForLCSS(x,y) <= spatialThreshold),delta=delta)
    similarity={}
    for y in prototypesRoutes: 
        if y in prototypes:
            prototypesIDs=prototypes[y]    
            for x in prototypesIDs:
                s=lcss.computeNormalized(partialObjPositions, objects[x].positions)
                if s >= minSimilarity:
                    similarity[x]=s
    
    newSimilarity={}
    for i in similarity:
        if i in secondStepPrototypes:
            for j in secondStepPrototypes[i]:
                newSimilarity[j]=similarity[i]
    probabilities= calculateProbability(nMatching,newSimilarity,objects)        
    return probabilities
    
def getPrototypeTrajectory(obj,route,currentInstant,prototypes,secondStepPrototypes,nMatching,objects,noiseEntryNums,noiseExitNums,minSimilarity=0.1,mostMatched=None,useDestination=True,useSpeedPrototype=True):
    partialInterval=moving.Interval(obj.getFirstInstant(),currentInstant)
    partialObjPositions= obj.getObjectInTimeInterval(partialInterval).positions    
    if useSpeedPrototype:
        prototypeTrajectories=findPrototypesSpeed(prototypes,secondStepPrototypes,nMatching,objects,route,partialObjPositions,noiseEntryNums,noiseExitNums,minSimilarity,mostMatched,useDestination)
    else:
        prototypeTrajectories=findPrototypes(prototypes,nMatching,objects,route,partialObjPositions,noiseEntryNums,noiseExitNums,minSimilarity,mostMatched)
    return prototypeTrajectories


class PredictionParameters(object):
    def __init__(self, name, maxSpeed = None, useCurvilinear = False):
        self.name = name
        self.maxSpeed = maxSpeed
        self.useCurvilinear = useCurvilinear

    def __str__(self):
        return '{0} {1}'.format(self.name, self.maxSpeed)

    def generatePredictedTrajectories(self, obj, instant):
        return None

    def computeCollisionPoint(self, p1, p2, probability, t):
        return SafetyPoint((p1+p2)*0.5, probability, t)

    def computeCrossingsCollisionsAtInstant(self, currentInstant, obj1, obj2, collisionDistanceThreshold, timeHorizon, computeCZ = False, debug = False):
        '''returns the lists of collision points and crossing zones'''
        predictedTrajectories1 = self.generatePredictedTrajectories(obj1, currentInstant)
        predictedTrajectories2 = self.generatePredictedTrajectories(obj2, currentInstant)

        collisionPoints = []
        if computeCZ:
            crossingZones = []
        else:
            crossingZones = None
        for et1 in predictedTrajectories1:
            for et2 in predictedTrajectories2:
                collision, t, p1, p2 = computeCollisionTime(et1, et2, collisionDistanceThreshold, timeHorizon)
                if collision:
                    collisionPoints.append(self.computeCollisionPoint(p1, p2, et1.probability*et2.probability, t))
                elif computeCZ: # check if there is a crossing zone
                    # TODO same computation as PET with metric + concatenate past trajectory with future trajectory
                    cz = None
                    t1 = 0
                    while not cz and t1 < timeHorizon: # t1 <= timeHorizon-1
                        t2 = 0
                        while not cz and t2 < timeHorizon:
                            cz = moving.segmentIntersection(et1.predictPosition(t1), et1.predictPosition(t1+1), et2.predictPosition(t2), et2.predictPosition(t2+1))
                            if cz is not None:
                                deltaV= (et1.predictPosition(t1)- et1.predictPosition(t1+1) - et2.predictPosition(t2)+ et2.predictPosition(t2+1)).norm2()
                                crossingZones.append(SafetyPoint(cz, et1.probability*et2.probability, abs(t1-t2)-(float(collisionDistanceThreshold)/deltaV)))
                            t2 += 1
                        t1 += 1                        

        if debug:
            savePredictedTrajectoriesFigure(currentInstant, obj1, obj2, predictedTrajectories1, predictedTrajectories2, timeHorizon)

        return collisionPoints, crossingZones

    def computeCrossingsCollisions(self, obj1, obj2, collisionDistanceThreshold, timeHorizon, computeCZ = False, debug = False, timeInterval = None, speedThreshold = 0.):
        '''Computes all crossing and collision points at each common instant for two road users. 
        No movement prediction below a certain speedThreshold for both objects'''
        collisionPoints = {}
        if computeCZ:
            crossingZones = {}
        else:
            crossingZones = None
        if timeInterval is not None:
            commonTimeInterval = timeInterval
        else:
            commonTimeInterval = obj1.commonTimeInterval(obj2)
        speedThreshold2 = max(0,speedThreshold)**2
        for t in list(commonTimeInterval)[:-1]: # do not look at the 1 last position/velocities, often with errors
            if obj1.getVelocityAtInstant(t).norm2Squared() > speedThreshold2 and obj2.getVelocityAtInstant(t).norm2Squared() > speedThreshold2:
                cp, cz = self.computeCrossingsCollisionsAtInstant(t, obj1, obj2, collisionDistanceThreshold, timeHorizon, computeCZ, debug)
                if len(cp) != 0:
                    collisionPoints[t] = cp
                if computeCZ and len(cz) != 0:
                    crossingZones[t] = cz
        return collisionPoints, crossingZones

    def computeCollisionProbability(self, obj1, obj2, collisionDistanceThreshold, timeHorizon, debug = False, timeInterval = None):
        '''Computes only collision probabilities
        Returns for each instant the collision probability and number of samples drawn'''
        collisionProbabilities = {}
        if timeInterval is not None:
            commonTimeInterval = timeInterval
        else:
            commonTimeInterval = obj1.commonTimeInterval(obj2)
        for i in list(commonTimeInterval)[:-1]:
            nCollisions = 0
            predictedTrajectories1 = self.generatePredictedTrajectories(obj1, i)
            predictedTrajectories2 = self.generatePredictedTrajectories(obj2, i)
            for et1 in predictedTrajectories1:
                for et2 in predictedTrajectories2:
                    collision, t, p1, p2 = computeCollisionTime(et1, et2, collisionDistanceThreshold, timeHorizon)
                    if collision:
                        nCollisions += 1
            # take into account probabilities ??
            nSamples = float(len(predictedTrajectories1)*len(predictedTrajectories2))
            collisionProbabilities[i] = [nSamples, float(nCollisions)/nSamples]

            if debug:
                savePredictedTrajectoriesFigure(i, obj1, obj2, predictedTrajectories1, predictedTrajectories2, timeHorizon)

        return collisionProbabilities

class ConstantPredictionParameters(PredictionParameters):
    def __init__(self):
        PredictionParameters.__init__(self, 'constant velocity')

    def generatePredictedTrajectories(self, obj, instant):
        return [PredictedTrajectoryConstantVelocity(obj.getPositionAtInstant(instant), obj.getVelocityAtInstant(instant))]

class ConstantPolyPredictionParameters(PredictionParameters):
    def __init__(self):
        PredictionParameters.__init__(self, 'constant velocity for polygon representation')

    def generatePredictedTrajectories(self, obj, instant):
        return [PredictedPolyTrajectoryConstantVelocity(obj.getBoundingPolygon(instant), obj.getVelocityAtInstant(instant))]

    def computeCollisionPoint(self, p1, p2, probability, t):
        polyRepr1 = p1.representative_point()
        polyRepr2 = p2.representative_point()
        p = moving.Point(polyRepr1.x+polyRepr2.x, polyRepr1.y+polyRepr2.y)
        return SafetyPoint(p, probability, t)
    
class NormalAdaptationPredictionParameters(PredictionParameters):
    def __init__(self, maxSpeed, nPredictedTrajectories, accelerationDistribution, steeringDistribution, useFeatures = False):
        '''An example of acceleration and steering distributions is
        lambda: random.triangular(-self.maxAcceleration, self.maxAcceleration, 0.)
        '''
        if useFeatures:
            name = 'point set normal adaptation'
        else:
            name = 'normal adaptation'
        PredictionParameters.__init__(self, name, maxSpeed)
        self.nPredictedTrajectories = nPredictedTrajectories
        self.useFeatures = useFeatures
        self.accelerationDistribution = accelerationDistribution
        self.steeringDistribution = steeringDistribution
        
    def __str__(self):
        return PredictionParameters.__str__(self)+' {0} {1} {2}'.format(self.nPredictedTrajectories, 
                                                                        self.maxAcceleration, 
                                                                        self.maxSteering)

    def generatePredictedTrajectories(self, obj, instant):
        predictedTrajectories = []
        if self.useFeatures and obj.hasFeatures():
            features = [f for f in obj.getFeatures() if f.existsAtInstant(instant)]
            positions = [f.getPositionAtInstant(instant) for f in features]
            velocities = [f.getVelocityAtInstant(instant) for f in features]
        else:
            positions = [obj.getPositionAtInstant(instant)]
            velocities = [obj.getVelocityAtInstant(instant)]
        probability = 1./float(len(positions)*self.nPredictedTrajectories)
        for i in range(self.nPredictedTrajectories):
            for initialPosition,initialVelocity in zip(positions, velocities):
                predictedTrajectories.append(PredictedTrajectoryRandomControl(initialPosition, 
                                                                              initialVelocity, 
                                                                              self.accelerationDistribution, 
                                                                              self.steeringDistribution, 
                                                                              probability, 
                                                                              maxSpeed = self.maxSpeed))
        return predictedTrajectories

class PointSetPredictionParameters(PredictionParameters):
    def __init__(self, maxSpeed):
        PredictionParameters.__init__(self, 'point set', maxSpeed)
    
    def generatePredictedTrajectories(self, obj, instant):
        predictedTrajectories = []
        if obj.hasFeatures():
            features = [f for f in obj.getFeatures() if f.existsAtInstant(instant)]
            positions = [f.getPositionAtInstant(instant) for f in features]
            velocities = [f.getVelocityAtInstant(instant) for f in features]
            probability = 1./float(len(positions))
            for initialPosition,initialVelocity in zip(positions, velocities):
                predictedTrajectories.append(PredictedTrajectoryConstantVelocity(initialPosition, initialVelocity, probability = probability))
            return predictedTrajectories
        else:
            print('Object {} has no features'.format(obj.getNum()))
            return None
        
class EvasiveActionPredictionParameters(PredictionParameters):
    def __init__(self, maxSpeed, nPredictedTrajectories, accelerationDistribution, steeringDistribution, useFeatures = False):
        '''Suggested acceleration distribution may not be symmetric, eg
        lambda: random.triangular(self.minAcceleration, self.maxAcceleration, 0.)'''

        if useFeatures:
            name = 'point set evasive action'
        else:
            name = 'evasive action'
        PredictionParameters.__init__(self, name, maxSpeed)
        self.nPredictedTrajectories = nPredictedTrajectories
        self.useFeatures = useFeatures
        self.accelerationDistribution = accelerationDistribution
        self.steeringDistribution = steeringDistribution

    def __str__(self):
        return PredictionParameters.__str__(self)+' {0} {1} {2} {3}'.format(self.nPredictedTrajectories, self.minAcceleration, self.maxAcceleration, self.maxSteering)

    def generatePredictedTrajectories(self, obj, instant):
        predictedTrajectories = []
        if self.useFeatures and obj.hasFeatures():
            features = [f for f in obj.getFeatures() if f.existsAtInstant(instant)]
            positions = [f.getPositionAtInstant(instant) for f in features]
            velocities = [f.getVelocityAtInstant(instant) for f in features]
        else:
            positions = [obj.getPositionAtInstant(instant)]
            velocities = [obj.getVelocityAtInstant(instant)]
        probability = 1./float(self.nPredictedTrajectories)
        for i in range(self.nPredictedTrajectories):
            for initialPosition,initialVelocity in zip(positions, velocities):
                predictedTrajectories.append(PredictedTrajectoryConstant(initialPosition, 
                                                                         initialVelocity, 
                                                                         moving.NormAngle(self.accelerationDistribution(), 
                                                                                          self.steeringDistribution()), 
                                                                         probability, 
                                                                         self.maxSpeed))
        return predictedTrajectories

class CVDirectPredictionParameters(PredictionParameters):
    '''Prediction parameters of prediction at constant velocity
    using direct computation of the intersecting point
    Warning: the computed time to collision may be higher than timeHorizon (not used)'''
    
    def __init__(self):
        PredictionParameters.__init__(self, 'constant velocity (direct computation)', None)

    def computeCrossingsCollisionsAtInstant(self, currentInstant, obj1, obj2, collisionDistanceThreshold, timeHorizon, computeCZ = False, debug = False, *kwargs):
        collisionPoints = []
        if computeCZ:
            crossingZones = []
        else:
            crossingZones = None

        p1 = obj1.getPositionAtInstant(currentInstant)
        p2 = obj2.getPositionAtInstant(currentInstant)
        if (p1-p2).norm2() <= collisionDistanceThreshold:
            collisionPoints = [SafetyPoint((p1+p2)*0.5, 1., 0.)]
        else:
            v1 = obj1.getVelocityAtInstant(currentInstant)
            v2 = obj2.getVelocityAtInstant(currentInstant)
            intersection = moving.intersection(p1, p1+v1, p2, p2+v2)

            if intersection is not None:
                dp1 = intersection-p1
                dp2 = intersection-p2
                dot1 = moving.Point.dot(dp1, v1)
                dot2 = moving.Point.dot(dp2, v2)
                if (computeCZ and (dot1 > 0 or dot2 > 0)) or (dot1 > 0 and dot2 > 0): # if the road users are moving towards the intersection or if computing pPET
                    dist1 = dp1.norm2()
                    dist2 = dp2.norm2()
                    s1 = math.copysign(v1.norm2(), dot1)
                    s2 = math.copysign(v2.norm2(), dot2)
                    halfCollisionDistanceThreshold = collisionDistanceThreshold/2.
                    timeInterval1 = moving.TimeInterval(max(0,dist1-halfCollisionDistanceThreshold)/s1, (dist1+halfCollisionDistanceThreshold)/s1)
                    timeInterval2 = moving.TimeInterval(max(0,dist2-halfCollisionDistanceThreshold)/s2, (dist2+halfCollisionDistanceThreshold)/s2)
                    collisionTimeInterval = moving.TimeInterval.intersection(timeInterval1, timeInterval2)
                    
                    if collisionTimeInterval.empty():
                        if computeCZ:
                            crossingZones.append(SafetyPoint(intersection, 1., timeInterval1.distance(timeInterval2)))
                    else:
                        collisionPoints.append(SafetyPoint(intersection, 1., collisionTimeInterval.center()))
    
        if debug and intersection is not None:
            from matplotlib.pyplot import plot, figure, axis, title
            figure()
            plot([p1.x, intersection.x], [p1.y, intersection.y], 'r')
            plot([p2.x, intersection.x], [p2.y, intersection.y], 'b')
            intersection.plot()            
            obj1.plot('r')
            obj2.plot('b')
            title('instant {0}'.format(currentInstant))
            axis('equal')

        return collisionPoints, crossingZones

class CVExactPredictionParameters(PredictionParameters):
    '''Prediction parameters of prediction at constant velocity
    using direct computation of the intersecting point (solving the equation)
    Warning: the computed time to collision may be higher than timeHorizon (not used)'''
    
    def __init__(self, useCurvilinear = False):
        PredictionParameters.__init__(self, 'constant velocity (direct exact computation)', None, useCurvilinear)

    def computeCrossingsCollisionsAtInstant(self, currentInstant, obj1, obj2, collisionDistanceThreshold, timeHorizon, computeCZ = False, debug = False, *kwargs):
        'TODO compute pPET'
        collisionPoints = []
        if computeCZ:
            crossingZones = []
        else:
            crossingZones = None

        if self.useCurvilinear:
            pass # Lionel
        else:
            p1 = obj1.getPositionAtInstant(currentInstant)
            p2 = obj2.getPositionAtInstant(currentInstant)
            v1 = obj1.getVelocityAtInstant(currentInstant)
            v2 = obj2.getVelocityAtInstant(currentInstant)
            #intersection = moving.intersection(p1, p1+v1, p2, p2+v2)

            if not moving.Point.parallel(v1, v2):
                ttc = moving.Point.timeToCollision(p1, p2, v1, v2, collisionDistanceThreshold)
                if ttc is not None:
                    collisionPoints.append(SafetyPoint((p1+(v1*ttc)+p2+(v2*ttc))*0.5, 1., ttc))
                else:
                    pass # compute pPET
        return collisionPoints, crossingZones

class CVExactPolyPredictionParameters(PredictionParameters):
    '''Prediction parameters of prediction at constant velocity
    for objects represented by boxes (bird eye view boxes)
    Warning: the computed time to collision may be higher than timeHorizon (not used)'''
    
    def __init__(self):
        PredictionParameters.__init__(self, 'constant velocity for polygon representation (direct exact)', None)

    def computeCrossingsCollisionsAtInstant(self, currentInstant, obj1, obj2, collisionDistanceThreshold, timeHorizon, computeCZ = False, debug = False, *kwargs):
        'TODO compute pPET'
        collisionPoints = []
        if computeCZ:
            crossingZones = []
        else:
            crossingZones = None

        if self.useCurvilinear:
            pass # Lionel
        else:
            poly1 = obj1.getBoundingPolygon(currentInstant)
            poly2 = obj2.getBoundingPolygon(currentInstant)
            v1 = obj1.getVelocityAtInstant(currentInstant)
            v2 = obj2.getVelocityAtInstant(currentInstant)

            if not moving.Point.parallel(v1, v2):
                ttc = moving.Point.timeToCollisionPoly(poly1, v1, poly2, v2)
                if ttc is not None:
                    collisionPoints.append(SafetyPoint((obj1.getPositionAtInstant(currentInstant)+(v1*ttc)+obj2.getPositionAtInstant(currentInstant)+(v2*ttc))*0.5, 1., ttc))
                else:
                    pass # compute pPET
        return collisionPoints, crossingZones


class PrototypePredictionParameters(PredictionParameters):
    def __init__(self, prototypes, nPredictedTrajectories, pointSimilarityDistance, minSimilarity, lcssMetric = 'cityblock', minFeatureTime = 10, constantSpeed = False, useFeatures = True):
        PredictionParameters.__init__(self, 'prototypes', None)
        self.prototypes = prototypes
        self.nPredictedTrajectories = nPredictedTrajectories
        self.lcss = LCSS(metric = lcssMetric, epsilon = pointSimilarityDistance)
        self.minSimilarity = minSimilarity
        self.minFeatureTime = minFeatureTime
        self.constantSpeed = constantSpeed
        self.useFeatures = useFeatures

    def getLcss(self):
        return self.lcss
        
    def addPredictedTrajectories(self, predictedTrajectories, obj, instant):
        obj.computeTrajectorySimilarities(self.prototypes, self.lcss)
        for proto, similarities in zip(self.prototypes, obj.prototypeSimilarities):
            if similarities[instant-obj.getFirstInstant()] >= self.minSimilarity:
                initialPosition = obj.getPositionAtInstant(instant)
                initialVelocity = obj.getVelocityAtInstant(instant)
                predictedTrajectory = PredictedTrajectoryPrototype(initialPosition, initialVelocity, proto.getMovingObject(), constantSpeed = self.constantSpeed, probability = proto.getNMatchings())
                if predictedTrajectory.valid:
                    predictedTrajectories.append(predictedTrajectory)
        
    def generatePredictedTrajectories(self, obj, instant):
        predictedTrajectories = []
        if instant-obj.getFirstInstant()+1 >= self.minFeatureTime:
            if self.useFeatures and obj.hasFeatures():
                if not hasattr(obj, 'currentPredictionFeatures'):
                    obj.currentPredictionFeatures = []
                else:
                    obj.currentPredictionFeatures[:] = [f for f in obj.currentPredictionFeatures if f.existsAtInstant(instant)]
                firstInstants = [(f,f.getFirstInstant()) for f in obj.getFeatures() if f.existsAtInstant(instant) and f not in obj.currentPredictionFeatures]
                firstInstants.sort(key = lambda t: t[1])
                for f,t1 in firstInstants[:min(self.nPredictedTrajectories, len(firstInstants), self.nPredictedTrajectories-len(obj.currentPredictionFeatures))]:
                    obj.currentPredictionFeatures.append(f)
                for f in obj.currentPredictionFeatures:
                    self.addPredictedTrajectories(predictedTrajectories, f, instant)
            else:
                self.addPredictedTrajectories(predictedTrajectories, obj, instant)
        return predictedTrajectories

if __name__ == "__main__":
    import doctest
    import unittest
    suite = doctest.DocFileSuite('tests/prediction.txt')
    #suite = doctest.DocTestSuite()
    unittest.TextTestRunner().run(suite)
    #doctest.testmod()
    #doctest.testfile("example.txt")

