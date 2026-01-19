#! /usr/bin/env python
''' Traffic Engineering Tools and Examples'''

from math import ceil
from numpy import e, log, arange
from numpy.random import triangular
from scipy import stats

from matplotlib.pyplot import figure, plot, xlabel, ylabel, xlim, ylim

from trafficintelligence import prediction

#########################
# Simulation
#########################

def generateTimeHeadways(meanTimeHeadway, simulationTime):
    '''Generates the time headways between arrivals 
    given the meanTimeHeadway and the negative exponential distribution
    over a time interval of length simulationTime (assumed to be in same time unit as headway'''
    from random import expovariate
    headways = []
    totalTime = 0
    flow = 1/meanTimeHeadway
    while totalTime < simulationTime:
        h = expovariate(flow)
        headways.append(h)
        totalTime += h
    return headways

class RoadUser(object):
    '''Simple example of inheritance to plot different road users (no history)'''
    def __init__(self, position, velocity):
        'Both fields are 2D numpy arrays'
        self.position = position.astype(float)        
        self.velocity = velocity.astype(float)

    def move(self, deltaT):
        self.position += deltaT*self.velocity

    def draw(self, init = False):
        if init:
            self.plotLine = plot(self.position[0], self.position[1], self.getDescriptor())[0]
        else:
            self.plotLine.set_data(self.position[0], self.position[1])

class PassengerVehicle(RoadUser):
    def getDescriptor(self):
        return 'dr'

class Pedestrian(RoadUser):
    def getDescriptor(self):
        return 'xb'

class Cyclist(RoadUser):
    def getDescriptor(self):
        return 'og'

class SLUser(object):
    '''Class for single lane road users

    Warning: does not work with decimal time, time must be integer based'''
    def __init__(self, t0, x0, v0, sigma):
        'sigma is the width of the triangular distribution around 1 for speed noise in ]0,0.1]'
        self.t = [t0]
        self.x = [x0]
        self.v = [v0]
        self.sigma = max(0.001,min(0.1,sigma))
        self.left = 1.-self.sigma
        self.right = 1.+self.sigma

    def getT(self):
        return self.t

    def existsAt(self, t):
        return t in self.t

    def getX(self):
        return self.x

    def getXAt(self, t):
        if self.existsAt(t):
            i = self.t.index(t)
            return self.x[i]
        else:
            return None
    
    def getV(self):
        return self.v
    
    def getVAt(self, t):
        if self.existsAt(t):
            i = self.t.index(t)
            return self.v[i]
        else:
            return None
    
    def update(self, a, deltat):
        self.t.append(self.t[-1]+1)
        self.v.append(max(0,self.v[-1]+a*deltat*triangular(self.left,1., self.right)))
        self.x.append(self.x[-1]+self.v[-1]*deltat)

    def plotX(self, deltat):
        plot([t*deltat for t in self.t], self.x)

    def plotV(self, deltat):
        plot([t*deltat for t in self.t], self.v)

#########################
# queueing models
#########################

class CapacityReduction(object):
    def __init__(self, beta, reductionDuration, demandCapacityRatio = None, demand = None, capacity = None):
        '''reduction duration should be positive
        demandCapacityRatio is demand/capacity (q/s)'''
        if demandCapacityRatio is None and demand is None and capacity is None:
            print('Missing too much information (demand, capacity and ratio)')
            import sys
            sys.exit()
        if 0 <= beta < 1:
            self.beta = beta
            self.reductionDuration = reductionDuration

            if demandCapacityRatio is not None:
                self.demandCapacityRatio = demandCapacityRatio
            if demand is not None:
                self.demand = demand
            if capacity is not None:
                self.capacity = capacity
            if capacity is not None and demand is not None:
                self.demandCapacityRatio = float(self.demand)/self.capacity
                if demand <= beta*capacity:
                    print('There is no queueing as the demand {} is inferior to the reduced capacity {}'.format(demand, beta*capacity))
        else:
            print('reduction coefficient (beta={}) is not in [0, 1['.format(beta))

    def queueingDuration(self):
        return self.reductionDuration*(1-self.beta)/(1-self.demandCapacityRatio)

    def nArrived(self, t):
        '''since the beginning of the capacity reduction'''
        if self.demand is None or t<0:
            print('Missing demand field')
            return None
        return self.demand*t

    def nServed(self, t):
        '''since the beginning of the capacity reduction'''
        if self.capacity is None or t<0:
            print('Missing capacity field')
            return None
        if 0<=t<=self.reductionDuration:
            return self.beta*self.capacity*t
        elif self.reductionDuration < t:
            qDuration = self.queueingDuration()
            if t <= qDuration:
                return self.beta*self.capacity*self.reductionDuration+self.capacity*(t-self.reductionDuration)
            else:
                return self.beta*self.capacity*self.reductionDuration+self.capacity*(qDuration-self.reductionDuration)+self.demand*(t-qDuration)

    def nQueued(self, t):
        return self.nArrived(t)-self.nServed(t)

    def maxNQueued(self):
        return self.nQueued(self.reductionDuration)

    def totalDelay(self):
        if self.capacity is None:
            print('Missing capacity field')
            return None
        return self.capacity*self.reductionDuration**2*(1-self.beta)*(self.demandCapacityRatio-self.beta)/(2*(1-self.demandCapacityRatio))
    
    def averageDelay(self):
        return self.reductionDuration*(self.demandCapacityRatio-self.beta)/(2*self.demandCapacityRatio)

    def averageNQueued(self):
        return self.totalDelay()/self.queueingDuration()


#########################
# fundamental diagrams
#########################

class FundamentalDiagram(object):
    ''' '''
    def __init__(self, name):
        self.name = name
        self.kj = None
        self.kc = None
        self.vf = None
        self.qmax = None

    def getJamDensity(self):
        return self.kj
    
    def getCriticalDensity(self): 
        return self.kc

    def getCapacity(self):
        return self.qmax

    def getFreeFlowSpeed(self):
        return self.vf

    def q(self, k):
        return k*self.v(k)

    @staticmethod
    def meanHeadway(k):
        return 1/k
    
    @staticmethod
    def meanSpacing(q):
        return 1/q

    def plotVK(self, language='fr', units={}):
        densities = [k for k in arange(1, self.kj+1)]
        figure()
        plot(densities, [self.v(k) for k in densities])
        xlim(xmin=0)
        ylim(ymin=0)
        xlabel('Densite (veh/km)') # todo other languages and adapt to units
        ylabel('Vitesse (km/h)')

    def plotQK(self, language='fr', units={}):
        densities = [k for k in arange(1, self.kj+1)]
        figure()
        plot(densities, [self.q(k) for k in densities])
        xlim(xmin=0)
        ylim(ymin=0)
        xlabel('Densite (veh/km)') # todo other languages and adapt to units
        ylabel('Debit (km/h)')

class GreenshieldsFD(FundamentalDiagram):
    '''Speed is a linear function of density'''
    def __init__(self, vf, kj):
        FundamentalDiagram.__init__(self,'Greenshields')
        self.vf=vf
        self.kj=kj
        self.kc=kj/2
        self.qmax=vf*kj/4
    
    def v(self,k):
        from numpy import log
        return self.vf*(1-k/self.kj)

        
class GreenbergFD(FundamentalDiagram):
    '''Speed is the logarithm of density'''
    def __init__(self, vc, kj):
        FundamentalDiagram.__init__(self,'Greenberg')
        self.vc=vc
        self.kj=kj
        self.qmax = self.kc*self.vc
        self.kc = self.kj/e
    
    def v(self,k):
        return self.vc*log(self.kj/k)

class TriangularFD(FundamentalDiagram):
    def __init__(self, vf = None, kc = None, kj = None, qmax = None, w = None):
        FundamentalDiagram.__init__(self,'Triangular')
        if vf is not None and kj is not None:
            self.vf=vf
            self.kj = kj
            if qmax is not None:
                self.qmax = qmax
                self.kc = qmax/vf
                self.w = qmax/(self.kc-kj)
            elif kc is not None:
                self.kc = kc
                self.qmax = vf*kc
                self.w = self.qmax/(self.kc-kj)

    def v(self, k):
        if k<self.kc:
            return self.vf
        else:
            return self.qmax/(self.kj-self.kc)*(1-self.kj/k)

def generateDensities(n, maxDensity):
    return stats.uniform.rvs(size=n)*maxDensity

def generateSpeedVolumes(fd, n, maxDensity, maxHGVProportion = 0, etrucks = 2.5):
    densities = generateDensities(n, maxDensity)
    speeds = [fd.v(k) for k in densities]
    volumes = [fd.q(k) for k in densities]
    if maxHGVProportion > 0:
        hgvProportions = stats.uniform.rvs(size=n)*maxHGVProportion # en pourcent
        volumes = [v/(1+(etrucks-1)*p/100) for v,p in zip(volumes, hgvProportions)]
    else:
        hgvProportions = None
    return speeds, volumes, hgvProportions

higwayMaxDensityLOS = {'A':7, 'B':11, 'C':16, 'D':22, 'E': 28}

def highwayLOS(k):
    'returns the highway level of service for density k in veh/km'
    for los, kmax in higwayMaxDensityLOS.items():
        if k<kmax: return los
    return 'F'
    
#########################
# intersection
#########################

class FourWayIntersection(object):
    '''Simple class for simple intersection outline'''
    def __init__(self, dimension, coordX, coordY):
        self.dimension = dimension
        self.coordX = coordX
        self.coordY = coordY

    def plot(self, options = 'k'):
        from matplotlib.pyplot import plot, axis
    
        minX = min(self.dimension[0])
        maxX = max(self.dimension[0])
        minY = min(self.dimension[1])
        maxY = max(self.dimension[1])
        
        plot([minX, self.coordX[0], self.coordX[0]], [self.coordY[0], self.coordY[0], minY],options)
        plot([self.coordX[1], self.coordX[1], maxX], [minY, self.coordY[0], self.coordY[0]],options)
        plot([minX, self.coordX[0], self.coordX[0]], [self.coordY[1], self.coordY[1], maxY],options)
        plot([self.coordX[1], self.coordX[1], maxX], [maxY, self.coordY[1], self.coordY[1]],options)
        axis('equal')

#########################
# traffic signals
#########################

class Volume(object):
    '''Class to represent volumes with varied vehicule types '''
    def __init__(self, volume, types = ['pc'], proportions = [1], equivalents = [1], nLanes = 1):
        '''mvtEquivalent is the equivalent if the movement is right of left turn'''

        # check the sizes of the lists
        if sum(proportions) == 1:
            self.volume = volume
            self.types = types
            self.proportions = proportions
            self.equivalents = equivalents
            self.nLanes = nLanes
        else:
            print('Proportions do not sum to 1')
            pass

    def checkProtected(self, opposedThroughMvt):
        '''Checks if this left movement should be protected,
        ie if one of the main two conditions on left turn is verified'''
        return self.volume >= 200 or self.volume*opposedThroughMvt.volume/opposedThroughMvt.nLanes > 50000

    def getPCUVolume(self):
        '''Returns the passenger-car equivalent for the input volume'''
        v = 0
        for p, e in zip(self.proportions, self.equivalents):
            v += p*e
        return v*self.volume

class IntersectionMovement(object):
    '''Represents an intersection movement
    with a volume, a type (through, left or right)
    and an equivalent for movement type'''
    def __init__(self, volume, mvtEquivalent = 1):
        self.volume = volume
        self.mvtEquivalent = mvtEquivalent

    def getTVUVolume(self):
        return self.mvtEquivalent*self.volume.getPCUVolume()    

class LaneGroup(object):
    '''Class that represents a group of mouvements'''

    def __init__(self, movements, nLanes):
        self.movements = movements
        self.nLanes = nLanes

    def getTVUVolume(self):
        return sum([mvt.getTVUVolume() for mvt in self.movements])

    def getCharge(self, saturationVolume):
        return self.getTVUVolume()/(self.nLanes*saturationVolume)

def optimalCycle(lostTime, criticalCharge):
    return (1.5*lostTime+5)/(1-criticalCharge)

def minimumCycle(lostTime, criticalCharge, degreeSaturation=1.):
    'degree of saturation can be used as the peak hour factor too'
    return lostTime/(1-criticalCharge/degreeSaturation)

class Cycle(object):
    '''Class to compute optimal cycle and the split of effective green times'''
    def __init__(self, phases, lostTime, saturationVolume):
        '''phases is a list of phases
        a phase is a list of lanegroups'''
        self.phases = phases
        self.lostTime = lostTime
        self.saturationVolume = saturationVolume

    def computeCriticalCharges(self):
        self.criticalCharges = [max([lg.getCharge(self.saturationVolume) for lg in phase]) for phase in self.phases]
        self.criticalCharge = sum(self.criticalCharges)
        
    def computeOptimalCycle(self):
        self.computeCriticalCharges()
        self.C = optimalCycle(self.lostTime, self.criticalCharge)
        return self.C

    def computeMinimumCycle(self, degreeSaturation=1.):
        self.computeCriticalCharges()
        self.C = minimumCycle(self.lostTime, self.criticalCharge, degreeSaturation)
        return self.C

    def computeEffectiveGreen(self):
        #from numpy import round
        #self.computeCycle() # in case it was not done before
        effectiveGreenTime = self.C-self.lostTime
        self.effectiveGreens = [round(c*effectiveGreenTime/self.criticalCharge,1) for c in self.criticalCharges]
        return self.effectiveGreens


def computeInterGreen(perceptionReactionTime, initialSpeed, intersectionLength, vehicleAverageLength = 6, deceleration = 3):
    '''Computes the intergreen time (yellow/amber plus all red time)
    Deceleration is positive
    All variables should be in the same units'''
    if deceleration > 0:
        return [perceptionReactionTime+float(initialSpeed)/(2*deceleration), float(intersectionLength+vehicleAverageLength)/initialSpeed]
    else:
        print('Issue deceleration should be strictly positive')
        return None

def uniformDelay(cycleLength, effectiveGreen, saturationDegree):
    '''Computes the uniform delay'''
    return 0.5*cycleLength*(1-float(effectiveGreen)/cycleLength)**2/(1-float(effectiveGreen*saturationDegree)/cycleLength)

def randomDelay(volume, saturationDegree):
    '''Computes the random delay = queueing time for M/D/1'''
    return saturationDegree**2/(2*volume*(1-saturationDegree))

def incrementalDelay(T, X, c, k=0.5, I=1):
    '''Computes the incremental delay (HCM)
    T in hours
    c capacity of the lane group
    k default for fixed time signal
    I=1 for isolated intersection (Poisson arrival)'''
    from math import sqrt
    return 900*T*(X - 1 + sqrt((X - 1)**2 + 8*k*I*X/(c*T)))

#########################
# misc
#########################

def timeChangingSpeed(v0, vf, a, TPR):
    'for decelerations, a < 0'
    return TPR-(vf-v0)/a

def distanceChangingSpeed(v0, vf, a, TPR):
    'for decelerations, a < 0'
    return TPR*v0+(vf**2-v0**2)/(2*a)
