#! /usr/bin/env python
'''Libraries for detecting, counting, etc., road users'''

from numpy import mean, isnan

from trafficintelligence import moving

# TODO graphical user interface for creation

class Sensor:
    def detect(self, o):
        print("Detect method not implemented")
        return False
    
    def detectInstants(self, o):
        print("DetectInstants method not implemented")
        return []

class BoxSensor(Sensor):
    def __init__(self, polygon, minNPointsInBox = 1):
        self.polygon = polygon # check 2xN?
        self.minNPointsInBox = minNPointsInBox
    
    def detectInstants(self, obj):
        indices = obj.getPositions().getInstantsInPolygon(self.polygon)
        firstInstant = obj.getFirstInstant()
        return [i+firstInstant for i in indices]

    def detect(self, obj):
        instants = self.detectInstants(obj)
        return len(instants) >= self.minNPointsInBox

def detectAnd(sensors, obj):
    'Returns True if all sensors detect the object'
    result = True
    for s in sensors:
        result = result and s.detect(obj)
        if not result:
            return result
    return result

def detectOr(sensors, obj):
    'Returns True if any sensor detects the object'
    result = False
    for s in sensors:
        result = result or s.detect(obj)
        if result:
            return result
    return result

def detectAndOrder(sensors, obj):
    'Returns True if all sensors are detected and in their order'
    detectionInstants = []
    for s in sensors:
        instants = s.detectInstants(obj)
        if len(instants) == 0:
            return False
        else:
            detectionInstants.append(mean(instants))
    result = True
    for i in range(len(sensors)-1):
        result = result and (detectionInstants[i] <= detectionInstants[i+1])
        if not result:
            return result
    return result
