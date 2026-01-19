#! /usr/bin/env python
'''Various utilities to load data saved by the UBC tool(s)'''

from trafficintelligence import utils, events, storage, indicators
from trafficintelligence.moving import MovingObject, TimeInterval, Trajectory


fileTypeNames = ['feature',
                 'object',
                 'prototype',
                 'contoursequence']

severityIndicatorNames = ['Distance',
                          'Collision Course Cosine',
                          'Velocity Cosine',
                          'Speed Differential',
                          'Collision Probability',
                          'Severity Index',
                          'Time to Collision']

userTypeNames = ['car',
                 'pedestrian',
                 'twowheels',
                 'bus',
                 'truck']

# severityIndicator = {'Distance': 0,
#                      'Cosine': 1,
#                      'Velocity Cosine': 2,
#                      'Speed Differential': 3,
#                      'Collision Probability': 4,
#                      'Severity Index': 5,
#                      'TTC': 6}

mostSevereIsMax = [False, 
                   False, 
                   True, 
                   True, 
                   True, 
                   True, 
                   False]

ignoredValue = [None, None, None, None, None, None, -1]

def getFileType(s):
    'Finds the type in fileTypeNames'
    for fileType in fileTypeNames:
        if s.find(fileType)>0:
            return fileType
    return ''

def isFileType(s, fileType):
    return (s.find(fileType)>0)

def saveTrajectoryUserTypes(inFilename, outFilename, objects):
    '''The program saves the objects, 
    by just copying the corresponding trajectory and velocity data
    from the inFilename, and saving the characteristics in objects (first line)
    into outFilename'''
    infile = utils.openCheck(inFilename)
    outfile = utils.openCheck(outFilename,'w')

    if (inFilename.find('features') >= 0) or infile is None or outfile is None:
        return

    lines = utils.getLines(infile)
    objNum = 0 # in inFilename
    while lines != []:
        # find object in objects (index i)
        i = 0
        while (i<len(objects)) and (objects[i].num != objNum):
            i+=1

        if i<len(objects):
            l = lines[0].split(' ')
            l[3] = str(objects[i].userType)
            outfile.write(' '.join(l)+'\n')
            for l in lines[1:]:
                outfile.write(l+'\n')
            outfile.write(utils.delimiterChar+'\n')
        # next object
        objNum += 1
        lines = utils.getLines(infile)

    print('read {0} objects'.format(objNum))

def modifyTrajectoryFile(modifyLines, filenameIn, filenameOut):
    '''Reads filenameIn, replaces the lines with the result of modifyLines and writes the result in filenameOut'''
    fileIn = utils.openCheck(filenameIn, 'r', True)
    fileOut = utils.openCheck(filenameOut, "w", True)

    lines = utils.getLines(fileIn)
    trajNum = 0
    while (lines != []):
        modifiedLines = modifyLines(trajNum, lines)
        if modifiedLines:
            for l in modifiedLines:
                fileOut.write(l+"\n")
            fileOut.write(utils.delimiterChar+"\n")
        lines = utils.getLines(fileIn)
        trajNum += 1
         
    fileIn.close()
    fileOut.close()

def copyTrajectoryFile(keepTrajectory, filenameIn, filenameOut):
    '''Reads filenameIn, keeps the trajectories for which the function keepTrajectory(trajNum, lines) is True
    and writes the result in filenameOut'''
    fileIn = utils.openCheck(filenameIn, 'r', True)
    fileOut = utils.openCheck(filenameOut, "w", True)

    lines = utils.getLines(fileIn)
    trajNum = 0
    while (lines != []):
        if keepTrajectory(trajNum, lines):
            for l in lines:
                fileOut.write(l+"\n")
            fileOut.write(utils.delimiterChar+"\n")
        lines = utils.getLines(fileIn)
        trajNum += 1
        
    fileIn.close()
    fileOut.close()

def loadTrajectories(filename, nObjects = -1):
    '''Loads trajectories'''

    f = utils.openCheck(filename)
    if f is None:
        return []

    objects = []
    objNum = 0
    objectType = getFileType(filename)
    lines = utils.getLines(f)
    while (lines != []) and ((nObjects<0) or (objNum<nObjects)):
        l = lines[0].split(' ')
        parsedLine = [int(n) for n in l[:4]]
        obj = MovingObject(num = objNum, timeInterval = TimeInterval(parsedLine[1],parsedLine[2]))
        #add = True
        if len(lines) >= 3:
            obj.positions = Trajectory.load(lines[1], lines[2])
            if len(lines) >= 5:
                obj.velocities = Trajectory.load(lines[3], lines[4])
                if objectType == 'object':
                    obj.userType = parsedLine[3]
                    obj.nObjects = float(l[4])
                    obj.featureNumbers = [int(n) for n in l[5:]]
                    
                    # load contour data if available
                    if len(lines) >= 6:
                        obj.contourType = utils.line2Floats(lines[6])
                        obj.contourOrigins = Trajectory.load(lines[7], lines[8])
                        obj.contourSizes = Trajectory.load(lines[9], lines[10])
                elif objectType == 'prototype':
                    obj.userType = parsedLine[3]
                    obj.nMatchings = int(l[4])

        if len(lines) != 2:
            objects.append(obj)
            objNum+=1
        else:
            print("Error two lines of data for feature {}".format(f.num))

        lines = utils.getLines(f)

    f.close()
    return objects
   
def getFeatureNumbers(objects):
    featureNumbers=[]
    for o in objects:
        featureNumbers += o.featureNumbers
    return featureNumbers

def loadInteractions(filename, nInteractions = -1):
    'Loads interactions from the old UBC traffic event format'
    f = utils.openCheck(filename)
    if f is None:
        return []

    interactions = []
    interactionNum = 0
    lines = utils.getLines(f)
    while (lines != []) and ((nInteractions<0) or (interactionNum<nInteractions)):
        parsedLine = [int(n) for n in lines[0].split(' ')]
        inter = events.Interaction(interactionNum, TimeInterval(parsedLine[1],parsedLine[2]), parsedLine[3], parsedLine[4], categoryNum = parsedLine[5])
        
        indicatorFrameNums = [int(n) for n in lines[1].split(' ')]
        for indicatorNum,line in enumerate(lines[2:]):
            values = {}
            for i,v in enumerate([float(n) for n in line.split(' ')]):
                if not ignoredValue[indicatorNum] or v != ignoredValue[indicatorNum]:
                    values[indicatorFrameNums[i]] = v
            inter.addIndicator(indicators.SeverityIndicator(severityIndicatorNames[indicatorNum], values, None, mostSevereIsMax[indicatorNum]))

        interactions.append(inter)
        interactionNum+=1
        lines = utils.getLines(f)

    f.close()
    return interactions

def loadCollisionPoints(filename, nPoints = -1):
    '''Loads collision points and returns a dict
    with keys as a pair of the numbers of the two interacting objects'''
    f = utils.openCheck(filename)
    if f is None:
        return []

    points = {}
    num = 0
    lines = utils.getLines(f)
    while (lines != []) and ((nPoints<0) or (num<nPoints)):
        parsedLine = [int(n) for n in lines[0].split(' ')]
        protagonistNums = (parsedLine[0], parsedLine[1])
        points[protagonistNums] = [[float(n) for n in lines[1].split(' ')],
                                   [float(n) for n in lines[2].split(' ')]]

        num+=1
        lines = utils.getLines(f)

    f.close()
    return points
