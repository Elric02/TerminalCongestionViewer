#! /usr/bin/env python
# -*- coding: utf-8 -*-
'''Various utilities to save and load data'''

from pathlib import Path
import shutil
from copy import copy
import sqlite3, logging

from numpy import log, min as npmin, max as npmax, round as npround, array, sum as npsum, loadtxt, floor as npfloor, ceil as npceil, linalg, int32, int64, reshape, dot, vstack, transpose, ones, zeros_like, pi, nan
from pandas import read_csv, merge, concat, DataFrame

from trafficintelligence import utils, moving, events, indicators
from trafficintelligence.base import VideoFilenameAddable


ngsimUserTypes = {'twowheels':1,
                  'car':2,
                  'truck':3}

tableNames = {'feature':'positions',
              'object': 'objects',
              'objectfeatures': 'positions'}

sqlite3.register_adapter(int64, lambda val: int(val))
sqlite3.register_adapter(int32, lambda val: int(val))

#########################
# Sqlite
#########################

# utils
def printDBError(error):
    print('DB Error: {}'.format(error))

def dropTables(connection, tableNames):
    'deletes the table with names in tableNames'
    try:
        cursor = connection.cursor()
        for tableName in tableNames:
            cursor.execute('DROP TABLE IF EXISTS '+tableName)
    except sqlite3.OperationalError as error:
        printDBError(error)

def deleteFromSqlite(filename, dataType):
    'Deletes (drops) some tables in the filename depending on type of data'
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            if dataType == 'object':
                dropTables(connection, ['objects', 'objects_features'])
            elif dataType == 'interaction':
                dropTables(connection, ['interactions', 'indicators'])
            elif dataType == 'curvilinear':
                dropTables(connection, ['curvilinear_positions'])
            elif dataType == 'bb':
                dropTables(connection, ['bounding_boxes'])
            elif dataType == 'pois':
                dropTables(connection, ['gaussians2d', 'objects_pois'])
            elif dataType == 'prototype':
                dropTables(connection, ['prototypes', 'objects_prototypes', 'features_prototypes'])
            else:
                print('Unknown data type {} to delete from database'.format(dataType))
    else:
        print('{} does not exist'.format(filename))

def tableExists(connection, tableName):
    'indicates if the table exists in the database'
    try:
        cursor = connection.cursor()
        cursor.execute('SELECT COUNT(*) FROM SQLITE_MASTER WHERE type = \'table\' AND name = \''+tableName+'\'')
        return cursor.fetchone()[0] == 1
    except sqlite3.OperationalError as error:
        printDBError(error)        

def tableNames(filename):
    'Lists the names of the tables in the SQLite file'
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            try:
                cursor = connection.cursor()
                cursor.execute('SELECT name FROM sqlite_master WHERE type = \'table\'')
                return [row[0] for row in cursor]
            except sqlite3.OperationalError as error:
                printDBError(error)
    return []
        
def createTrajectoryTable(cursor, tableName):
    if tableName.endswith('positions') or tableName.endswith('velocities'):
        cursor.execute("CREATE TABLE IF NOT EXISTS "+tableName+" (trajectory_id INTEGER, frame_number INTEGER, x_coordinate REAL, y_coordinate REAL, PRIMARY KEY(trajectory_id, frame_number))")
    else:
        print('Unallowed name {} for trajectory table'.format(tableName))

def createObjectsTable(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS objects (object_id INTEGER, road_user_type INTEGER, n_objects INTEGER, PRIMARY KEY(object_id))")

def createAssignmentTable(cursor, objectType1, objectType2, objectIdColumnName1, objectIdColumnName2):
    cursor.execute("CREATE TABLE IF NOT EXISTS "+objectType1+"s_"+objectType2+"s ("+objectIdColumnName1+" INTEGER, "+objectIdColumnName2+" INTEGER, PRIMARY KEY("+objectIdColumnName1+","+objectIdColumnName2+"))")

def createObjectsFeaturesTable(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS objects_features (object_id INTEGER, trajectory_id INTEGER, PRIMARY KEY(object_id, trajectory_id))")


def createCurvilinearTrajectoryTable(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS curvilinear_positions (trajectory_id INTEGER, frame_number INTEGER, s_coordinate REAL, y_coordinate REAL, lane TEXT, PRIMARY KEY(trajectory_id, frame_number))")

def createFeatureCorrespondenceTable(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS feature_correspondences (trajectory_id INTEGER, source_dbname VARCHAR, db_trajectory_id INTEGER, PRIMARY KEY(trajectory_id))')

def createInteractionTable(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS interactions (id INTEGER PRIMARY KEY, object_id1 INTEGER, object_id2 INTEGER, first_frame_number INTEGER, last_frame_number INTEGER, FOREIGN KEY(object_id1) REFERENCES objects(id), FOREIGN KEY(object_id2) REFERENCES objects(id))')

def createIndicatorTable(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS indicators (interaction_id INTEGER, indicator_type INTEGER, frame_number INTEGER, value REAL, FOREIGN KEY(interaction_id) REFERENCES interactions(id), PRIMARY KEY(interaction_id, indicator_type, frame_number))')

def insertTrajectoryQuery(tableName):
    return "INSERT INTO "+tableName+" VALUES (?,?,?,?)"

def insertObjectQuery():
    return "INSERT INTO objects VALUES (?,?,?)"

def insertObjectFeatureQuery():
    return "INSERT INTO objects_features VALUES (?,?)"

def createIndex(connection, tableName, columnName, unique = False):
    '''Creates an index for the column in the table
    I will make querying with a condition on this column faster'''
    try:
        cursor = connection.cursor()
        s = "CREATE "
        if unique:
            s += "UNIQUE "
        cursor.execute(s+"INDEX IF NOT EXISTS "+tableName+"_"+columnName+"_index ON "+tableName+"("+columnName+")")
        connection.commit()
        #connection.close()
    except sqlite3.OperationalError as error:
        printDBError(error)

def getNumberRowsTable(connection, tableName, columnName = None):
    '''Returns the number of rows for the table
    If columnName is not None, means we want the number of distinct values for that column
    (otherwise, we can just count(*))'''
    try:
        cursor = connection.cursor()
        if columnName is None:
            cursor.execute("SELECT COUNT(*) from "+tableName)
        else:
            cursor.execute("SELECT COUNT(DISTINCT "+columnName+") from "+tableName)
        return cursor.fetchone()[0]
    except sqlite3.OperationalError as error:
        printDBError(error)

def getMinMax(connection, tableName, columnName, minmax):
    '''Returns max/min or both for given column in table
    minmax must be string max, min or minmax'''
    try:
        cursor = connection.cursor()
        if minmax == 'min' or minmax == 'max':
            cursor.execute("SELECT "+minmax+"("+columnName+") from "+tableName)
        elif minmax == 'minmax':
            cursor.execute("SELECT MIN("+columnName+"), MAX("+columnName+") from "+tableName)
        else:
            print("Argument minmax unknown: {}".format(minmax))
        return cursor.fetchone()[0]
    except sqlite3.OperationalError as error:
        printDBError(error)

def getObjectCriteria(objectNumbers):
    if objectNumbers is None:
        query = ''
    elif type(objectNumbers) == int:
        query = '<= {0}'.format(objectNumbers-1)
    elif type(objectNumbers) == list:
        query = 'in ('+', '.join([str(n) for n in objectNumbers])+')'
    else:
        print('objectNumbers {} are not a known type ({})'.format(objectNumbers, type(objectNumbers)))
        query = ''
    return query

def loadTrajectoriesFromTable(connection, tableName, trajectoryType, objectNumbers = None, timeStep = None):
    '''Loads trajectories (in the general sense) from the given table
    can be positions or velocities

    returns a moving object'''
    cursor = connection.cursor()

    try:
        objectCriteria = getObjectCriteria(objectNumbers)
        queryStatement = None
        if trajectoryType == 'feature':
            queryStatement = 'SELECT trajectory_id, frame_number, x_coordinate, y_coordinate from '+tableName
            if objectNumbers is not None and timeStep is not None:
                queryStatement += ' WHERE trajectory_id '+objectCriteria+' AND frame_number%{} = 0'.format(timeStep)
            elif objectNumbers is not None:
                queryStatement += ' WHERE trajectory_id '+objectCriteria
            elif timeStep is not None:
                queryStatement += ' WHERE frame_number%{} = 0'.format(timeStep)
            queryStatement += ' ORDER BY trajectory_id, frame_number'
        elif trajectoryType == 'object':
            queryStatement = 'SELECT OF.object_id, P.frame_number, avg(P.x_coordinate), avg(P.y_coordinate) from '+tableName+' P, objects_features OF WHERE P.trajectory_id = OF.trajectory_id'
            if objectNumbers is not None:
                queryStatement += ' AND OF.object_id '+objectCriteria
            if timeStep is not None:
                queryStatement += ' AND P.frame_number%{} = 0'.format(timeStep)
            queryStatement += ' GROUP BY OF.object_id, P.frame_number ORDER BY OF.object_id, P.frame_number'
        elif trajectoryType in ['bbtop', 'bbbottom']:
            if trajectoryType == 'bbtop':
                corner = 'top_left'
            elif trajectoryType == 'bbbottom':
                corner = 'bottom_right'
            queryStatement = 'SELECT object_id, frame_number, x_'+corner+', y_'+corner+' FROM '+tableName
            if objectNumbers is not None and timeStep is not None:
                queryStatement += ' WHERE object_id '+objectCriteria+' AND frame_number%{} = 0'.format(timeStep)
            elif objectNumbers is not None:
                queryStatement += ' WHERE object_id '+objectCriteria
            elif timeStep is not None:
                queryStatement += ' WHERE frame_number%{} = 0'.format(timeStep)
            queryStatement += ' ORDER BY object_id, frame_number'
        else:
            print('Unknown trajectory type {}'.format(trajectoryType))
        if queryStatement is not None:
            cursor.execute(queryStatement)
            logging.debug(queryStatement)
    except sqlite3.OperationalError as error:
        printDBError(error)
        return []

    objId = -1
    obj = None
    objects = []
    for row in cursor:
        if row[0] != objId:
            objId = row[0]
            if obj is not None and (obj.length() == obj.positions.length() or (timeStep is not None and npceil(obj.length()/timeStep) == obj.positions.length())):
                objects.append(obj)
            elif obj is not None:
                print('Object {} is missing {} positions'.format(obj.getNum(), int(obj.length())-obj.positions.length()))
            obj = moving.MovingObject(row[0], timeInterval = moving.TimeInterval(row[1], row[1]), positions = moving.Trajectory([[row[2]],[row[3]]]))
        else:
            obj.timeInterval.last = row[1]
            obj.positions.addPositionXY(row[2],row[3])

    if obj is not None and (obj.length() == obj.positions.length() or (timeStep is not None and npceil(obj.length()/timeStep) == obj.positions.length())):
        objects.append(obj)
    elif obj is not None:
        print('Object {} is missing {} positions'.format(obj.getNum(), int(obj.length())-obj.positions.length()))

    return objects

def loadObjectAttributesFromTable(cursor, objectNumbers, loadNObjects = False):
    '''Loads the object user_type and n_objects fields'''
    objectCriteria = getObjectCriteria(objectNumbers)
    queryStatement = 'SELECT object_id, road_user_type'
    if loadNObjects:
        queryStatement += ', n_objects'
    queryStatement += ' FROM objects'
    if objectNumbers is not None:
        queryStatement += ' WHERE object_id '+objectCriteria
    cursor.execute(queryStatement)
    attributes = {}
    if loadNObjects:
        for row in cursor:
            attributes[row[0]] = row[1:]
    else:
        for row in cursor:
            attributes[row[0]] = row[1]
    return attributes

def loadTrajectoriesFromSqlite(filename, trajectoryType, objectNumbers = None, withFeatures = False, timeStep = None, nLongestFeaturesPerObject = None):
    '''Loads the trajectories (in the general sense, 
    either features, objects (feature groups), longest features per object, or bounding box series)
    types are only feature or object
    if object, features can be loaded with withFeatures or nLongestObjectFeatures used to select the n longest features

    The number loaded is either the first objectNumbers objects,
    or the indices in objectNumbers from the database'''
    objects = []
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            objects = loadTrajectoriesFromTable(connection, 'positions', trajectoryType, objectNumbers, timeStep)
            objectVelocities = loadTrajectoriesFromTable(connection, 'velocities', trajectoryType, objectNumbers, timeStep)

            if len(objectVelocities) > 0:
                for o,v in zip(objects, objectVelocities):
                    if o.getNum() == v.getNum():
                        o.velocities = v.positions
                        o.velocities.duplicateLastPosition() # avoid having velocity shorter by one position than positions
                    else:
                        print('Could not match positions {0} with velocities {1}'.format(o.getNum(), v.getNum()))

            if trajectoryType == 'object':
                cursor = connection.cursor()
                try:
                    # attribute feature numbers to objects
                    queryStatement = 'SELECT trajectory_id, object_id FROM objects_features'
                    if objectNumbers is not None:
                        queryStatement += ' WHERE object_id '+getObjectCriteria(objectNumbers)
                    queryStatement += ' ORDER BY object_id' # order is important to group all features per object
                    logging.debug(queryStatement)
                    cursor.execute(queryStatement)

                    featureNumbers = {}
                    for row in cursor:
                        objId = row[1]
                        if objId not in featureNumbers:
                            featureNumbers[objId] = [row[0]]
                        else:
                            featureNumbers[objId].append(row[0])

                    for obj in objects:
                        obj.featureNumbers = featureNumbers[obj.getNum()]

                    # load userType
                    attributes = loadObjectAttributesFromTable(cursor, objectNumbers, True)
                    for obj in objects:
                        userType, nObjects = attributes[obj.getNum()]
                        obj.setUserType(userType)
                        obj.setNObjects(nObjects)

                    # add features
                    if withFeatures:
                        for obj in objects:
                            obj.features = loadTrajectoriesFromSqlite(filename, 'feature', obj.featureNumbers, timeStep = timeStep)
                    elif nLongestFeaturesPerObject is not None:
                        for obj in objects:
                            queryStatement = 'SELECT trajectory_id, max(frame_number)-min(frame_number) AS length FROM positions WHERE trajectory_id '+getObjectCriteria(obj.featureNumbers)+' GROUP BY trajectory_id ORDER BY length DESC'
                            logging.debug(queryStatement)
                            cursor.execute(queryStatement)
                            obj.features = loadTrajectoriesFromSqlite(filename, 'feature', [row[0] for i,row in enumerate(cursor) if i<nLongestFeaturesPerObject], timeStep = timeStep)

                except sqlite3.OperationalError as error:
                    printDBError(error)
    else:
        print('Impossible to load from non-existing file '+filename)
    return objects

def loadObjectFeatureFrameNumbers(filename, objectNumbers = None):
    'Loads the feature frame numbers for each object'
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            queryStatement = 'SELECT OF.object_id, TL.trajectory_id, TL.length FROM (SELECT trajectory_id, max(frame_number)-min(frame_number) AS length FROM positions GROUP BY trajectory_id) TL, objects_features OF WHERE TL.trajectory_id = OF.trajectory_id'
            if objectNumbers is not None:
                queryStatement += ' AND object_id '+getObjectCriteria(objectNumbers)
            queryStatement += ' ORDER BY OF.object_id, TL.length DESC'
            logging.debug(queryStatement)
            cursor.execute(queryStatement)
            objectFeatureNumbers = {}
            for row in cursor:
                objId = row[0]
                if objId in objectFeatureNumbers:
                    objectFeatureNumbers[objId].append(row[1])
                else:
                    objectFeatureNumbers[objId] = [row[1]]
            return objectFeatureNumbers
        except sqlite3.OperationalError as error:
            printDBError(error)
            return None

def addCurvilinearTrajectoriesFromSqlite(filename, objects):
    '''Adds curvilinear positions (s_coordinate, y_coordinate, lane)
    from a database to an existing MovingObject dict (indexed by each objects's num) or list'''
    if type(objects) == list:
        tmpobjects = {obj.getNum(): obj for obj in objects}
    elif type(objects) == dict:
        tmpobjects = objects
    else:
        print('objects should be a list of dictionary of objects indexed by their num')
        return None
    
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()

        try:
            cursor.execute('SELECT * from curvilinear_positions order by trajectory_id, frame_number')
        except sqlite3.OperationalError as error:
            printDBError(error)
            return []

        missingObjectNumbers = []
        objNum = None
        for row in cursor:
            if objNum != row[0]:
                objNum = row[0]
                if objNum in tmpobjects:
                    tmpobjects[objNum].curvilinearPositions = moving.CurvilinearTrajectory()
                else:
                    missingObjectNumbers.append(objNum)
            if objNum in tmpobjects:
                tmpobjects[objNum].curvilinearPositions.addPositionSYL(row[2],row[3],row[4])
        if len(missingObjectNumbers) > 0:
            print('List of missing objects to attach corresponding curvilinear trajectories: {}'.format(missingObjectNumbers))

def saveTrajectoriesToTable(connection, objects, trajectoryType):
    'Saves trajectories in table tableName'
    cursor = connection.cursor()
    # Parse feature and/or object structure and commit to DB
    if(trajectoryType == 'feature' or trajectoryType == 'object'):
        # Extract features from objects
        if trajectoryType == 'object':
            features = []
            for obj in objects:
                if obj.hasFeatures():
                    features += obj.getFeatures()
            if len(features) == 0:
                print('Warning, objects have no features') # todo save centroid trajectories?
        elif trajectoryType == 'feature':
            features = objects
        # Setup feature queries
        createTrajectoryTable(cursor, "positions")
        createTrajectoryTable(cursor, "velocities")
        positionQuery = insertTrajectoryQuery("positions")
        velocityQuery = insertTrajectoryQuery("velocities")
        # Setup object queries
        if trajectoryType == 'object':    
            createObjectsTable(cursor)
            createObjectsFeaturesTable(cursor)
            objectQuery = insertObjectQuery()
            objectFeatureQuery = insertObjectFeatureQuery()
        for feature in features:
            num = feature.getNum()
            frameNum = feature.getFirstInstant()
            for p in feature.getPositions():
                cursor.execute(positionQuery, (num, frameNum, p.x, p.y))
                frameNum += 1
            velocities = feature.getVelocities()
            if velocities is not None:
                frameNum = feature.getFirstInstant()
                for v in velocities[:-1]:
                    cursor.execute(velocityQuery, (num, frameNum, v.x, v.y))
                    frameNum += 1
        if trajectoryType == 'object':
            for obj in objects:
                if obj.hasFeatures():
                    for feature in obj.getFeatures():
                        featureNum = feature.getNum()
                        cursor.execute(objectFeatureQuery, (obj.getNum(), featureNum))
                cursor.execute(objectQuery, (obj.getNum(), obj.getUserType(), obj.nObjects if hasattr(obj, 'nObjects') and obj.nObjects is not None else 1))   
    # Parse curvilinear position structure
    elif(trajectoryType == 'curvilinear'):
        createCurvilinearTrajectoryTable(cursor)
        curvilinearQuery = "INSERT INTO curvilinear_positions VALUES (?,?,?,?,?)"
        for obj in objects:
            num = obj.getNum()
            frameNum = obj.getFirstInstant()
            for p in obj.getCurvilinearPositions():
                cursor.execute(curvilinearQuery, (num, frameNum, p[0], p[1], p[2]))
                frameNum += 1
    else:
        print('Unknown trajectory type {}'.format(trajectoryType))
    connection.commit()

def saveTrajectoriesToSqlite(outputFilename, objects, trajectoryType):
    '''Writes features, ie the trajectory positions (and velocities if exist)
    with their instants to a specified sqlite file
    Either feature positions (and velocities if they exist)
    or curvilinear positions will be saved at a time'''

    with sqlite3.connect(outputFilename) as connection:
        try:
            saveTrajectoriesToTable(connection, objects, trajectoryType)
        except sqlite3.OperationalError as error:
            printDBError(error)

def setRoadUserTypes(filename, objects):
    '''Saves the user types of the objects in the sqlite database stored in filename
    The objects should exist in the objects table'''
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        for obj in objects:
            cursor.execute('update objects set road_user_type = {} WHERE object_id = {}'.format(obj.getUserType(), obj.getNum()))
        connection.commit()

# todo addBBTrajectories -> remove class and just add, comme curvilinear
def addBoundingBoxTrajectoriesFromSqlite(filename, objects):
    '''Adds curvilinear positions (s_coordinate, y_coordinate, lane)
    from a database to an existing MovingObject dict (indexed by each objects's num) or list'''
    pass

def loadBBMovingObjectsFromSqlite(filename, objectNumbers = None, timeStep = None):
    '''Loads bounding box moving object from an SQLite
    (format of SQLite output by the ground truth annotation tool
    or Urban Tracker'''
    objects = []
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            topCorners = loadTrajectoriesFromTable(connection, 'bounding_boxes', 'bbtop', objectNumbers, timeStep)
            bottomCorners = loadTrajectoriesFromTable(connection, 'bounding_boxes', 'bbbottom', objectNumbers, timeStep)
            userTypes = loadObjectAttributesFromTable(connection.cursor(), objectNumbers) # string format is same as object

            for i in range(len(topCorners)):#t, b in zip(topCorners, bottomCorners):
                num = topCorners[i].getNum()
                if num == bottomCorners[i].getNum():
                    annotation = moving.BBMovingObject(topCorners[i], bottomCorners[i], num, topCorners[i].getTimeInterval(), userTypes[num])
                    objects.append(annotation)
                else:
                    print('Mismatch id for top left and bottom right corners (resp. {} {})'.format(num, bottomCorners[i].getNum()))
    return objects

def saveBBMovingObjectsToSqlite(outputFilename, objects):
    '''Saves bounding boxes as obtained from annotation
    object detector-based tracking tool like dltrack
    CREATE TABLE bounding_boxes ( object_id INTEGER, frame_number INTEGER, x_top_left REAL, y_top_left REAL, x_bottom_right REAL, y_bottom_right REAL,  PRIMARY KEY( object_id, frame_number ) )'''
    pass

def saveInteraction(cursor, interaction):
    roadUserNumbers = list(interaction.getRoadUserNumbers())
    cursor.execute('INSERT INTO interactions VALUES({}, {}, {}, {}, {})'.format(interaction.getNum(), roadUserNumbers[0], roadUserNumbers[1], interaction.getFirstInstant(), interaction.getLastInstant()))

def saveInteractionsToSqlite(filename, interactions):
    'Saves only the interactions in the table'
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            createInteractionTable(cursor)
            for inter in interactions:
                saveInteraction(cursor, inter)
        except sqlite3.OperationalError as error:
            printDBError(error)
        connection.commit()

def saveIndicator(cursor, interactionNum, indicator):
    for instant in indicator.getTimeInterval():
        if indicator[instant]:
            cursor.execute('INSERT INTO indicators VALUES({}, {}, {}, {})'.format(interactionNum, events.Interaction.indicatorNameToIndices[indicator.getName()], instant, indicator[instant]))

def saveIndicatorsToSqlite(filename, interactions, indicatorNames = events.Interaction.indicatorNames):
    'Saves the indicator values in the table'
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            createInteractionTable(cursor)
            createIndicatorTable(cursor)
            for inter in interactions:
                saveInteraction(cursor, inter)
                for indicatorName in indicatorNames:
                    indicator = inter.getIndicator(indicatorName)
                    if indicator is not None:
                        saveIndicator(cursor, inter.getNum(), indicator)
        except sqlite3.OperationalError as error:
            printDBError(error)
        connection.commit()

def loadInteractionsFromSqlite(filename):
    '''Loads interaction and their indicators
    
    TODO choose the interactions to load'''
    interactions = []
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT INT.id, INT.object_id1, INT.object_id2, INT.first_frame_number, INT.last_frame_number, IND.indicator_type, IND.frame_number, IND.value from interactions INT, indicators IND WHERE INT.id = IND.interaction_id ORDER BY INT.id, IND.indicator_type, IND.frame_number')
                interactionNum = -1
                indicatorTypeNum = -1
                tmpIndicators = {}
                for row in cursor:
                    if row[0] != interactionNum:
                        interactionNum = row[0]
                        interactions.append(events.Interaction(interactionNum, moving.TimeInterval(row[3],row[4]), row[1], row[2]))
                        interactions[-1].indicators = {}
                    if indicatorTypeNum != row[5] or row[0] != interactionNum:
                        indicatorTypeNum = row[5]
                        indicatorName = events.Interaction.indicatorNames[indicatorTypeNum]
                        indicatorValues = {row[6]:row[7]}
                        interactions[-1].indicators[indicatorName] = indicators.SeverityIndicator(indicatorName, indicatorValues, mostSevereIsMax = not indicatorName in events.Interaction.mostSevereIsMinIndicators)
                    else:
                        indicatorValues[row[6]] = row[7]
                        interactions[-1].indicators[indicatorName].timeInterval.last = row[6]
            except sqlite3.OperationalError as error:
                printDBError(error)
                return []
    return interactions
# load first and last object instants
# CREATE TEMP TABLE IF NOT EXISTS object_instants AS SELECT OF.object_id, min(frame_number) as first_instant, max(frame_number) as last_instant from positions P, objects_features OF WHERE P.trajectory_id = OF.trajectory_id group by OF.object_id order by OF.object_id

def createBoundingBoxTableFromFeatures(filename, invHomography = None):
    '''Create the table to store the object bounding boxes in image space
    calculated from the object feature coordinates'''
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute('CREATE TABLE IF NOT EXISTS bounding_boxes (object_id INTEGER, frame_number INTEGER, x_top_left REAL, y_top_left REAL, x_bottom_right REAL, y_bottom_right REAL,  PRIMARY KEY(object_id, frame_number))')
            cursor.execute('INSERT INTO bounding_boxes SELECT object_id, frame_number, min(x), min(y), max(x), max(y) from '
                  '(SELECT object_id, frame_number, (x*{}+y*{}+{})/w as x, (x*{}+y*{}+{})/w as y from '
                  '(SELECT OF.object_id, P.frame_number, P.x_coordinate as x, P.y_coordinate as y, P.x_coordinate*{}+P.y_coordinate*{}+{} as w from positions P, objects_features OF WHERE P.trajectory_id = OF.trajectory_id)) '.format(invHomography[0,0], invHomography[0,1], invHomography[0,2], invHomography[1,0], invHomography[1,1], invHomography[1,2], invHomography[2,0], invHomography[2,1], invHomography[2,2])+
                  'GROUP BY object_id, frame_number')
        except sqlite3.OperationalError as error:
            printDBError(error)
        connection.commit()

def loadBoundingBoxTableForDisplay(filename):
    '''Loads bounding boxes from bounding_boxes table for display over trajectories'''
    boundingBoxes = {} # list of bounding boxes for each instant
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'bounding_boxes\'')
                result = cursor.fetchall()
                if len(result) > 0:
                    cursor.execute('SELECT * FROM bounding_boxes')
                    for row in cursor:
                        boundingBoxes.setdefault(row[1], []).append([moving.Point(row[2], row[3]), moving.Point(row[4], row[5])])
            except sqlite3.OperationalError as error:
                printDBError(error)
    return boundingBoxes

#########################
# saving and loading for scene interpretation: POIs and Prototypes
#########################

def savePrototypesToSqlite(filename, prototypes):
    '''save the prototypes (a prototype is defined by a filename, a number (id) and type)'''
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute('CREATE TABLE IF NOT EXISTS prototypes (prototype_filename VARCHAR, prototype_id INTEGER, trajectory_type VARCHAR CHECK (trajectory_type IN (\"feature\", \"object\")), nmatchings INTEGER, PRIMARY KEY (prototype_filename, prototype_id, trajectory_type))')
            for p in prototypes:
                cursor.execute('INSERT INTO prototypes VALUES(?,?,?,?)', (p.getFilename(), p.getNum(), p.getTrajectoryType(), p.getNMatchings()))
        except sqlite3.OperationalError as error:
            printDBError(error)
        connection.commit()

def setPrototypeMatchingsInSqlite(filename, prototypes):
    '''updates the prototype matchings'''
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            for p in prototypes:
                if p.getNMatchings() is None:
                    nMatchings = 'NULL'
                else:
                    nMatchings = p.getNMatchings()
                cursor.execute('UPDATE prototypes SET nmatchings = {} WHERE prototype_filename = \"{}\" AND prototype_id = {} AND trajectory_type = \"{}\"'.format(nMatchings, p.getFilename(), p.getNum(), p.getTrajectoryType()))
        except sqlite3.OperationalError as error:
            printDBError(error)
        connection.commit()

def prototypeAssignmentNames(objectType):
    tableName = objectType+'s_prototypes'
    if objectType == 'feature':
        #tableName = 'features_prototypes'
        objectIdColumnName = 'trajectory_id'
    elif objectType == 'object':
        #tableName = 'objects_prototypes'
        objectIdColumnName = 'object_id'
    return tableName, objectIdColumnName
        
def savePrototypeAssignmentsToSqlite(filename, objectNumbers, objectType, labels, prototypes):
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            tableName, objectIdColumnName = prototypeAssignmentNames(objectType)
            cursor.execute('CREATE TABLE IF NOT EXISTS '+tableName+' ('+objectIdColumnName+' INTEGER, prototype_filename VARCHAR, prototype_id INTEGER, trajectory_type VARCHAR CHECK (trajectory_type IN (\"feature\", \"object\")), PRIMARY KEY('+objectIdColumnName+', prototype_filename, prototype_id, trajectory_type))')
            for objNum, label in zip(objectNumbers, labels):
                if label >=0:
                    proto = prototypes[label]
                    cursor.execute('INSERT INTO '+tableName+' VALUES(?,?,?,?)', (objNum, proto.getFilename(), proto.getNum(), proto.getTrajectoryType()))
        except sqlite3.OperationalError as error:
            printDBError(error)
        connection.commit()

def loadPrototypeAssignmentsFromSqlite(filename, objectType):
    '''Loads the assignments between prototypes
    Returns a dictionary with prototypes as keys and a list of 
    the objects/feature nums assigned to each'''
    prototypeAssignments = {}
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            cursor = connection.cursor()
            try:
                tableName, objectIdColumnName = prototypeAssignmentNames(objectType)
                cursor.execute('SELECT * FROM '+tableName)
                for row in cursor:
                    p = moving.Prototype(row[1], row[2], row[3])
                    if p in prototypeAssignments:
                        prototypeAssignments[p].append(row[0])
                    else:
                        prototypeAssignments[p] = [row[0]]
                return prototypeAssignments
            except sqlite3.OperationalError as error:
                printDBError(error)
    return prototypeAssignments

def loadPrototypesFromSqlite(filename, withTrajectories = True):
    'Loads prototype ids and matchings (if stored)'
    prototypes = []
    if Path(filename).is_file():
        parentPath = Path(filename).resolve().parent
        with sqlite3.connect(filename) as connection:
            cursor = connection.cursor()
            objects = []
            try:
                cursor.execute('SELECT * FROM prototypes')
                for row in cursor:
                    prototypes.append(moving.Prototype(row[0], row[1], row[2], row[3]))
                if withTrajectories:
                    for p in prototypes:
                        p.setMovingObject(loadTrajectoriesFromSqlite(str(parentPath/p.getFilename()), p.getTrajectoryType(), [p.getNum()])[0])
                    # loadingInformation = {} # complicated slightly optimized
                    # for p in prototypes:
                    #     dbfn = p.getFilename()
                    #     trajType = p.getTrajectoryType()
                    #     if (dbfn, trajType) in loadingInformation:
                    #         loadingInformation[(dbfn, trajType)].append(p)
                    #     else:
                    #         loadingInformation[(dbfn, trajType)] = [p]
                    # for k, v in loadingInformation.iteritems():
                    #     objects += loadTrajectoriesFromSqlite(k[0], k[1], [p.getNum() for p in v])
            except sqlite3.OperationalError as error:
                printDBError(error)
        if len(set([p.getTrajectoryType() for p in prototypes])) > 1:
            print('Different types of prototypes in database ({}).'.format(set([p.getTrajectoryType() for p in prototypes])))
    return prototypes

def savePOIsToSqlite(filename, gmm, gmmType, gmmId):
    '''Saves a Gaussian mixture model (of class sklearn.mixture.GaussianMixture)
    gmmType is a type of GaussianMixture, learnt either from beginnings or ends of trajectories'''
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        if gmmType not in ['beginning', 'end']:
            print('Unknown POI type {}. Exiting'.format(gmmType))
            import sys
            sys.exit()
        try:
            cursor.execute('CREATE TABLE IF NOT EXISTS gaussians2d (poi_id INTEGER, id INTEGER, type VARCHAR, x_center REAL, y_center REAL, covariance VARCHAR, covariance_type VARCHAR, weight, precisions_cholesky VARCHAR, PRIMARY KEY(poi_id, id))')
            for i in range(gmm.n_components):
                cursor.execute('INSERT INTO gaussians2d VALUES(?,?,?,?,?,?,?,?,?)', (gmmId, i, gmmType, gmm.means_[i][0], gmm.means_[i][1], str(gmm.covariances_[i].tolist()), gmm.covariance_type, gmm.weights_[i], str(gmm.precisions_cholesky_[i].tolist())))
            connection.commit()
        except sqlite3.OperationalError as error:
            printDBError(error)

def savePOIAssignmentsToSqlite(filename, objects):
    'save the od fields of objects'
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute('CREATE TABLE IF NOT EXISTS objects_pois (object_id INTEGER, origin_poi_id INTEGER, destination_poi_id INTEGER, PRIMARY KEY(object_id))')
            for o in objects:
                cursor.execute('INSERT INTO objects_pois VALUES(?,?,?)', (o.getNum(), o.od[0], o.od[1]))
            connection.commit()
        except sqlite3.OperationalError as error:
            printDBError(error)
    
def loadPOIsFromSqlite(filename):
    'Loads all 2D Gaussians in the database'
    from sklearn import mixture # todo if not avalaible, load data in duck-typed class with same fields
    from ast import literal_eval
    pois = []
    if Path(filename).is_file():
        with sqlite3.connect(filename) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT * from gaussians2d')
                gmmId = None
                gmm = []
                for row in cursor:
                    if gmmId is None or row[0] != gmmId:
                        if len(gmm) > 0:
                            tmp = mixture.GaussianMixture(len(gmm), covarianceType)
                            tmp.means_ = array([gaussian['mean'] for gaussian in gmm])
                            tmp.covariances_ = array([gaussian['covar'] for gaussian in gmm])
                            tmp.weights_ = array([gaussian['weight'] for gaussian in gmm])
                            tmp.gmmTypes = [gaussian['type'] for gaussian in gmm]
                            tmp.precisions_cholesky_ = array([gaussian['precisions'] for gaussian in gmm])
                            pois.append(tmp)
                        gaussian = {'type': row[2],
                                    'mean': row[3:5],
                                    'covar': array(literal_eval(row[5])),
                                    'weight': row[7],
                                    'precisions': array(literal_eval(row[8]))}
                        gmm = [gaussian]
                        covarianceType = row[6]
                        gmmId = row[0]
                    else:
                        gmm.append({'type': row[2],
                                    'mean': row[3:5],
                                    'covar': array(literal_eval(row[5])),
                                    'weight': row[7],
                                    'precisions': array(literal_eval(row[8]))})
                if len(gmm) > 0:
                    tmp = mixture.GaussianMixture(n_components=len(gmm), covariance_type=covarianceType)
                    tmp.means_ = array([gaussian['mean'] for gaussian in gmm])
                    tmp.covariances_ = array([gaussian['covar'] for gaussian in gmm])
                    tmp.weights_ = array([gaussian['weight'] for gaussian in gmm])
                    tmp.gmmTypes = [gaussian['type'] for gaussian in gmm]
                    tmp.precisions_cholesky_ = array([gaussian['precisions'] for gaussian in gmm])
                    pois.append(tmp)
            except sqlite3.OperationalError as error:
                printDBError(error)
    return pois
    
#########################
# saving and loading for scene interpretation (Mohamed Gomaa Mohamed's PhD)
#########################

def writePrototypesToSqlite(prototypes,nMatching, outputFilename):
    ''' prototype dataset is a dictionary with  keys== routes, values== prototypes Ids '''
    connection = sqlite3.connect(outputFilename)
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS prototypes (prototype_id INTEGER,routeIDstart INTEGER,routeIDend INTEGER, nMatching INTEGER, PRIMARY KEY(prototype_id))')
    
    for route in prototypes:
        if prototypes[route]!=[]:
            for i in prototypes[route]:
                cursor.execute('insert into prototypes (prototype_id, routeIDstart,routeIDend, nMatching) values (?,?,?,?)',(i,route[0],route[1],nMatching[route][i]))
                    
    connection.commit()
    connection.close()
    
def readPrototypesFromSqlite(filename):
    '''
    This function loads the prototype file in the database 
    It returns a dictionary for prototypes for each route and nMatching
    '''
    prototypes = {}
    nMatching={}

    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT * from prototypes order by prototype_id, routeIDstart,routeIDend, nMatching')
    except sqlite3.OperationalError as error:
        printDBError(error)
        return []

    for row in cursor:
        route=(row[1],row[2])
        if route not in prototypes:
            prototypes[route]=[]
        prototypes[route].append(row[0])
        nMatching[row[0]]=row[3]

    connection.close()
    return prototypes,nMatching
    
def writeLabelsToSqlite(labels, outputFilename):
    """ labels is a dictionary with  keys: routes, values: prototypes Ids
    """
    connection = sqlite3.connect(outputFilename)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS labels (object_id INTEGER,routeIDstart INTEGER,routeIDend INTEGER, prototype_id INTEGER, PRIMARY KEY(object_id))")
    
    for route in labels:
        if labels[route]!=[]:
            for i in labels[route]:
                for j in labels[route][i]:
                    cursor.execute("insert into labels (object_id, routeIDstart,routeIDend, prototype_id) values (?,?,?,?)",(j,route[0],route[1],i))
                    
    connection.commit()
    connection.close()
    
def loadLabelsFromSqlite(filename):
    labels = {}

    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT * from labels order by object_id, routeIDstart,routeIDend, prototype_id')
    except sqlite3.OperationalError as error:
        printDBError(error)
        return []

    for row in cursor:
        route=(row[1],row[2])
        p=row[3]
        if route not in labels:
            labels[route]={}
        if p not in labels[route]:
            labels[route][p]=[]
        labels[route][p].append(row[0])

    connection.close()
    return labels

def writeSpeedPrototypeToSqlite(prototypes,nmatching, outFilename):
    """ to match the format of second layer prototypes"""
    connection = sqlite3.connect(outFilename)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS speedprototypes (spdprototype_id INTEGER,prototype_id INTEGER,routeID_start INTEGER, routeID_end INTEGER, nMatching INTEGER, PRIMARY KEY(spdprototype_id))")
    
    for route in prototypes:
        if prototypes[route]!={}:
            for i in prototypes[route]:
                if prototypes[route][i]!= []:
                    for j in prototypes[route][i]:
                        cursor.execute("insert into speedprototypes (spdprototype_id,prototype_id, routeID_start, routeID_end, nMatching) values (?,?,?,?,?)",(j,i,route[0],route[1],nmatching[j]))
                    
    connection.commit()
    connection.close()
    
def loadSpeedPrototypeFromSqlite(filename):
    """
    This function loads the prototypes table in the database of name <filename>.
    """
    prototypes = {}
    nMatching={}
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT * from speedprototypes order by spdprototype_id,prototype_id, routeID_start, routeID_end, nMatching')
    except sqlite3.OperationalError as error:
        printDBError(error)
        return []

    for row in cursor:
        route=(row[2],row[3])
        if route not in prototypes:
            prototypes[route]={}
        if row[1] not in prototypes[route]:
            prototypes[route][row[1]]=[]
        prototypes[route][row[1]].append(row[0])
        nMatching[row[0]]=row[4]

    connection.close()
    return prototypes,nMatching


def writeRoutesToSqlite(Routes, outputFilename):
    """ This function writes the activity path define by start and end IDs"""
    connection = sqlite3.connect(outputFilename)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS routes (object_id INTEGER,routeIDstart INTEGER,routeIDend INTEGER, PRIMARY KEY(object_id))")
    
    for route in Routes:
        if Routes[route]!=[]:
            for i in Routes[route]:
                cursor.execute("insert into routes (object_id, routeIDstart,routeIDend) values (?,?,?)",(i,route[0],route[1]))
                    
    connection.commit()
    connection.close()
    
def loadRoutesFromSqlite(filename):
    Routes = {}

    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT * from routes order by object_id, routeIDstart,routeIDend')
    except sqlite3.OperationalError as error:
        printDBError(error)
        return []

    for row in cursor:
        route=(row[1],row[2])
        if route not in Routes:
            Routes[route]=[]
        Routes[route].append(row[0])

    connection.close()
    return Routes

def setRoutes(filename, objects):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()
    for obj in objects:
        cursor.execute('update objects set startRouteID = {} WHERE object_id = {}'.format(obj.startRouteID, obj.getNum()))
        cursor.execute('update objects set endRouteID = {} WHERE object_id = {}'.format(obj.endRouteID, obj.getNum()))        
    connection.commit()
    connection.close()

#########################
# txt files
#########################

def loadCSVs(filenames, **kwargs):
    '''Loads all the data from the filenames (eg from glob) and returns a concatenated dataframe'''
    data = read_csv(filenames[0], **kwargs)
    for f in filenames[1:]:
        data = data.append(read_csv(filenames[0], **kwargs))
    return data

def saveList(filename, l):
    f = utils.openCheck(filename, 'w')
    for x in l:
        f.write('{}\n'.format(x))
    f.close()

def loadListStrings(filename, commentCharacters = utils.commentChar):
    f = utils.openCheck(filename, 'r')
    result = utils.getLines(f, commentCharacters)
    f.close()
    return result

def getValuesFromINIFile(filename, option, delimiterChar = '=', commentCharacters = utils.commentChar):
    values = []
    for l in loadListStrings(filename, commentCharacters):
        if l.startswith(option):
            values.append(l.split(delimiterChar)[1].strip())
    return values

def addSectionHeader(propertiesFile, headerName = 'main'):
    '''Add fake section header 

    from http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788
    use read_file in Python 3.2+
    '''
    yield '[{}]\n'.format(headerName)
    for line in propertiesFile:
        yield line

def loadPemsTraffic(filename):
    '''Loads traffic data downloaded from the http://pems.dot.ca.gov clearinghouse 
    into pandas dataframe'''
    data=read_csv(filename, nrows = 3)
    headerNames = ['time', 'station', 'district', 'freeway', 'direction', 'lanetype', 'length', 'nsamples', 'pctobserved', 'totalflow', 'occupancy', 'speed'] # default for 5 min
    nLanes = int((len(data.columns)-len(headerNames))/5)
    for i in range(1, nLanes+1):
        headerNames += ['nsamples{}'.format(i), 'flow{}'.format(i), 'occupancy{}'.format(i), 'speed{}'.format(i), 'pctobserved{}'.format(i)]
    return read_csv(filename, names = headerNames)

def generatePDLaneColumn(data):
    data['LANE'] = data['LANE\\LINK\\NO'].astype(str)+'_'+data['LANE\\INDEX'].astype(str)

def convertTrajectoriesVissimToSqlite(filename):
    '''Relies on a system call to sqlite3
    sqlite3 [file.sqlite] < import_fzp.sql'''
    sqlScriptFilename = "import_fzp.sql"
    # create sql file
    out = utils.openCheck(sqlScriptFilename, "w")
    out.write(".separator \";\"\n"+
              "CREATE TABLE IF NOT EXISTS curvilinear_positions (t REAL, trajectory_id INTEGER, link_id INTEGER, lane_id INTEGER, s_coordinate REAL, y_coordinate REAL, speed REAL, PRIMARY KEY (t, trajectory_id));\n"+
              ".import "+filename+" curvilinear_positions\n"+
              "DELETE FROM curvilinear_positions WHERE trajectory_id IS NULL OR trajectory_id = \"NO\";\n")
    out.close()
    # system call
    from subprocess import run
    out = utils.openCheck("err.log", "w")
    run("sqlite3 "+utils.removeExtension(filename)+".sqlite < "+sqlScriptFilename, stderr = out)
    out.close()
    shutil.os.remove(sqlScriptFilename)

def loadObjectNumbersInLinkFromVissimFile(filename, linkIds):
    '''Finds the ids of the objects that go through any of the link in the list linkIds'''
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        queryStatement = 'SELECT DISTINCT trajectory_id FROM curvilinear_positions where link_id IN ('+','.join([str(id) for id in linkIds])+')'
        try:
            cursor.execute(queryStatement)
            return [row[0] for row in cursor]
        except sqlite3.OperationalError as error:
            printDBError(error)

def getNObjectsInLinkFromVissimFile(filename, linkIds):
    '''Returns the number of objects that traveled through the link ids'''
    with sqlite3.connect(filename) as connection:
        cursor = connection.cursor()
        queryStatement = 'SELECT link_id, COUNT(DISTINCT trajectory_id) FROM curvilinear_positions where link_id IN ('+','.join([str(id) for id in linkIds])+') GROUP BY link_id'
        try:
            cursor.execute(queryStatement)
            return {row[0]:row[1] for row in cursor}
        except sqlite3.OperationalError as error:
            printDBError(error)

def loadTrajectoriesFromVissimFile(filename, simulationStepsPerTimeUnit, objectNumbers = None, warmUpLastInstant = None, usePandas = False, nDecimals = 2, lowMemory = True):
    '''Reads data from VISSIM .fzp trajectory file
    simulationStepsPerTimeUnit is the number of simulation steps per unit of time used by VISSIM (second)
    for example, there seems to be 10 simulation steps per simulated second in VISSIM, 
    so simulationStepsPerTimeUnit should be 10, 
    so that all times correspond to the number of the simulation step (and can be stored as integers)
    
    Objects positions will be considered only after warmUpLastInstant 
    (if the object has no such position, it won't be loaded)

    Assumed to be sorted over time
    Warning: if reading from SQLite a limited number of objects, objectNumbers will be the maximum object id'''
    objects = {} # dictionary of objects index by their id

    if usePandas:
        data = read_csv(filename, delimiter=';', comment='*', header=0, skiprows = 1, low_memory = lowMemory)
        generatePDLaneColumn(data)
        data['TIME'] = data['$VEHICLE:SIMSEC']*simulationStepsPerTimeUnit
        if warmUpLastInstant is not None:
            data = data[data['TIME']>=warmUpLastInstant]
        grouped = data.loc[:,['NO','TIME']].groupby(['NO'], as_index = False)
        instants = grouped['TIME'].agg({'first': npmin, 'last': npmax})
        for row_index, row in instants.iterrows():
            objNum = int(row['NO'])
            tmp = data[data['NO'] == objNum]
            objects[objNum] = moving.MovingObject(num = objNum, timeInterval = moving.TimeInterval(row['first'], row['last']), nObjects = 1)
            # positions should be rounded to nDecimals decimals only
            objects[objNum].curvilinearPositions = moving.CurvilinearTrajectory(S = npround(tmp['POS'].tolist(), nDecimals), Y = npround(tmp['POSLAT'].tolist(), nDecimals), lanes = tmp['LANE'].tolist())
            if objectNumbers is not None and objectNumbers > 0 and len(objects) >= objectNumbers:
                return list(objects.values())
    else:
        if filename.endswith(".fzp"):
            inputfile = utils.openCheck(filename, quitting = True)
            line = utils.readline(inputfile, '*$')
            while len(line) > 0:#for line in inputfile:
                data = line.strip().split(';')
                objNum = int(data[1])
                instant = float(data[0])*simulationStepsPerTimeUnit
                s = float(data[4])
                y = float(data[5])
                lane = data[2]+'_'+data[3]
                if objNum not in objects:
                    if warmUpLastInstant is None or instant >= warmUpLastInstant:
                        if objectNumbers is None or len(objects) < objectNumbers:
                            objects[objNum] = moving.MovingObject(num = objNum, timeInterval = moving.TimeInterval(instant, instant), nObjects = 1)
                            objects[objNum].curvilinearPositions = moving.CurvilinearTrajectory()
                if (warmUpLastInstant is None or instant >= warmUpLastInstant) and objNum in objects:
                    objects[objNum].timeInterval.last = instant
                    objects[objNum].curvilinearPositions.addPositionSYL(s, y, lane)
                line = utils.readline(inputfile, '*$')
        elif filename.endswith(".sqlite"):
            with sqlite3.connect(filename) as connection:
                cursor = connection.cursor()
                queryStatement = 'SELECT t, trajectory_id, link_id, lane_id, s_coordinate, y_coordinate FROM curvilinear_positions'
                if objectNumbers is not None:
                    queryStatement += ' WHERE trajectory_id '+getObjectCriteria(objectNumbers)
                queryStatement += ' ORDER BY trajectory_id, t'
                try:
                    cursor.execute(queryStatement)
                    for row in cursor:
                        objNum = row[1]
                        instant = row[0]*simulationStepsPerTimeUnit
                        s = row[4]
                        y = row[5]
                        lane = '{}_{}'.format(row[2], row[3])
                        if objNum not in objects:
                            if warmUpLastInstant is None or instant >= warmUpLastInstant:
                                if objectNumbers is None or len(objects) < objectNumbers:
                                    objects[objNum] = moving.MovingObject(num = objNum, timeInterval = moving.TimeInterval(instant, instant), nObjects = 1)
                                    objects[objNum].curvilinearPositions = moving.CurvilinearTrajectory()
                        if (warmUpLastInstant is None or instant >= warmUpLastInstant) and objNum in objects:
                            objects[objNum].timeInterval.last = instant
                            objects[objNum].curvilinearPositions.addPositionSYL(s, y, lane)
                except sqlite3.OperationalError as error:
                    printDBError(error)
        else:
            print("File type of "+filename+" not supported (only .sqlite and .fzp files)")
        return list(objects.values())

def selectPDLanes(data, lanes = None):
    '''Selects the subset of data for the right lanes

    Lane format is a string 'x_y' where x is link index and y is lane index'''
    if lanes is not None:
        if 'LANE' not in data.columns:
            generatePDLaneColumn(data)
        indices = (data['LANE'] == lanes[0])
        for l in lanes[1:]:
            indices = indices | (data['LANE'] == l)
        return data[indices]
    else:
        return data

def countStoppedVehiclesVissim(filename, lanes = None, proportionStationaryTime = 0.7):
    '''Counts the number of vehicles stopped for a long time in a VISSIM trajectory file
    and the total number of vehicles

    Vehicles are considered finally stationary
    if more than proportionStationaryTime of their total time
    If lanes is not None, only the data for the selected lanes will be provided
    (format as string x_y where x is link index and y is lane index)'''
    if filename.endswith(".fzp"):
        columns = ['NO', '$VEHICLE:SIMSEC', 'POS']
        if lanes is not None:
            columns += ['LANE\\LINK\\NO', 'LANE\\INDEX']
        data = read_csv(filename, delimiter=';', comment='*', header=0, skiprows = 1, usecols = columns, low_memory = lowMemory)
        data = selectPDLanes(data, lanes)
        data.sort(['$VEHICLE:SIMSEC'], inplace = True)

        nStationary = 0
        nVehicles = 0
        for name, group in data.groupby(['NO'], sort = False):
            nVehicles += 1
            positions = array(group['POS'])
            diff = positions[1:]-positions[:-1]
            if npsum(diff == 0.) >= proportionStationaryTime*(len(positions)-1):
                nStationary += 1
    elif filename.endswith(".sqlite"):
        # select trajectory_id, t, s_coordinate, speed from curvilinear_positions where trajectory_id between 1860 and 1870 and speed < 0.1
        # pb of the meaning of proportionStationaryTime in arterial network? Why proportion of existence time?
        pass
    else:
        print("File type of "+filename+" not supported (only .sqlite and .fzp files)")

    return nStationary, nVehicles

def countCollisionsVissim(filename, lanes = None, collisionTimeDifference = 0.2, lowMemory = True):
    '''Counts the number of collisions per lane in a VISSIM trajectory file

    To distinguish between cars passing and collision, 
    one checks when the sign of the position difference inverts
    (if the time are closer than collisionTimeDifference)
    If lanes is not None, only the data for the selected lanes will be provided
    (format as string x_y where x is link index and y is lane index)'''
    data = read_csv(filename, delimiter=';', comment='*', header=0, skiprows = 1, usecols = ['LANE\\LINK\\NO', 'LANE\\INDEX', '$VEHICLE:SIMSEC', 'NO', 'POS'], low_memory = lowMemory)
    data = selectPDLanes(data, lanes)
    data = data.convert_objects(convert_numeric=True)

    merged = merge(data, data, how='inner', left_on=['LANE\\LINK\\NO', 'LANE\\INDEX', '$VEHICLE:SIMSEC'], right_on=['LANE\\LINK\\NO', 'LANE\\INDEX', '$VEHICLE:SIMSEC'], sort = False)
    merged = merged[merged['NO_x']>merged['NO_y']]

    nCollisions = 0
    for name, group in merged.groupby(['LANE\\LINK\\NO', 'LANE\\INDEX', 'NO_x', 'NO_y']):
        diff = group['POS_x']-group['POS_y']
        # diff = group['POS_x']-group['POS_y'] # to check the impact of convert_objects and the possibility of using type conversion in read_csv or function to convert strings if any
        if len(diff) >= 2 and npmin(diff) < 0 and npmax(diff) > 0:
            xidx = diff[diff < 0].argmax()
            yidx = diff[diff > 0].argmin()
            if abs(group.loc[xidx, '$VEHICLE:SIMSEC'] - group.loc[yidx, '$VEHICLE:SIMSEC']) <= collisionTimeDifference:
                nCollisions += 1

    # select TD1.link_id, TD1.lane_id from temp.diff_positions as TD1, temp.diff_positions as TD2 where TD1.link_id = TD2.link_id and TD1.lane_id = TD2.lane_id and TD1.id1 = TD2.id1 and TD1.id2 = TD2.id2 and TD1.t = TD2.t+0.1 and TD1.diff*TD2.diff < 0; # besoin de faire un group by??
    # create temp table diff_positions as select CP1.t as t, CP1.link_id as link_id, CP1.lane_id as lane_id, CP1.trajectory_id as id1, CP2.trajectory_id as id2, CP1.s_coordinate - CP2.s_coordinate as diff from curvilinear_positions CP1, curvilinear_positions CP2 where CP1.link_id = CP2.link_id and CP1.lane_id = CP2.lane_id and CP1.t = CP2.t and CP1.trajectory_id > CP2.trajectory_id;
    # SQL select link_id, lane_id, id1, id2, min(diff), max(diff) from (select CP1.t as t, CP1.link_id as link_id, CP1.lane_id as lane_id, CP1.trajectory_id as id1, CP2.trajectory_id as id2, CP1.s_coordinate - CP2.s_coordinate as diff from curvilinear_positions CP1, curvilinear_positions CP2 where CP1.link_id = CP2.link_id and CP1.lane_id = CP2.lane_id and CP1.t = CP2.t and CP1.trajectory_id > CP2.trajectory_id) group by link_id, lane_id, id1, id2 having min(diff)*max(diff) < 0
    return nCollisions
    
def loadTrajectoriesFromNgsimFile(filename, nObjects = -1, sequenceNum = -1):
    '''Reads data from the trajectory data provided by NGSIM project 
    and returns the list of Feature objects'''
    objects = []

    inputfile = utils.openCheck(filename, quitting = True)

    def createObject(numbers):
        firstFrameNum = int(numbers[1])
        # do the geometry and usertype

        firstFrameNum = int(numbers[1])
        lastFrameNum = firstFrameNum+int(numbers[2])-1
        #time = moving.TimeInterval(firstFrameNum, firstFrameNum+int(numbers[2])-1)
        obj = moving.MovingObject(num = int(numbers[0]), 
                                  timeInterval = moving.TimeInterval(firstFrameNum, lastFrameNum), 
                                  positions = moving.Trajectory([[float(numbers[6])],[float(numbers[7])]]), 
                                  userType = int(numbers[10]), nObjects = 1)
        obj.userType = int(numbers[10])
        obj.laneNums = [int(numbers[13])]
        obj.precedingVehicles = [int(numbers[14])] # lead vehicle (before)
        obj.followingVehicles = [int(numbers[15])] # following vehicle (after)
        obj.spaceHeadways = [float(numbers[16])] # feet
        obj.timeHeadways = [float(numbers[17])] # seconds
        obj.curvilinearPositions = moving.CurvilinearTrajectory([float(numbers[5])],[float(numbers[4])], obj.laneNums) # X is the longitudinal coordinate
        obj.speeds = [float(numbers[11])]
        obj.size = [float(numbers[8]), float(numbers[9])] # 8 lengh, 9 width # TODO: temporary, should use a geometry object
        return obj

    numbers = utils.readline(inputfile).strip().split()
    if (len(numbers) > 0):
        obj = createObject(numbers)

    for line in inputfile:
        numbers = line.strip().split()
        if obj.getNum() != int(numbers[0]):
            # check and adapt the length to deal with issues in NGSIM data
            if (obj.length() != obj.positions.length()):
                print('length pb with object {} ({},{})'.format(obj.getNum(),obj.length(),obj.positions.length()))
                obj.last = obj.getFirstInstant()+obj.positions.length()-1
                #obj.velocities = utils.computeVelocities(f.positions) # compare norm to speeds ?
            objects.append(obj)
            if (nObjects>0) and (len(objects)>=nObjects):
                break
            obj = createObject(numbers)
        else:
            obj.laneNums.append(int(numbers[13]))
            obj.positions.addPositionXY(float(numbers[6]), float(numbers[7]))
            obj.curvilinearPositions.addPositionSYL(float(numbers[5]), float(numbers[4]), obj.laneNums[-1])
            obj.speeds.append(float(numbers[11]))
            obj.precedingVehicles.append(int(numbers[14]))
            obj.followingVehicles.append(int(numbers[15]))
            obj.spaceHeadways.append(float(numbers[16]))
            obj.timeHeadways.append(float(numbers[17]))

            if (obj.size[0] != float(numbers[8])):
                print('changed length obj {}'.format(obj.getNum()))
            if (obj.size[1] != float(numbers[9])):
                print('changed width obj {}'.format(obj.getNum()))
    
    inputfile.close()
    return objects

# from https://github.com/kuixu/kitti_object_vis
def inverse_rigid_trans(Tr):
    """ Inverse a rigid body transform matrix (3x4 as [R|t])
        [R'|-R't; 0|1]
    """
    inv_Tr = zeros_like(Tr)  # 3x4
    inv_Tr[0:3, 0:3] = transpose(Tr[0:3, 0:3])
    inv_Tr[0:3, 3] = dot(-transpose(Tr[0:3, 0:3]), Tr[0:3, 3])
    return inv_Tr

def loadKITTICalibration(filename):
    '''Loads KITTI calibration data'''
    calib = {}
    with open(filename, 'r') as f:
        for l in f:
            l = l.rstrip()
            if len(l) == 0:
                continue
            key, value = l.split(' ', 1)
            if ":" in key:
                key = key[:-1]
            try:
                calib[key] = array([float(x) for x in value.split()])
            except ValueError as e:
                print(e)
                continue
    #calib['Tr_velo_to_cam'] = calib['Tr_velo_cam']
    #calib['Tr_imu_to_velo'] = calib['Tr_imu_velo']
    calib['Tr_cam_velo'] = inverse_rigid_trans(reshape(calib['Tr_velo_cam'], (3, 4)))
    calib['Tr_velo_imu'] = inverse_rigid_trans(reshape(calib['Tr_imu_velo'], (3, 4)))
    
    P = reshape(calib['P2'], (3, 4))
    calib['c_u'] = P[0, 2]
    calib['c_v'] = P[1, 2]
    calib['f_u'] = P[0, 0]
    calib['f_v'] = P[1, 1]
    calib['b_x'] = P[0, 3] / (-calib['f_u'])  # relative
    calib['b_y'] = P[1, 3] / (-calib['f_v'])
    return calib

# from https://github.com/utiasSTARS/pykitti/blob/master/pykitti/utils.py


def loadTrajectoriesFromKITTI(filename, kittiCalibration = None, oxts = None, resultFile = False, halfWidth = None):
    '''Reads data from KITTI ground truth or output from an object detection and tracking method

    kittiCalibration is obtained from loading training/testing calibration file for each sequence 00XX.txt
    oxts is obtained using utils.load_oxts_packets_and_poses(['./training/oxts/0001.txt']) from pykitti
    Ref: https://github.com/pratikac/kitti/blob/master/readme.tracking.txt

    non-null halfWidth indicates positions are smoothed using moving window avefraging'''
    from pykitti.utils import roty, rotz
    from trafficintelligence.cvutils import cartesian2Homogeneous

    if kittiCalibration is not None:
        invR0 = linalg.inv(reshape(kittiCalibration['R_rect'], (3, 3)))
        transCam2Velo = transpose(kittiCalibration['Tr_cam_velo'])
    
    header = ['frame', # 0, 1, ..., n
              'trackingid', # -1, 0 , 1, ..., k
              'usertype',  # 'Car', 'Pedestrian', ...
              'truncation', # truncated pixel ratio [0..1]
              'occlusion', # 0=visible, 1=partly occluded, 2=fully occluded, 3=unknown
              'alpha', # object observation angle [-pi..pi]
              # extract 2d bounding box in 0-based coordinates
              'xmin', # left
              'ymin', # top
              'xmax', # right
              'ymax', # bottom
              # extract 3d bounding box information
              'h', # box height
              'w', # box width
              'l', # box length (in meters)
              'x', 'y', 'z', # location (x,y,z) in camera coord
              'ry'] # yaw angle (around Y-axis in camera coordinates) [-pi..pi]
    if resultFile:
        header.append('score')
    data = read_csv(filename, delimiter = ' ', names = header)
    data = data[data.trackingid > -1]

    objects = []
    featureNum = 0
    for objNum in data.trackingid.unique():
        tmp = data[data.trackingid == objNum].sort_values('frame')
        t = moving.Trajectory()#([[float(numbers[6])],[float(numbers[7])]])
        interval = moving.TimeInterval(int(tmp.frame.min()), int(tmp.frame.max()))
        userType = tmp.iloc[0].usertype
        if len(tmp) != interval.length(): #interpolate
            print(objNum, len(tmp), interval.length())
            instants = set(interval).difference(tmp.frame)
            missing = concat([tmp[['frame']+header[10:]], DataFrame([[t]+[nan]*(len(header)-10) for t in instants], columns = ['frame']+header[10:])], ignore_index=True).sort_values('frame')
            tmp = missing.interpolate()
        featureTrajectories = [moving.Trajectory() for i in range(4)]
        for i, r in tmp.iterrows():
            if kittiCalibration is not None:
                _, Tr_imu_to_world = oxts[r.frame]
                # transfer_matrix = {'P2': reshape(kittiCalibration['P2'], (3, 4)),
                #                    'R_rect': kittiCalibration['R_rect'],
                #                    'Tr_cam_to_velo': kittiCalibration['Tr_cam_velo'],
                #                    # transfer_matrix['Tr_velo_to_cam'] = kittiCalibration['Tr_velo_to_cam']
                #                    'Tr_velo_to_imu': kittiCalibration['Tr_velo_imu'],
                #                    # transfer_matrix['Tr_imu_to_velo'] = kittiCalibration['Tr_imu_to_velo']
                #                    'Tr_imu_to_world': Tr_imu2w}
                # 3D object
                # compute rotational matrix around yaw axis
                R = roty(r.ry)

                # 3d bounding box corners
                x_corners = [r.l / 2, r.l / 2, -r.l / 2, -r.l / 2, r.l / 2, r.l / 2, -r.l / 2, -r.l / 2]
                y_corners = [0, 0, 0, 0, -r.h, -r.h, -r.h, -r.h]
                z_corners = [r.w / 2, -r.w / 2, -r.w / 2, r.w / 2, r.w / 2, -r.w / 2, -r.w / 2, r.w / 2]
                # rotate and translate 3d bounding box
                corners3d = dot(R, vstack([x_corners, y_corners, z_corners]))
                corners3d[0, :] = corners3d[0, :] + r.x#obj.t[0]
                corners3d[1, :] = corners3d[1, :] + r.y#obj.t[1]
                corners3d[2, :] = corners3d[2, :] + r.z#obj.t[2]
                # corners3d = transpose(corners3d)
                # box3d_pts_3d_velo = calib.project_rect_to_velo(box3d_pts_3d)
                refCorners = transpose(dot(invR0, corners3d)) # 8x3 avoid double transpose np.transpose(pts_3d_rect))) in pts_3d_ref = self.project_rect_to_ref(pts_3d_rect)
                homRefCorners = cartesian2Homogeneous(refCorners)
                veloCorners = dot(homRefCorners, transCam2Velo)
                # self.project_ref_to_velo(pts_3d_ref)
                # boxes3d_world_single, flag_imu, flag_world = calib_function.project_velo_to_world(bboxes_3d_velo=boxes3d_velo_single, Tr_matrix=transfer_matrix)
                #Tr_velo_to_imu = transfer_matrix['Tr_velo_to_imu']
                #Tr_imu_to_world = transfer_matrix['Tr_imu_to_world']
                homVeloCorners = cartesian2Homogeneous(veloCorners)
                imuCorners = dot(kittiCalibration['Tr_velo_imu'], homVeloCorners.T).T # 8x3

                homImuCorners = cartesian2Homogeneous(imuCorners)
                worldCorners = dot(Tr_imu_to_world, homImuCorners.T).T # 8x3
            else: #LUMPI format
                '''
                ^ z   
                |
                |
                |
                . - - - - - - - > x
                /
                /
                /
                v y
                
                '''
                R = rotz(r.ry)
                # # 3d bounding box corners x,y,z as center of the 3d bbox
                # #            0     1      2    3      4     5    6     7
                x_corners = [r.l/2,  r.l/2, -r.l/2, -r.l/2,  r.l/2,  r.l/2, -r.l/2, -r.l/2]
                y_corners = [r.w/2, -r.w/2, -r.w/2,  r.w/2,  r.w/2, -r.w/2, -r.w/2,  r.w/2]
                z_corners = [r.h/2,  r.h/2,  r.h/2,  r.h/2, -r.h/2, -r.h/2, -r.h/2, -r.h/2]
                corners3d = dot(R, vstack([x_corners, y_corners, z_corners]))
                # add the center
                corners3d[0, :] = corners3d[0, :] + r.x
                corners3d[1, :] = corners3d[1, :] + r.y
                corners3d[2, :] = corners3d[2, :] + r.z
                worldCorners = transpose(corners3d)
            # take first 4 lines of corners, x,y,_ # x0, y0, _ = boxes3d[0]
            xCoords = worldCorners[:4,0]
            yCoords = worldCorners[:4,1]
            t.addPositionXY(xCoords.mean(), yCoords.mean())
            for j in range(4):
                featureTrajectories[j].addPositionXY(xCoords[j], yCoords[j])
            # check https://docs.opencv.org/3.4/d9/d0c/group__calib3d.html#ga1019495a2c8d1743ed5cc23fa0daff8c
        if interval.length()>1:
            if halfWidth is not None:
                for i in range(4):
                    featureTrajectories[i] = featureTrajectories[i].filterMovingWindow(halfWidth)
            objects.append(moving.MovingObject(num = objNum, timeInterval = interval, positions = t, velocities = t.differentiate(True), userType = userType, features = [moving.MovingObject(num = featureNum+i, timeInterval = copy(interval), positions = featureTrajectories[i], velocities = featureTrajectories[i].differentiate(True)) for i in range(4)]))
            featureNum += 4
    
    # ego vehicle
    if oxts is not None:
        t = moving.Trajectory()
        featureTrajectories = [moving.Trajectory() for i in range(4)]
        interval = moving.TimeInterval(0, len(oxts)-1)#int(data.frame.min()), int(data.frame.max()))
        R = roty(pi/2)
        #for frame in interval:
        for _, Tr_imu_to_world in oxts:
            l = 4.5 # m
            w = 1.8 # m
            h = 0.
            # 3d bounding box corners
            x_corners = [l / 2, l / 2, -l / 2, -l / 2, l / 2, l / 2, -l / 2, -l / 2]
            y_corners = [0, 0, 0, 0, -h, -h, -h, -h]
            z_corners = [w / 2, -w / 2, -w / 2, w / 2, w / 2, -w / 2, -w / 2, w / 2]
            # rotate and translate 3d bounding box
            corners3d = dot(R, vstack([x_corners, y_corners, z_corners]))
            corners3d[0, :] = corners3d[0, :]
            corners3d[1, :] = corners3d[1, :]
            corners3d[2, :] = corners3d[2, :]
            refCorners = transpose(dot(invR0, corners3d)) # 8x3 avoid double transpose np.transpose(pts_3d_rect))) in pts_3d_ref = self.project_rect_to_ref(pts_3d_rect)
            homRefCorners = cartesian2Homogeneous(refCorners)
            veloCorners = dot(homRefCorners, transCam2Velo)
            homVeloCorners = cartesian2Homogeneous(veloCorners)
            imuCorners = dot(kittiCalibration['Tr_velo_imu'], homVeloCorners.T).T # 8x3

            homImuCorners = cartesian2Homogeneous(imuCorners)
            worldCorners = dot(Tr_imu_to_world, homImuCorners.T).T # 8x3
                
            # take first 4 lines of corners, x,y,_ # x0, y0, _ = boxes3d[0]
            xCoords = worldCorners[:4,0]
            yCoords = worldCorners[:4,1]
            t.addPositionXY(xCoords.mean(), yCoords.mean())
            for i in range(4):
                featureTrajectories[i].addPositionXY(xCoords[i], yCoords[i])                
                # check https://docs.opencv.org/3.4/d9/d0c/group__calib3d.html#ga1019495a2c8d1743ed5cc23fa0daff8c        
        if halfWidth is not None:
            for i in range(4):
                featureTrajectories[i] = featureTrajectories[i].filterMovingWindow(halfWidth)
        objects.append(moving.MovingObject(num = max([o.getNum() for o in objects])+1, timeInterval = interval, positions = t, velocities = t.differentiate(True), userType = 'Car', features = [moving.MovingObject(num = featureNum+i, timeInterval = copy(interval), positions = featureTrajectories[i], velocities = featureTrajectories[i].differentiate(True)) for i in range(4)]))

    return objects

def loadTrajectoriesFromKITTI2D(filename, kittiCalibration = None, oxts = None, halfWidth = None):
    '''Loads x,y coordinate series
    e.g. obtained by projecting from image to ground world plane using a homography
    Format: frame_id,track_id,usertype,x,y,score 

    if oxts is not None, it is obtained using utils.load_oxts_packets_and_poses(['./training/oxts/0001.txt']) from pykitti
    to generate the movement of the ego vehicle

    non-null halfWidth indicates positions are smoothed using moving window avefraging'''

    header = ['frame','trackingid','usertype','x','y','score']
    data = read_csv(filename, delimiter=' ', names = header)
    objects = []
    for objNum in data.trackingid.unique():
        tmp = data[data.trackingid == objNum].sort_values('frame')
        interval = moving.TimeInterval(int(tmp.frame.min()), int(tmp.frame.max()))
        userType = tmp.iloc[0].usertype
        if len(tmp) != interval.length(): #interpolate
            print(objNum, len(tmp), interval.length())
            instants = set(interval).difference(tmp.frame)
            missing = concat([tmp[['frame', 'x', 'y']], DataFrame([[t, nan, nan] for t in instants], columns = ['frame', 'x', 'y'])], ignore_index=True).sort_values('frame')
            tmp = missing.interpolate()
            #print(tmp.info())
        if interval.length()>1:
            t = moving.Trajectory([tmp.x.to_list(), tmp.y.to_list()])
            if halfWidth is not None:
                t = t.filterMovingWindow(halfWidth)
            objects.append(moving.MovingObject(num = objNum, timeInterval = interval, positions = t, velocities = t.differentiate(True), userType = userType))
        
    # ego vehicle
    if kittiCalibration is not None and oxts is not None:
        invR0 = linalg.inv(reshape(kittiCalibration['R_rect'], (3, 3)))
        transCam2Velo = transpose(kittiCalibration['Tr_cam_velo'])
        t = moving.Trajectory()
        featureTrajectories = [moving.Trajectory() for i in range(4)]
        interval = moving.TimeInterval(0, len(oxts)-1)#int(data.frame.min()), int(data.frame.max()))
        #from pykitti.utils import roty
        from trafficintelligence.cvutils import cartesian2Homogeneous
        for _, Tr_imu_to_world in oxts:
            #R = roty(pi/2)
            #corners3d = dot(R, [0,0,0])
            #print(corners3d)
            #refCorners = transpose(dot(invR0, corners3d))
            #print(invR0, refCorners) # 000
            #homRefCorners = cartesian2Homogeneous(array([refCorners]))
            veloCorners = transCam2Velo[3, :] # dot(homRefCorners, transCam2Velo)
            #print(transCam2Velo, veloCorners) # last column of transCam2Velo
            homVeloCorners = cartesian2Homogeneous(veloCorners)
            imuCorners = dot(kittiCalibration['Tr_velo_imu'], homVeloCorners.T).T # 8x3
            #print(kittiCalibration['Tr_velo_imu'], imuCorners)
            homImuCorners = cartesian2Homogeneous(imuCorners)
            worldCorners = dot(Tr_imu_to_world, homImuCorners.T).T # 8x3
            #print(Tr_imu_to_world, worldCorners)
            xCoords = worldCorners[:4,0]
            yCoords = worldCorners[:4,1]
            t.addPositionXY(xCoords.mean(), yCoords.mean())
        if halfWidth is not None:
            t = t.filterMovingWindow(halfWidth)
        objects.append(moving.MovingObject(num = max([o.getNum() for o in objects])+1, timeInterval = interval, positions = t, velocities = t.differentiate(True), userType = 'Car'))
    return objects
    
def convertNgsimFile(inputfile, outputfile, append = False, nObjects = -1, sequenceNum = 0):
    '''Reads data from the trajectory data provided by NGSIM project
    and converts to our current format.'''
    if append:
        out = utils.openCheck(outputfile,'a')
    else:
        out = utils.openCheck(outputfile,'w')
    nObjectsPerType = [0,0,0]

    features = loadNgsimFile(inputfile, sequenceNum)
    for f in features:
        nObjectsPerType[f.userType-1] += 1
        f.write(out)

    print(nObjectsPerType)
        
    out.close()

def loadPinholeCameraModel(filename, tanalystFormat = True):
    '''Loads the data from a file containing the camera parameters
    (pinhole camera model, http://docs.opencv.org/2.4/modules/calib3d/doc/camera_calibration_and_3d_reconstruction.html)
    and returns a dictionary'''
    if tanalystFormat:
        f = utils.openCheck(filename, quitting = True)
        content = utils.getLines(f)
        cameraData = {}
        for l in content:
            tmp = l.split(':')
            cameraData[tmp[0]] = float(tmp[1].strip().replace(',','.'))
        return cameraData
    else:
        print('Unknown camera model (not tanalyst format')
        return None

def savePositionsToCsv(f, obj):
    timeInterval = obj.getTimeInterval()
    positions = obj.getPositions()
    curvilinearPositions = obj.getCurvilinearPositions()
    for i in range(int(obj.length())):
        p1 = positions[i]
        s = '{},{},{},{}'.format(obj.num,timeInterval[i],p1.x,p1.y)
        if curvilinearPositions is not None:
            p2 = curvilinearPositions[i]
            s += ',{},{}'.format(p2[0],p2[1])
        f.write(s+'\n')

def saveTrajectoriesToCsv(filename, objects):
    f = utils.openCheck(filename, 'w')
    for i,obj in enumerate(objects):
        savePositionsToCsv(f, obj)
    f.close()


#########################
# Utils to read .ini type text files for configuration, meta data...
#########################

class ClassifierParameters(VideoFilenameAddable):
    'Class for the parameters of object classifiers'
    def loadConfigFile(self, filename):
        from configparser import ConfigParser

        config = ConfigParser()
        config.read_file(addSectionHeader(utils.openCheck(filename)))

        parentPath = Path(filename).parent
        self.sectionHeader = config.sections()[0]

        self.pedBikeCarSVMFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'pbv-svm-filename'))
        self.bikeCarSVMFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'bv-svm-filename'))
        self.dlFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'dl-filename'))
        self.percentIncreaseCrop = config.getfloat(self.sectionHeader, 'percent-increase-crop')
        self.minNPixels = config.getint(self.sectionHeader, 'min-npixels-crop')
        x = config.getint(self.sectionHeader, 'hog-rescale-size')
        self.hogRescaleSize = (x, x)
        self.hogNOrientations = config.getint(self.sectionHeader, 'hog-norientations')
        x = config.getint(self.sectionHeader, 'hog-npixels-cell')
        self.hogNPixelsPerCell = (x, x)
        x = config.getint(self.sectionHeader, 'hog-ncells-block')
        self.hogNCellsPerBlock = (x, x)
        self.hogBlockNorm = config.get(self.sectionHeader, 'hog-block-norm')
        self.confidence = config.getfloat(self.sectionHeader, 'confidence')
        
        self.speedAggregationMethod = config.get(self.sectionHeader, 'speed-aggregation-method')
        self.nFramesIgnoreAtEnds = config.getint(self.sectionHeader, 'nframes-ignore-at-ends')
        self.speedAggregationCentile = config.getint(self.sectionHeader, 'speed-aggregation-centile')
        self.minSpeedEquiprobable = config.getfloat(self.sectionHeader, 'min-speed-equiprobable')
        self.maxPercentUnknown = config.getfloat(self.sectionHeader, 'max-prop-unknown-appearance')
        self.maxPedestrianSpeed = config.getfloat(self.sectionHeader, 'max-ped-speed')
        self.maxCyclistSpeed = config.getfloat(self.sectionHeader, 'max-cyc-speed')
        self.meanPedestrianSpeed = config.getfloat(self.sectionHeader, 'mean-ped-speed')
        self.stdPedestrianSpeed = config.getfloat(self.sectionHeader, 'std-ped-speed')
        self.locationCyclistSpeed = config.getfloat(self.sectionHeader, 'cyc-speed-loc')
        self.scaleCyclistSpeed = config.getfloat(self.sectionHeader, 'cyc-speed-scale')
        self.meanVehicleSpeed = config.getfloat(self.sectionHeader, 'mean-veh-speed')
        self.stdVehicleSpeed = config.getfloat(self.sectionHeader, 'std-veh-speed')

    def __init__(self, filename = None):
        self.configFilename = filename
        if filename is not None and Path(filename).is_file():
            self.loadConfigFile(filename)
        else:
            print('Configuration filename {} could not be loaded.'.format(filename))

    def convertToFrames(self, frameRate, speedRatio = 3.6):
        '''Converts parameters with a relationship to time in 'native' frame time
        speedRatio is the conversion from the speed unit in the config file
        to the distance per second

        ie param(config file) = speedRatio x fps x param(used in program)
        eg km/h = 3.6 (m/s to km/h) x frame/s x m/frame'''
        denominator = frameRate*speedRatio
        #denominator2 = denominator**2
        self.minSpeedEquiprobable = self.minSpeedEquiprobable/denominator
        self.maxPedestrianSpeed = self.maxPedestrianSpeed/denominator
        self.maxCyclistSpeed = self.maxCyclistSpeed/denominator
        self.meanPedestrianSpeed = self.meanPedestrianSpeed/denominator
        self.stdPedestrianSpeed = self.stdPedestrianSpeed/denominator
        self.meanVehicleSpeed = self.meanVehicleSpeed/denominator
        self.stdVehicleSpeed = self.stdVehicleSpeed/denominator
        # special case for the lognormal distribution
        self.locationCyclistSpeed = self.locationCyclistSpeed-log(denominator)
        #self.scaleCyclistSpeed = self.scaleCyclistSpeed # no modification of scale

        
class ProcessParameters(VideoFilenameAddable):
    '''Class for all parameters controlling data processing: input,
    method parameters, etc. for tracking and safety

    Note: framerate is already taken into account'''

    def loadConfigFile(self, filename):
        from configparser import ConfigParser

        config = ConfigParser({ 'acceleration-bound' : '3', 
                                'min-velocity-cosine' : '0.8', 
                                'ndisplacements' : '3', 
                                'max-nfeatures' : '1000',
                                'feature-quality' : '0.0812219538558',
                                'min-feature-distanceklt' : '3.54964337411',
                                'block-size' : '7',
                                'use-harris-detector' : '0',
                                'k' : '0.04',
                                'window-size' : '6',
                                'pyramid-level' : '5',
                                'min-tracking-error' : '0.183328975142',
                                'max-number-iterations' : '20',
                                'feature-flag' : '0',
                                'min-feature-eig-threshold' : '1e-4',
                                'min-feature-time' : '15',
                                'min-feature-displacement' : '0.05',
                                'tracker-reload-time' : '10'}, strict=False)
        config.read_file(addSectionHeader(utils.openCheck(filename)))
        parentPath = Path(filename).parent
        self.sectionHeader = config.sections()[0]
        # Tracking/display parameters
        self.videoFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'video-filename'))
        self.databaseFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'database-filename'))
        self.homographyFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'homography-filename'))
        if Path(self.homographyFilename).is_file():
            self.homography = loadtxt(self.homographyFilename)
        else:
            self.homography = None
        self.intrinsicCameraFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'intrinsic-camera-filename'))
        if Path(self.intrinsicCameraFilename).is_file():
            self.intrinsicCameraMatrix = loadtxt(self.intrinsicCameraFilename)
        else:
            self.intrinsicCameraMatrix = None
        distortionCoefficients = getValuesFromINIFile(filename, 'distortion-coefficients', '=')        
        self.distortionCoefficients = [float(x) for x in distortionCoefficients]
        self.undistortedImageMultiplication  = config.getfloat(self.sectionHeader, 'undistorted-size-multiplication')
        self.maskFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'mask-filename'))
      
        self.undistort = config.getboolean(self.sectionHeader, 'undistort')
        self.firstFrameNum = config.getint(self.sectionHeader, 'frame1')
        self.videoFrameRate = config.getfloat(self.sectionHeader, 'video-fps')
        
        self.classifierFilename = utils.getRelativeFilename(parentPath, config.get(self.sectionHeader, 'classifier-filename'))
        
        # Safety parameters
        self.maxPredictedSpeed = config.getfloat(self.sectionHeader, 'max-predicted-speed')/3.6/self.videoFrameRate
        self.predictionTimeHorizon = config.getfloat(self.sectionHeader, 'prediction-time-horizon')*self.videoFrameRate
        self.collisionDistance = config.getfloat(self.sectionHeader, 'collision-distance')
        self.crossingZones = config.getboolean(self.sectionHeader, 'crossing-zones')
        self.predictionMethod = config.get(self.sectionHeader, 'prediction-method')
        self.nPredictedTrajectories = config.getint(self.sectionHeader, 'npredicted-trajectories')
        self.maxNormalAcceleration = config.getfloat(self.sectionHeader, 'max-normal-acceleration')/self.videoFrameRate**2
        self.maxNormalSteering = config.getfloat(self.sectionHeader, 'max-normal-steering')/self.videoFrameRate
        self.minExtremeAcceleration = config.getfloat(self.sectionHeader, 'min-extreme-acceleration')/self.videoFrameRate**2
        self.maxExtremeAcceleration = config.getfloat(self.sectionHeader, 'max-extreme-acceleration')/self.videoFrameRate**2
        self.maxExtremeSteering = config.getfloat(self.sectionHeader, 'max-extreme-steering')/self.videoFrameRate
        self.useFeaturesForPrediction = config.getboolean(self.sectionHeader, 'use-features-prediction')
        self.constantSpeedPrototypePrediction = config.getboolean(self.sectionHeader, 'constant-speed')
        self.maxLcssDistance = config.getfloat(self.sectionHeader, 'max-lcss-distance')
        self.lcssMetric = config.get(self.sectionHeader, 'lcss-metric')
        self.minLcssSimilarity = config.getfloat(self.sectionHeader, 'min-lcss-similarity')
        
        # Tracking parameters
        self.accelerationBound = config.getint(self.sectionHeader, 'acceleration-bound')
        self.minVelocityCosine = config.getfloat(self.sectionHeader, 'min-velocity-cosine')
        self.ndisplacements = config.getint(self.sectionHeader, 'ndisplacements')
        self.maxNFeatures = config.getint(self.sectionHeader, 'max-nfeatures')
        self.minFeatureDistanceKLT = config.getfloat(self.sectionHeader, 'min-feature-distanceklt')
        self.featureQuality = config.getfloat(self.sectionHeader, 'feature-quality')
        self.blockSize = config.getint(self.sectionHeader, 'block-size')
        self.useHarrisDetector = config.getboolean(self.sectionHeader, 'use-harris-detector')
        self.k = config.getfloat(self.sectionHeader, 'k')
        self.winSize = config.getint(self.sectionHeader, 'window-size')
        self.pyramidLevel = config.getint(self.sectionHeader, 'pyramid-level')
        self.maxNumberTrackingIterations = config.getint(self.sectionHeader, 'max-number-iterations')
        self.minTrackingError = config.getfloat(self.sectionHeader, 'min-tracking-error')
        self.featureFlags = config.getboolean(self.sectionHeader, 'feature-flag')
        self.minFeatureEigThreshold = config.getfloat(self.sectionHeader, 'min-feature-eig-threshold')
        self.minFeatureTime = config.getint(self.sectionHeader, 'min-feature-time')
        self.minFeatureDisplacement = config.getfloat(self.sectionHeader, 'min-feature-displacement')
        self.smoothingHalfWidth = config.getint(self.sectionHeader, 'smoothing-halfwidth')
        #self.updateTimer = config.getint(self.sectionHeader, 'tracker-reload-time')
        

    def __init__(self, filename = None):
        self.configFilename = filename
        if filename is not None and Path(filename).is_file():
            self.loadConfigFile(filename)
        else:
            print('Configuration filename {} could not be loaded.'.format(filename))

def processVideoArguments(args):
    '''Loads information from configuration file
    then checks what was passed on the command line
    for override (eg video filename and database filename'''
    if args.configFilename is not None: # consider there is a configuration file
        #parentPath = Path(args.configFilename).parent
        params = ProcessParameters(args.configFilename)
        videoFilename = params.videoFilename
        databaseFilename = params.databaseFilename
        if params.homography is not None:
            homography = params.homography
            invHomography = linalg.inv(params.homography)
        else:
            homography = None
            invHomography = None
        intrinsicCameraMatrix = params.intrinsicCameraMatrix
        distortionCoefficients = array(params.distortionCoefficients)
        undistortedImageMultiplication = params.undistortedImageMultiplication
        undistort = params.undistort
        firstFrameNum = params.firstFrameNum
    else:
        params = None
        homography = None
        invHomography = None
        undistort = False
        intrinsicCameraMatrix = None
        distortionCoefficients = []
        undistortedImageMultiplication = None
        undistort = False
        firstFrameNum = 0

    # override video and database filenames if present on command line (path is relative to command line location)
    if args.videoFilename is not None:
        videoFilename = args.videoFilename
    else:
        videoFilename = params.videoFilename
    if args.databaseFilename is not None:
        databaseFilename = args.databaseFilename
    else:
        databaseFilename = params.databaseFilename

    return params, videoFilename, databaseFilename, homography, invHomography, intrinsicCameraMatrix, distortionCoefficients, undistortedImageMultiplication, undistort, firstFrameNum
    
# deprecated
class SceneParameters(object):
    def __init__(self, config, sectionName):
        from configparser import NoOptionError
        from ast import literal_eval
        try:
            self.sitename = config.get(sectionName, 'sitename')
            self.databaseFilename = config.get(sectionName, 'data-filename')
            self.homographyFilename = config.get(sectionName, 'homography-filename')
            self.calibrationFilename = config.get(sectionName, 'calibration-filename') 
            self.videoFilename = config.get(sectionName, 'video-filename')
            self.frameRate = config.getfloat(sectionName, 'framerate')
            self.date = datetime.strptime(config.get(sectionName, 'date'), datetimeFormat) # 2011-06-22 11:00:39
            self.translation = literal_eval(config.get(sectionName, 'translation')) #         = [0.0, 0.0]
            self.rotation = config.getfloat(sectionName, 'rotation')
            self.duration = config.getint(sectionName, 'duration')
        except NoOptionError as e:
            print(e)
            print('Not a section for scene meta-data')

    @staticmethod
    def loadConfigFile(filename):
        from configparser import ConfigParser
        config = ConfigParser()
        config.readfp(utils.openCheck(filename))
        configDict = dict()
        for sectionName in config.sections():
            configDict[sectionName] = SceneParameters(config, sectionName) 
        return configDict


if __name__ == '__main__':
    import doctest
    import unittest
    suite = doctest.DocFileSuite('tests/storage.txt')
    unittest.TextTestRunner().run(suite)
#     #doctest.testmod()
#     #doctest.testfile("example.txt")
