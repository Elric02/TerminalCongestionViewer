from datetime import datetime, timedelta
from pathlib import Path
from os import path, listdir, sep
from math import floor

from numpy import zeros, loadtxt, array

from sqlalchemy import orm, create_engine, Column, Integer, Float, DateTime, String, ForeignKey, Boolean, Interval
from sqlalchemy.orm import relationship, backref, sessionmaker, declarative_base

from trafficintelligence.utils import datetimeFormat, removeExtension, getExtension, TimeConverter
from trafficintelligence.cvutils import computeUndistortMaps, videoFilenameExtensions, infoVideo
from trafficintelligence.moving import TimeInterval, Trajectory

"""
Metadata to describe how video data and configuration files for video analysis are stored

Typical example is 

site1/view1/2012-06-01/video.avi
           /2012-06-02/video.avi
                       ...
     /view2/2012-06-01/video.avi
           /2012-06-02/video.avi
     ...

- where site1 is the path to the directory containing all information pertaining to the site, 
relative to directory of the SQLite file storing the metadata
represented by Site class
(can contain for example the aerial or map image of the site, used for projection)

- view1 is the directory for the first camera field of view (camera fixed position) at site site1
represented by CameraView class
(can contain for example the homography file, mask file and tracking configuration file)

- YYYY-MM-DD is the directory containing all the video files for that day
with camera view view1 at site site1


"""

Base = declarative_base()

class Site(Base):
    __tablename__ = 'sites'
    idx = Column(Integer, primary_key=True)
    name = Column(String) # path to directory containing all site information (in subdirectories), relative to the database position
    description = Column(String) # longer names, eg intersection of road1 and road2
    xcoordinate = Column(Float)  # ideally moving.Point, but needs to be 
    ycoordinate = Column(Float)
    mapImageFilename = Column(String) # path to map image file, relative to site name, ie sitename/mapImageFilename
    nUnitsPerPixel = Column(Float) # number of units of distance per pixel in map image
    worldDistanceUnit = Column(String, default = 'm') # make sure it is default in the database
    
    def __init__(self, name, description = "", xcoordinate = None, ycoordinate = None, mapImageFilename = None, nUnitsPerPixel = 1., worldDistanceUnit = 'm'):
        self.name = name
        self.description = description
        self.xcoordinate = xcoordinate
        self.ycoordinate = ycoordinate
        self.mapImageFilename = mapImageFilename
        self.nUnitsPerPixel = nUnitsPerPixel
        self.worldDistanceUnit = worldDistanceUnit

    def getPath(self):
        return self.name

    def getMapImageFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.getPath(), self.mapImageFilename)
        else:
            return self.mapImageFilename


class EnvironementalFactors(Base):
    '''Represents any environmental factors that may affect the results, in particular
    * changing weather conditions
    * changing road configuration, geometry, signalization, etc.
    ex: sunny, rainy, before counter-measure, after counter-measure'''
    __tablename__ = 'environmental_factors'
    idx = Column(Integer, primary_key=True)
    startTime = Column(DateTime)
    endTime = Column(DateTime)
    description = Column(String) # eg sunny, before, after
    siteIdx = Column(Integer, ForeignKey('sites.idx'))

    site = relationship("Site", backref = backref('environmentalFactors'))

    def __init__(self, startTime, endTime, description, site):
        'startTime is passed as string in utils.datetimeFormat, eg 2011-06-22 10:00:39'
        self.startTime = datetime.strptime(startTime, datetimeFormat)
        self.endTime = datetime.strptime(endTime, datetimeFormat)
        self.description = description
        self.site = site

class CameraType(Base):
    ''' Represents parameters of the specific camera used. 

    Taken and adapted from tvalib'''
    __tablename__ = 'camera_types'
    idx = Column(Integer, primary_key=True)
    name = Column(String)
    resX = Column(Integer)
    resY = Column(Integer)
    frameRate = Column(Float)
    frameRateTimeUnit = Column(String, default = 's')
    intrinsicCameraMatrixStr = Column(String)
    distortionCoefficientsStr = Column(String)
    
    def __init__(self, name, resX, resY, frameRate, frameRateTimeUnit = 's', trackingConfigurationFilename = None, intrinsicCameraFilename = None, intrinsicCameraMatrix = None, distortionCoefficients = None):
        self.name = name
        self.resX = resX
        self.resY = resY
        self.frameRate = frameRate
        self.frameRateTimeUnit = frameRateTimeUnit
        self.intrinsicCameraMatrix = None # should be np.array
        self.distortionCoefficients = None # list
        
        if trackingConfigurationFilename is not None:
            from storage import ProcessParameters
            params = ProcessParameters(trackingConfigurationFilename)
            self.intrinsicCameraMatrix = params.intrinsicCameraMatrix
            self.distortionCoefficients = params.distortionCoefficients
        elif intrinsicCameraFilename is not None:
            self.intrinsicCameraMatrix = loadtxt(intrinsicCameraFilename)
            self.distortionCoefficients = distortionCoefficients
        else:
            self.intrinsicCameraMatrix = intrinsicCameraMatrix
            self.distortionCoefficients = distortionCoefficients
            
        if self.intrinsicCameraMatrix is not None:
            self.intrinsicCameraMatrixStr = str(self.intrinsicCameraMatrix.tolist())
        if self.distortionCoefficients is not None and len(self.distortionCoefficients) == 5:
            self.distortionCoefficientsStr = str(self.distortionCoefficients)

    @orm.reconstructor
    def initOnLoad(self):
        if self.intrinsicCameraMatrixStr is not None:
            from ast import literal_eval
            self.intrinsicCameraMatrix = array(literal_eval(self.intrinsicCameraMatrixStr))
        else:
            self.intrinsicCameraMatrix = None
        if self.distortionCoefficientsStr is not None:
            self.distortionCoefficients = literal_eval(self.distortionCoefficientsStr)
        else:
            self.distortionCoefficients = None

    def computeUndistortMaps(self, undistortedImageMultiplication = None):
        if undistortedImageMultiplication is not None and self.intrinsicCameraMatrix is not None and self.distortionCoefficients is not None:
            [self.map1, self.map2], newCameraMatrix = computeUndistortMaps(self.resX, self.resY, undistortedImageMultiplication, self.intrinsicCameraMatrix, self.distortionCoefficients)
        else:
            self.map1 = None
            self.map2 = None

    @staticmethod
    def getCameraType(session, cameraTypeId, resX = None):
        'Returns the site(s) matching the index or the name'
        if str.isdigit(cameraTypeId):
            return session.query(CameraType).filter(CameraType.idx == int(cameraTypeId)).all()
        else:
            if resX is not None:
                return session.query(CameraType).filter(CameraType.name.like('%'+cameraTypeId+'%')).filter(CameraType.resX == resX).all()
            else:
                return session.query(CameraType).filter(CameraType.name.like('%'+cameraTypeId+'%')).all()

# class SiteDescription(Base): # list of lines and polygons describing the site, eg for sidewalks, center lines
            
class CameraView(Base):
    __tablename__ = 'camera_views'
    idx = Column(Integer, primary_key=True)
    description = Column(String)
    homographyFilename = Column(String) # path to homograph file, relative to the site name
    siteIdx = Column(Integer, ForeignKey('sites.idx'))
    cameraTypeIdx = Column(Integer, ForeignKey('camera_types.idx'))
    trackingConfigurationFilename = Column(String) # path to configuration .cfg file, relative to site name
    maskFilename = Column(String) # path to mask file, relative to site name
    virtual = Column(Boolean) # indicates it is not a real camera view, eg merged
    
    site = relationship("Site", backref = backref('cameraViews'))
    cameraType = relationship('CameraType', backref = backref('cameraViews'))

    def __init__(self, description, homographyFilename, site, cameraType, trackingConfigurationFilename, maskFilename, virtual = False):
        self.description = description
        self.homographyFilename = homographyFilename
        self.site = site
        self.cameraType = cameraType
        self.trackingConfigurationFilename = trackingConfigurationFilename
        self.maskFilename = maskFilename
        self.virtual = virtual

    def getHomographyFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.site.getPath(), self.homographyFilename)
        else:
            return self.homographyFilename

    def getTrackingConfigurationFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.site.getPath(), self.trackingConfigurationFilename)
        else:
            return self.trackingConfigurationFilename

    def getMaskFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.site.getPath(), self.maskFilename)
        else:
            return self.maskFilename

    def getTrackingParameters(self):
        return ProcessParameters(self.getTrackingConfigurationFilename())

    def getHomographyDistanceUnit(self):
        return self.site.worldDistanceUnit
    
class Alignment(Base):
    __tablename__ = 'alignments'
    idx = Column(Integer, primary_key=True)
    siteIdx = Column(Integer, ForeignKey('sites.idx'))
    
    site = relationship("Site", backref = backref('alignments'))

    def __init__(self, site):
        self.site = site

    def getTrajectory(self):
        t = Trajectory()
        for p in self.points:
            t.addPositionXY(p.x_coordinate, p.y_coordinate)
        return t

class Point(Base):
    __tablename__ = 'positions'
    trajectory_id = Column(Integer, ForeignKey('alignments.idx'), primary_key=True)
    frame_number = Column(Integer, primary_key=True) # order of points in this alignment, as index
    x_coordinate = Column(Float)
    y_coordinate = Column(Float)

    alignment = relationship("Alignment", backref = backref('points', order_by = trajectory_id))
    
    def __init__(self, alignment, index, x, y):
        self.alignment = alignment
        self.frame_number = index
        self.x_coordinate = x
        self.y_coordinate = y

class VideoSequence(Base):
    __tablename__ = 'video_sequences'
    idx = Column(Integer, primary_key=True)
    name = Column(String) # path to the video file relative to the the site name
    startTime = Column(DateTime)
    duration = Column(Interval) # video sequence duration
    databaseFilename = Column(String) # path to the database file relative to the the site name
    virtual = Column(Boolean) # indicates it is not a real video sequence (no video file), eg merged
    cameraViewIdx = Column(Integer, ForeignKey('camera_views.idx'))

    cameraView = relationship("CameraView", backref = backref('videoSequences', order_by = idx))

    def __init__(self, name, startTime, duration, cameraView, databaseFilename = None, virtual = False):
        '''startTime is passed as string in utils.datetimeFormat, eg 2011-06-22 10:00:39
        duration is a timedelta object'''
        self.name = name
        if isinstance(startTime, str):
            self.startTime = datetime.strptime(startTime, datetimeFormat)
        else:
            self.startTime = startTime
        self.duration = duration
        self.cameraView = cameraView
        if databaseFilename is None and len(self.name) > 0:
            self.databaseFilename = removeExtension(self.name)+'.sqlite'
        else:
            self.databaseFilename = databaseFilename
        self.virtual = virtual

    def getVideoSequenceFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.cameraView.site.getPath(), self.name)
        else:
            return self.name

    def getDatabaseFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.cameraView.site.getPath(), self.databaseFilename)
        else:
            return self.databaseFilename

    def getTimeInterval(self):
        return TimeInterval(self.startTime, self.startTime+self.duration)
        
    def containsInstant(self, instant):
        'instant is a datetime'
        return self.startTime <= instant and self.startTime+self.duration

    def intersection(self, startTime, endTime):
        'returns the moving.TimeInterval intersection with [startTime, endTime]'
        return TimeInterval.intersection(self.getTimeInterval(), TimeInterval(startTime, endTime)) 
        
    def getFrameNum(self, instant):
        'Warning, there is no check of correct time units'
        if self.containsInstant(instant):
            return int(floor((instant-self.startTime).seconds*self.cameraView.cameraType.frameRate))
        else:
            return None

class TrackingAnnotation(Base):
    __tablename__ = 'tracking_annotations'
    idx = Column(Integer, primary_key=True)
    description = Column(String) # description
    groundTruthFilename = Column(String)
    firstFrameNum = Column(Integer) # first frame num of annotated data (could be computed on less data)
    lastFrameNum = Column(Integer)
    videoSequenceIdx = Column(Integer, ForeignKey('video_sequences.idx'))
    maskFilename = Column(String) # path to mask file (can be different from camera view, for annotations), relative to site name
    undistorted = Column(Boolean) # indicates whether the annotations were done in undistorted video space

    videoSequence = relationship("VideoSequence", backref = backref('trackingAnnotations'))
    
    def __init__(self, description, groundTruthFilename, firstFrameNum, lastFrameNum, videoSequence, maskFilename, undistorted = True):
        self.description = description
        self.groundTruthFilename = groundTruthFilename
        self.firstFrameNum = firstFrameNum
        self.lastFrameNum = lastFrameNum
        self.videoSequence = videoSequence
        self.undistorted = undistorted
        self.maskFilename = maskFilename

    def getGroundTruthFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.videoSequence.cameraView.site.getPath(), self.groundTruthFilename)
        else:
            return self.groundTruthFilename

    def getMaskFilename(self, relativeToSiteFilename = True):
        if relativeToSiteFilename:
            return path.join(self.videoSequence.cameraView.site.getPath(), self.maskFilename)
        else:
            return self.maskFilename

    def getTimeInterval(self):
        return TimeInterval(self.firstFrameNum, self.lastFrameNum)
        
# add class for Analysis: foreign key VideoSequenceId, dataFilename, configFilename (get the one from camera view by default), mask? (no, can be referenced in the tracking cfg file)

# class Analysis(Base): # parameters necessary for processing the data: free form
# eg bounding box depends on camera view, tracking configuration depends on camera view 
# results: sqlite

def createDatabase(filename):
    'creates a session to query the filename'
    if Path(filename).is_file():
        print('The file '+filename+' exists')
        return None
    else:
        engine = create_engine('sqlite:///'+filename)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        return Session()

def connectDatabase(filename):
    'creates a session to query the filename'
    if Path(filename).is_file():
        engine = create_engine('sqlite:///'+filename)
        Session = sessionmaker(bind=engine)
        return Session()
    else:
        print('The file '+filename+' does not exist')
        return None

def getSite(session, siteId = None, name = None, description = None):
    'Returns the site(s) matching the index or the name'
    if siteId is not None:
        return session.query(Site).filter(Site.idx == int(siteId)).all()
    elif name is not None:
        return session.query(Site).filter(Site.name.like('%'+name+'%')).all()
    elif description is not None:
        return session.query(Site).filter(Site.description.like('%'+description+'%')).all()
    else:
        print('No siteId, name or description have been provided to the function')
        return []

def getCameraView(session, viewId):
    'Returns the site(s) matching the index'
    return session.query(CameraView).filter(CameraView.idx == int(viewId)).first()

def getSiteVideoSequences(site):
    return [vs for cv in site.cameraViews for vs in cv.videoSequences]

def initializeSites(session, directoryName, nViewsPerSite = 1):
    '''Initializes default site objects and n camera views per site
    
    eg somedirectory/montreal/ contains intersection1, intersection2, etc.
    The site names would be somedirectory/montreal/intersection1, somedirectory/montreal/intersection2, etc.
    The views should be directories in somedirectory/montreal/intersection1'''
    sites = []
    cameraViews = []
    names = sorted(listdir(directoryName))
    for name in names:
        if path.isdir(directoryName+sep+name):
            sites.append(Site(directoryName+sep+name, None))
            for cameraViewIdx in range(1, nViewsPerSite+1):
                cameraViews.append(CameraView('view{}'.format(cameraViewIdx), None, sites[-1], None, None, None))
    session.add_all(sites)
    session.add_all(cameraViews)
    session.commit()

def initializeVideos(session, cameraView, directoryName, startTime = None, datetimeFormat = None):
    '''Initializes videos with time or tries to guess it from filename
    directoryName should contain the videos to find and be the relative path from the site location'''
    names = sorted(listdir(directoryName))
    videoSequences = []
    if datetimeFormat is not None:
        timeConverter = TimeConverter(datetimeFormat)
    for name in names:
        prefix = removeExtension(name)
        extension = getExtension(name)
        if extension in videoFilenameExtensions:
            if datetimeFormat is not None:
                from argparse import ArgumentTypeError
                try:
                    t1 = timeConverter.convert(name[:name.rfind('_')])
                    print('DB time {} / Time from filename {}'.format(startTime, t1))
                except ArgumentTypeError as e:
                    print('File format error for time {} (prefix {})'.format(name, prefix))
            vidinfo = infoVideo(directoryName+sep+name)
            duration = vidinfo['number of frames']#timedelta(minutes = 27, seconds = 33)
            fps = vidinfo['fps']
            duration = timedelta(seconds=duration/fps)
            videoSequences.append(VideoSequence(directoryName+sep+name, startTime, duration, cameraView, directoryName+sep+prefix+'.sqlite'))
            startTime += duration
    session.add_all(videoSequences)
    session.commit()

def generateTimeIntervals(videoSequences, maxTimeGap):
    ''

def addAlignment(session, site, t):
    'Adds trajectory (moving.Trajectory) t to metadata of site'
    al = Alignment(site)
    session.add(al)
    session.commit()
    points = []
    for i,p in enumerate(t):
        points.append(Point(al, i, p.x, p.y))
    session.add_all(points)
    session.commit()

def reportSiteVideoDurations(sites):
    for s in sites:
        print(s.name)
        for cv in s.cameraViews:
            startTime = cv.videoSequences[0].startTime
            duration = cv.videoSequences[0].duration
            for vs in cv.videoSequences:
                duration += vs.duration
            print(cv.description, startTime, duration)

    
# management
# TODO need to be able to copy everything from a site from one sqlite to another, and delete everything attached to a site
