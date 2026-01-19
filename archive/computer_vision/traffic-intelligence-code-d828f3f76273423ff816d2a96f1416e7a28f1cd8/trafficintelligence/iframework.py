''' Framework to record data to measure the three street functions: access, transit and place '''

from enum import Enum
from pathlib import Path
from datetime import datetime

from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import Table, Column, Integer, Boolean, String, Float, DateTime, Enum as SQLEnum, ForeignKey, CheckConstraint, create_engine
from sqlalchemy.orm import relationship, backref, sessionmaker, declarative_base

Base = declarative_base()

GenderEnum = Enum('GenderEnum', 'male female unknown')
ModeEnum = Enum('ModeEnum', 'car_driver car_passenger transit taxi motorcycle cycling walking other') # the idea is that the mode could be sufficient to record all events (line and zone crossings), whether the actual, more precise vehicle, is 
VehicleEnum = Enum('VehicleEnum', 'car bike van truck bus taxi motorcycle scooter skate rollers mobility_scooter')
ActivityEnum = Enum('ActivityEnum', 'unknown strolling jogging shopping sitting talking resting eating playing doing_exercise smoking using_cellphone observing reading_writing performing selling playing_with_pet taking_pet_for_walk')
DisabilityEnum = Enum('DisabilityEnum', 'none wheelchair walker cane white_cane')
AgeEnum = Enum('AgeEnum', 'unknown infant toddler child teen young_adult adult senior') # 0-1, 1-4, 4-12, 12-18...

# should there be a survey object for site info, observer, etc?

class Mode(Base):
    '''A mode is personal, because in a group (family), some might have a scooter or rollers'''
    __tablename__ = 'modes'
    idx = Column(Integer, primary_key=True)
    personIdx = Column(Integer, ForeignKey('persons.idx'))
    vehicleIdx = Column(Integer, ForeignKey('vehicles.idx'))
    name = Column(SQLEnum(ModeEnum), nullable=False)
    startTime = Column(DateTime) # None first time if only one mode
    pointIdx = Column(Integer, ForeignKey('points.idx'))

    person = relationship('Person', backref = backref('modes'))
    vehicle = relationship('Vehicle')
    point = relationship('Point')

    def __init__(self, name, person, vehicle = None, startTime = None, p = None):
        self.person = person
        self.name = name
        self.vehicle = vehicle
        self.startTime = startTime
        self.point = p

    @staticmethod
    def initGroup(name, group, vehicle = None, startTime = None):
        return [Mode(name, p, startTime) for p in group.getPersons()]

class Group(Base):
    __tablename__ = 'groups'
    idx = Column(Integer, primary_key=True)
    
    def __init__(self, persons):
        for p in persons:
            GroupBelonging(p, self)

    def getPersons(self):
        return [gb.person for gb in self.groupBelongings]

class GroupBelonging(Base):
    __tablename__ = 'groupbelongings'
    groupIdx = Column(Integer, ForeignKey('groups.idx'), primary_key=True)
    personIdx = Column(Integer, ForeignKey('persons.idx'), primary_key=True)
    pointIdx = Column(Integer, ForeignKey('points.idx'))
    startTime = Column(DateTime) # None first time if only one group

    person = relationship('Person', backref = backref('groupBelongings'))
    group = relationship('Group', backref = backref('groupBelongings'))
    point = relationship('Point')

    def __init__(self, person, group, startTime = None, p = None):
        self.person = person
        self.group = group
        self.startTime = startTime
        self.point = p

# in aggregated form, there is a total number of observations for a given time interval, a number for each binary variable and k-1 variables for a categorical variable with k categories
class Person(Base):
    __tablename__ = 'persons'
    idx = Column(Integer, primary_key=True)
    #groupIdx = Column(Integer, ForeignKey('groups.idx'))
    age = Column(SQLEnum(AgeEnum), nullable=True) #Column(String)
    gender = Column(SQLEnum(GenderEnum), nullable=False)
    disability = Column(String) #Column(SQLEnum(DisabilityEnum) #Boolean in Studio
    stroller = Column(Boolean) # the booleans could be strings or enum to have more information
    bag = Column(Boolean) 
    animal = Column(Boolean)
    # databaseFilename = Column(String) # refers to trajectory database, relative path
    # objectIdx = Column(Integer) # refers to object id in trajectory database

    #group = relationship('Group', backref = backref('persons'))

    def __init__(self, age = 'unknown', gender = 'unknown', disability = False, stroller = False, bag = False, animal = False):
        self.age = age
        self.gender = gender
        self.disability = disability
        self.stroller = stroller
        self.bag = bag
        self.animal = animal

    def getAgeNum(self):
        if str.isnumeric(self.age):
            return int(self.age)
        elif '.' in self.age:
            try:
                return float(self.age)
            except ValueError:
                pass
        else:
            return self.age

    def getGroups(self):
        if len(self.groupBelongings) > 0:
            return [gb.group for gb in self.groupBelongings]
        else:
            return None

class Vehicle(Base):
    __tablename__ = 'vehicles'
    idx = Column(Integer, primary_key=True)
    category = Column(SQLEnum(VehicleEnum), nullable=False)
    trailer = Column(Boolean)
    # databaseFilename = Column(String) # refers to trajectory database, relative path
    # objectIdx = Column(Integer) # refers to object id in trajectory database

    def __init__(self, category, trailer = False):
        self.category = category
        self.trailer = trailer

class Point(Base):
    __tablename__ = 'points'
    idx = Column(Integer, primary_key=True)
    x = Column(Float)
    y = Column(Float)

    def __init__(self, x, y):
        self.x = x
        self.y = y

pointLineAssociation = Table('pointlines', Base.metadata,
                             Column('pointIdx', Integer, ForeignKey('points.idx')),
                             Column('lineIdx', Integer, ForeignKey('lines.idx')))

class Line(Base):
    __tablename__ = 'lines'
    idx = Column(Integer, primary_key=True)
    name = Column(String)
    # todo define lines for access counting: add type? - AccessLine?

    points = relationship('Point', secondary=pointLineAssociation)

    def __init__(self, name, x1, y1, x2, y2):
        self.name = name
        self.points = [Point(x1, y1), Point(x2, y2)]

pointZoneAssociation = Table('pointzones', Base.metadata,
                             Column('pointIdx', Integer, ForeignKey('points.idx')),
                             Column('zoneIdx', Integer, ForeignKey('zones.idx')))

class Zone(Base):
    __tablename__ = 'zones'
    idx = Column(Integer, primary_key=True)
    name = Column(String)

    points = relationship('Point', secondary=pointZoneAssociation)

    def __init__(self, name, xs = None, ys = None):
        'xs and ys are the list of x and y coordinates'
        self.name = name
        if xs is not None and ys is not None:
            for x, y in zip(xs, ys):
                self.addPoint(x, y)

    def addPoint(self, x, y):
        self.points.append(Point(x, y))

class AbstractCrossing:
    def initPersonGroupCrossing(self, group, person, modeName, vehicle):
        ''' initiates with the crossing the group or person

        design question: what should be done about simple line counting, 
        without information about persons'''
        if person is None and group is not None: # create group
            self.group = group
            if modeName is not None:
                Mode.initGroup(modeName, group, vehicle)
        elif person is not None and group is None: # create person
            self.group = Group([person])
            if modeName is not None:
                Mode(modeName, person, vehicle)
        else:
            print('Warning: crossing person and group or both None')

class LineCrossing(AbstractCrossing,Base):
    __tablename__ = 'linecrossings' # formerly 'linepassings'
    idx = Column(Integer, primary_key=True)
    lineIdx = Column(Integer, ForeignKey('lines.idx'))
    groupIdx = Column(Integer, ForeignKey('groups.idx'))
    pointIdx = Column(Integer, ForeignKey('points.idx'))
    instant = Column(DateTime)
    speed = Column(Float)
    # wrongDirection = Column(Boolean)
    rightToLeft = Column(Boolean)

    line = relationship('Line')
    group = relationship('Group')
    point = relationship('Point')

    def __init__(self, line, instant, speed = None, wrongDirection = None, p = None, group = None, person = None, modeName = None, vehicle = None):
        # makes it possible to create person and mode for just counting
        # pass modeName as string to instantiate after
        self.line = line
        self.instant = instant
        self.speed = speed
        self.wrongDirection = wrongDirection
        self.point = p
        self.initPersonGroupCrossing(group, person, modeName, vehicle)

class ZoneCrossing(AbstractCrossing,Base):
    __tablename__ = 'zonecrossings'
    idx = Column(Integer, primary_key=True)
    zoneIdx = Column(Integer, ForeignKey('zones.idx'))
    groupIdx = Column(Integer, ForeignKey('groups.idx'))
    pointIdx = Column(Integer, ForeignKey('points.idx'))
    instant = Column(DateTime)
    entering = Column(Boolean)
    speed = Column(Float)

    zone = relationship('Zone')
    group = relationship('Group')
    point = relationship('Point')

    def __init__(self, zone, instant, entering, speed = None, p = None, group = None, person = None, modeName = None, vehicle = None):
        self.zone = zone
        self.instant = instant
        self.entering = entering
        self.speed = speed
        self.point = p
        self.initPersonGroupCrossing(group, person, modeName, vehicle)

class Activity(AbstractCrossing,Base):
    __tablename__ = 'activities'
    idx = Column(Integer, primary_key=True)
    activity = Column(SQLEnum(ActivityEnum), nullable=False) #Column(String)
    groupIdx = Column(Integer, ForeignKey('groups.idx'))
    # can an activity be done in a vehicle? Is it relevant? Can it be unambiguously identified?
    startTime = Column(DateTime)
    endTime = Column(DateTime)
    zoneIdx = Column(Integer, ForeignKey('zones.idx'))
    pointIdx = Column(Integer, ForeignKey('points.idx'))

    group = relationship('Group')
    zone = relationship('Zone')
    point = relationship('Point')
  
    def __init__(self, activity, startTime, endTime, zone, p = None, group = None, person = None, modeName = None, vehicle = None):
        self.activity = activity
        self.startTime = startTime
        self.endTime = endTime
        self.zone = zone
        self.point = p
        self.initPersonGroupCrossing(group, person, modeName, vehicle)

def createDatabase(filename, insertInExisting = False, createOnlyGroupTables = False):
    'creates a session to query the filename'
    if Path(filename).is_file() and not insertInExisting:
        print('The file '+filename+' exists')
        return None
    else:
        engine = create_engine('sqlite:///'+filename)
        if createOnlyGroupTables:
            Base.metadata.create_all(engine, tables = [Base.metadata.tables['modes'], Base.metadata.tables['groups'], Base.metadata.tables['groupbelongings'], Base.metadata.tables['persons'], Base.metadata.tables['vehicles'], Base.metadata.tables['points']])
        else:
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

if __name__ == '__main__': # demo code
    session = createDatabase('test.sqlite')
    if session is None:
        session = connectDatabase('test.sqlite')
    # count example
    p = Person('infant', 'female', bag = True)
    veh1 = Vehicle('car')
    modes = [Mode('cardriver', p, veh1), Mode('walking', p, startTime = datetime(2020,7,7,11,20))]
    
    line = Line('line1', 0.,0.,0.,10.)
    zone = Zone('zone1', [0., 0., 1., 1.], [0., 1., 1., 0.])
    destination = Zone('destination1', [10., 10., 11., 11.], [10., 11., 11., 10.])
    counts = [LineCrossing(line, datetime(2020,7,2,23,20+i), person = Person(AgeEnum(1+i), 'female', disability = True), modeName = 'walking') for i in range(5)]
    group1 = Group([Person(AgeEnum(3+i),'female', False, False, True, False) for i in range(3)])
    groupMode1 = Mode.initGroup('walking', group1)
    activities = [Activity('strolling', datetime(2020,7,2,23,0), datetime(2020,7,2,23,10), zone, person = Person('adult', 'male', True, False, True, False)),
                  Activity('eating', datetime(2020,7,2,23,10), datetime(2020,7,2,23,12), zone, person = Person('senior', 'male', True, False, True, False)),
                  Activity('playing', datetime(2020,7,2,22,0), datetime(2020,7,2,23,0), zone, group = group1)]
    counts.append(LineCrossing(line, datetime(2020,7,2,23,5), group = group1))
    counts.append(LineCrossing(line, datetime(2020,7,2,23,7), person = Person('young_adult', 'unknown'), modeName = 'cardriver', vehicle = Vehicle('car')))
    counts.append(LineCrossing(line, datetime(2020,7,2,23,9), person = Person('teen', 'unknown'), modeName = 'other', vehicle = Vehicle('scooter')))
    counts.append(LineCrossing(line, datetime(2020,7,2,23,11), person = Person('teen', 'female'), modeName = 'cycling'))
    counts.append(LineCrossing(line, datetime(2020,7,2,23,13), person = Person(), modeName = 'cardriver')) # example of counting cars without knowing the driver and passenger's attributes
    counts.append(LineCrossing(line, datetime(2020,7,2,23,15), group = Group([Person(AgeEnum(6+i)) for i in range(3)]), modeName = 'car_passenger'))

    counts.append(ZoneCrossing(zone, datetime(2020,7,7,9,5), True, person = Person('adult', 'male', False, False, True, False)))
    
    session.add_all([line, p, zone, group1, destination]+modes+groupMode1+counts+activities)
    
    session.commit()
    session.close()
