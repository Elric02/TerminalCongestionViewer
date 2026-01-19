#! /usr/bin/env python3
import argparse
import pandas as pd
import sqlite3
from numpy import min, max

#import utils
#import storage

parser = argparse.ArgumentParser(description='The program finds ground truth annotation objects with missing positions (and the times that do not have a position')
#parser.add_argument('configFilename', help = 'name of the configuration file') 
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Ground Truth Sqlite database', required = True)

args = parser.parse_args()

data = pd.read_sql_query("select object_id, frame_number from bounding_boxes", sqlite3.connect(args.databaseFilename))
#aggData = data.groupby(["object_id"]).agg([min, max, len])

nProblems = 0
for object_id in data["object_id"].unique():
    tmp = data[data["object_id"] == object_id]
    firstInstant = min(tmp["frame_number"])
    lastInstant = max(tmp["frame_number"])
    if lastInstant-firstInstant != len(tmp)-1:
        nProblems += 1
        times=set(range(firstInstant, lastInstant+1))
        print("Object {} has missing positions at instants: {}".format(object_id, times.difference(tmp["frame_number"])))

if nProblems == 0:
    print("No problems were detected in database {}".format(args.databaseFilename))
