#! /usr/bin/env python3

import sys, argparse

from trafficintelligence import utils, storage

parser = argparse.ArgumentParser(description='The program deletes (drops) the tables in the database before saving new results (for objects, tables object_features and objects are dropped; for interactions, the tables interactions and indicators are dropped')
#parser.add_argument('configFilename', help = 'name of the configuration file')
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database', required = True)
parser.add_argument('-t', dest = 'dataType', help = 'type of the data to remove', required = True, choices = ['object','interaction', 'bb', 'pois', 'prototype'])
args = parser.parse_args()

storage.deleteFromSqlite(args.databaseFilename, args.dataType)
