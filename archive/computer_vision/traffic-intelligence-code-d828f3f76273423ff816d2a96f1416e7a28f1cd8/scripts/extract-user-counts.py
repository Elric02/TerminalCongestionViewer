#! /usr/bin/env python

#import numpy as np
import argparse # os
import pandas as pd
import sqlite3

from trafficintelligence import utils, moving

parser = argparse.ArgumentParser(description='The program generates a csv of the user counts per SQLite database in the directory ')
parser.add_argument('-i', dest = 'dirname', help = 'name of the directory containing all the databases', default = '.')
args = parser.parse_args()

out = open('user-counts.csv', 'w')
out.write('filename')
for i in range(1,7):
    out.write(','+moving.userTypeNames[i])
out.write('\n')
for fn in utils.listfiles(args.dirname, 'sqlite'):
    with sqlite3.connect(fn) as connection:
        data = pd.read_sql('SELECT road_user_type, count(*) AS n FROM objects GROUP BY road_user_type', connection)
        counts = {i:'0' for i in range(1,7)}
        for i,r in data.iterrows():
            counts[r['road_user_type']]=str(r['n'])
        fn2 = fn.split('/')[-1].rsplit('.',1)[0]
        out.write(fn2+','+','.join(counts.values())+'\n')
out.close()
