#! /usr/bin/env python
'''Libraries for the SUMO traffic simulation software
http://sumo.dlr.de
'''
import pandas as pd

    

def loadTazEdges(inFilename):
    '''Converts list of OSM edges per OSM edge and groups per TAZ
    format is csv with first two columns the OSM id and TAZ id, then the list of SUMO edge id

    Returns the list of SUMO edge per TAZ'''
    data = []
    tazs = {}
    with open(inFilename,'r') as f:
        f.readline() # skip the headers
        for r in f:
            tmp = r.strip().split(',')
            tazID = tmp[1]
            for edge in tmp[2:]:                
                if len(edge) > 0:
                    if tazID in tazs:
                        if edge not in tazs[tazID]:
                            tazs[tazID].append(edge)
                    else:
                        tazs[tazID] = [edge]
    return tazs

def edge2Taz(tazs):
    '''Returns the associative array of the TAZ of each SUMO edge'''
    edge2Tazs = {}
    for taz, edges in tazs.items():
        for edge in edges:
            if edge in edge2Tazs:
                print('error for edge: {} (taz {}/{})'.format(edge, edge2Tazs[edge], taz))
            edge2Tazs[edge] = taz
    return edge2Tazs

def saveTazEdges(outFilename, tazs):
    with open(outFilename,'w') as out:
        out.write('<tazs>\n')
        for tazID in tazs:
            out.write('<taz id="{}" edges="'.format(tazID)+' '.join(tazs[tazID])+'"/>\n')
        out.write('</tazs>\n')

# TODO add utils from process-cyber.py?
        
# if __name__ == "__main__":
#     import doctest
#     import unittest
#     suite = doctest.DocFileSuite('tests/sumo.txt')
#     #suite = doctest.DocTestSuite()
#     unittest.TextTestRunner().run(suite)
