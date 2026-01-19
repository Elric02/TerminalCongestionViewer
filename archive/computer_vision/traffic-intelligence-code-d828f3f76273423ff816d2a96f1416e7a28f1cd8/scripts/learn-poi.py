#! /usr/bin/env python3

import argparse

import numpy as np
from sklearn import mixture
import matplotlib.pyplot as plt

from trafficintelligence import storage, ml

parser = argparse.ArgumentParser(description='The program learns and displays Gaussians fit to beginnings and ends of object trajectories (based on Mohamed Gomaa Mohamed 2015 PhD).')
parser.add_argument('-d', dest = 'databaseFilename', help = 'name of the Sqlite database file', required = True)
parser.add_argument('-t', dest = 'trajectoryType', help = 'type of trajectories to display', choices = ['feature', 'object'], default = 'object')
parser.add_argument('-n', dest = 'nObjects', help = 'number of objects to display', type = int)
parser.add_argument('-norigins', dest = 'nOriginClusters', help = 'number of clusters for trajectory origins', required = True, type = int)
parser.add_argument('-ndestinations', dest = 'nDestinationClusters', help = 'number of clusters for trajectory destinations (=norigins if not provided)', type = int)
parser.add_argument('--covariance-type', dest = 'covarianceType', help = 'type of covariance of Gaussian model', default = "full")
parser.add_argument('-w', dest = 'worldImageFilename', help = 'filename of the world image')
parser.add_argument('-u', dest = 'unitsPerPixel', help = 'number of units of distance per pixel', type = float, default = 1.)
parser.add_argument('--display', dest = 'display', help = 'displays points of interests', action = 'store_true') # default is manhattan distance
parser.add_argument('--assign', dest = 'assign', help = 'assigns the trajectories to the POIs and saves the assignments', action = 'store_true')
parser.add_argument('--display-paths', dest = 'displayPaths', help = 'displays all possible origin destination if assignment is done', action = 'store_true')

# TODO test Variational Bayesian Gaussian Mixture BayesianGaussianMixture

args = parser.parse_args()

objects = storage.loadTrajectoriesFromSqlite(args.databaseFilename, args.trajectoryType, args.nObjects)

beginnings = []
ends = []
for o in objects:
    beginnings.append(o.getPositionAt(0).aslist())
    ends.append(o.getPositionAt(int(o.length())-1).aslist())
    if args.assign:
        o.od = [-1, -1]

beginnings = np.array(beginnings)
ends = np.array(ends)

nDestinationClusters = args.nDestinationClusters
if args.nDestinationClusters is None:
    nDestinationClusters = args.nOriginClusters

gmmId=0
models = {}
for nClusters, points, gmmType in zip([args.nOriginClusters, nDestinationClusters],
                                      [beginnings, ends],
                                      ['beginning', 'end']):
    # estimation
    gmm = mixture.GaussianMixture(n_components=nClusters, covariance_type = args.covarianceType)
    models[gmmType]=gmm.fit(points)
    if not models[gmmType].converged_:
        print('Warning: model for '+gmmType+' points did not converge')
    if args.display or args.assign:
        labels = models[gmmType].predict(points)
    # plot
    if args.display:
        fig = plt.figure()
        if args.worldImageFilename is not None and args.unitsPerPixel is not None:
            img = plt.imread(args.worldImageFilename)
            plt.imshow(img)
        ml.plotGMMClusters(models[gmmType], labels, points, fig, nUnitsPerPixel = args.unitsPerPixel)
        plt.axis('image')
        plt.title(gmmType)
        print(gmmType+' Clusters:\n{}'.format(ml.computeClusterSizes(labels, range(models[gmmType].n_components))))
    # save
    storage.savePOIsToSqlite(args.databaseFilename, models[gmmType], gmmType, gmmId)
    # save assignments
    if args.assign:
        for o, l in zip(objects, labels):
            if gmmType == 'beginning':
                o.od[0] = l
            elif gmmType == 'end':
                o.od[1] = l
    gmmId += 1

if args.assign:
    storage.savePOIAssignments(args.databaseFilename, objects)
    if args.displayPaths:
        for i in range(args.nOriginClusters):
            for j in range(args.nDestinationClusters):
                odObjects = [o for o in objects if o.od[0] == i and o.od[1] == j]
                if len(odObjects) > 0:
                    fig = plt.figure()
                    ax = fig.add_subplot(111)
                    ml.plotGMM(models['beginning'].means_[i], models['beginning'].covariances_[i], i, fig, 'b')
                    ml.plotGMM(models['end'].means_[j], models['end'].covariances_[j], j, fig, 'r')
                    for o in odObjects:
                        o.plot(withOrigin = True)
                    plt.title('OD {} to {}'.format(i,j))
                    plt.axis('equal')
                    plt.show()


if args.display:
    plt.axis('equal')
    plt.show()
