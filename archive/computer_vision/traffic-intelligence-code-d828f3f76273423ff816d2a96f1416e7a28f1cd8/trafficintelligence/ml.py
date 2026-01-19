#! /usr/bin/env python
'''Libraries for machine learning algorithms'''

from os import path
from random import shuffle
from copy import copy, deepcopy

import numpy as np
from matplotlib.pylab import text
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy.cluster.vq import kmeans, whiten, vq
from sklearn import mixture
try:
    import cv2
    opencvAvailable = True
except ImportError:
    print('OpenCV library could not be loaded (video replay functions will not be available)') # TODO change to logging module
    opencvAvailable = False

from trafficintelligence import utils

#####################
# OpenCV ML models
#####################

def computeConfusionMatrix(model, samples, responses):
    '''computes the confusion matrix of the classifier (model)

    samples should be n samples by m variables'''
    classifications = {}
    predictions = model.predict(samples)
    for predicted, y in zip(predictions, responses):
        classifications[(y, predicted)] = classifications.get((y, predicted), 0)+1
    return classifications

if opencvAvailable:
    class SVM(object):
        '''wrapper for OpenCV SimpleVectorMachine algorithm'''
        def __init__(self, svmType = cv2.ml.SVM_C_SVC, kernelType = cv2.ml.SVM_RBF, degree = 0, gamma = 1, coef0 = 0, Cvalue = 1, nu = 0, p = 0):
            self.model = cv2.ml.SVM_create()
            self.model.setType(svmType)
            self.model.setKernel(kernelType)
            self.model.setDegree(degree)
            self.model.setGamma(gamma)
            self.model.setCoef0(coef0)
            self.model.setC(Cvalue)
            self.model.setNu(nu)
            self.model.setP(p)

        def save(self, filename):
            self.model.save(filename)
            
        def train(self, samples, layout, responses, computePerformance = False):
            self.model.train(samples, layout, responses)
            if computePerformance:
                return computeConfusionMatrix(self, samples, responses)

        def predict(self, hog):
            retval, predictions = self.model.predict(hog)
            if hog.shape[0] == 1:
                return predictions[0][0]
            else:
                return np.asarray(predictions, dtype = np.int).ravel().tolist()

    def SVM_load(filename):
        if path.exists(filename):
            svm = SVM()
            svm.model = cv2.ml.SVM_load(filename)
            return svm
        else:
            print('Provided filename {} does not exist: model not loaded!'.format(filename))
            return None
        
#####################
# Clustering
#####################

class Centroid(object):
    'Wrapper around instances to add a counter'

    def __init__(self, instance, nInstances = 1):
        self.instance = instance
        self.nInstances = nInstances

    # def similar(instance2):
    #     return self.instance.similar(instance2)

    def add(self, instance2):
        self.instance = self.instance.multiply(self.nInstances)+instance2
        self.nInstances += 1
        self.instance = self.instance.multiply(1/float(self.nInstances))

    def average(c):
        inst = self.instance.multiply(self.nInstances)+c.instance.multiply(instance.nInstances)
        inst.multiply(1/(self.nInstances+instance.nInstances))
        return Centroid(inst, self.nInstances+instance.nInstances)

    def plot(self, options = ''):
        self.instance.plot(options)
        text(self.instance.position.x+1, self.instance.position.y+1, str(self.nInstances))

def kMedoids(similarityMatrix, initialCentroids = None, k = None):
    '''Algorithm that clusters any dataset based on a similarity matrix
    Either the initialCentroids or k are passed'''
    pass

def assignCluster(data, similarFunc, initialCentroids = None, shuffleData = True):
    '''k-means algorithm with similarity function
    Two instances should be in the same cluster if the sameCluster function returns true for two instances. It is supposed that the average centroid of a set of instances can be computed, using the function. 
    The number of clusters will be determined accordingly

    data: list of instances
    averageCentroid: '''
    localdata = copy(data) # shallow copy to avoid modifying data
    if shuffleData:
        shuffle(localdata)
    if initialCentroids is None:
        centroids = [Centroid(localdata[0])]
    else:
        centroids = deepcopy(initialCentroids)
    for instance in localdata[1:]:
        i = 0
        while i<len(centroids) and not similarFunc(centroids[i].instance, instance):
            i += 1
        if i == len(centroids):
            centroids.append(Centroid(instance))
        else:
            centroids[i].add(instance)

    return centroids

# TODO recompute centroids for each cluster: instance that minimizes some measure to all other elements

def spectralClustering(similarityMatrix, k, iter=20):
    '''Spectral Clustering algorithm'''
    n = len(similarityMatrix)
    # create Laplacian matrix
    rowsum = np.sum(similarityMatrix,axis=0)
    D = np.diag(1 / np.sqrt(rowsum))
    I = np.identity(n)
    L = I - np.dot(D,np.dot(similarityMatrix,D))
    # compute eigenvectors of L
    U,sigma,V = np.linalg.svd(L)
    # create feature vector from k first eigenvectors
    # by stacking eigenvectors as columns
    features = np.array(V[:k]).T
    # k-means
    features = whiten(features)
    centroids,distortion = kmeans(features,k, iter)
    code,distance = vq(features,centroids) # code starting from 0 (represent first cluster) to k-1 (last cluster)
    return code,sigma

def assignToPrototypeClusters(instances, initialPrototypeIndices, similarities, minSimilarity, similarityFunc, minClusterSize = 0):
    '''Assigns instances to prototypes 
    if minClusterSize is not 0, the clusters will be refined by removing iteratively the smallest clusters
    and reassigning all elements in the cluster until no cluster is smaller than minClusterSize

    labels are indices in the prototypeIndices'''
    prototypeIndices = copy(initialPrototypeIndices)
    indices = [i for i in range(len(instances)) if i not in prototypeIndices]
    labels = [-1]*len(instances)
    assign = True
    while assign:
        for i in prototypeIndices:
            labels[i] = i
        for i in indices:
            for j in prototypeIndices:
                if similarities[i][j] < 0:
                    similarities[i][j] = similarityFunc(instances[i], instances[j])
                    similarities[j][i] = similarities[i][j]
            label = similarities[i][prototypeIndices].argmax()
            if similarities[i][prototypeIndices[label]] >= minSimilarity:
                labels[i] = prototypeIndices[label]
            else:
                labels[i] = -1 # outlier
        clusterSizes = {i: sum(np.array(labels) == i) for i in prototypeIndices}
        smallestClusterIndex = min(clusterSizes, key = clusterSizes.get)
        assign = (clusterSizes[smallestClusterIndex] < minClusterSize)
        if assign:
            prototypeIndices.remove(smallestClusterIndex)
            indices = [i for i in range(similarities.shape[0]) if labels[i] == smallestClusterIndex]
    return prototypeIndices, labels

def prototypeCluster(instances, similarities, minSimilarity, similarityFunc, optimizeCentroid = False, randomInitialization = False, initialPrototypeIndices = None):
    '''Finds exemplar (prototype) instance that represent each cluster
    Returns the prototype indices (in the instances list)

    the elements in the instances list must have a length (method __len__), or one can use the optimizeCentroid
    the positions in the instances list corresponds to the similarities
    if similarityFunc is provided, the similarities are calculated as needed (this is faster) if not in similarities (negative if not computed)
    similarities must still be allocated with the right size

    if an instance is different enough (<minSimilarity), 
    it will become a new prototype. 
    Non-prototype instances will be assigned to an existing prototype

    if optimizeCentroid is True, each time an element is added, we recompute the centroid trajectory as the most similar to all in the cluster

    initialPrototypeIndices are indices in instances

    TODO: check how similarity evolves in clusters'''
    if len(instances) == 0:
        print('no instances to cluster (empty list)')
        return None

    # sort instances based on length
    indices = list(range(len(instances)))
    if randomInitialization or optimizeCentroid:
        indices = np.random.permutation(indices).tolist()
    else:
        indices.sort(key=lambda i: len(instances[i]))
    # initialize clusters
    clusters = []
    if initialPrototypeIndices is None:
        prototypeIndices = [indices[0]]
    else:
        prototypeIndices = initialPrototypeIndices # think of the format: if indices, have to be in instances
    for i in prototypeIndices:
        clusters.append([i])
        indices.remove(i)
    # go through all instances
    for i in indices:
        for j in prototypeIndices:
            if similarities[i][j] < 0:
                similarities[i][j] = similarityFunc(instances[i], instances[j])
                similarities[j][i] = similarities[i][j]
        label = similarities[i][prototypeIndices].argmax() # index in prototypeIndices
        if similarities[i][prototypeIndices[label]] < minSimilarity:
            prototypeIndices.append(i)
            clusters.append([])
        else:
            clusters[label].append(i)
            if optimizeCentroid:
                if len(clusters[label]) >= 2: # no point if only one element in cluster
                    for j in clusters[label][:-1]:
                        if similarities[i][j] < 0:
                            similarities[i][j] = similarityFunc(instances[i], instances[j])
                            similarities[j][i] = similarities[i][j]
                    clusterIndices = clusters[label]
                    clusterSimilarities = similarities[clusterIndices][:,clusterIndices]
                    newCentroidIdx = clusterIndices[clusterSimilarities.sum(0).argmax()]
                    if prototypeIndices[label] != newCentroidIdx:
                        prototypeIndices[label] = newCentroidIdx
            elif len(instances[prototypeIndices[label]]) < len(instances[i]): # replace prototype by current instance i if longer # otherwise, possible to test if randomInitialization or initialPrototypes is not None
                prototypeIndices[label] = i
    return prototypeIndices

def computeClusterSizes(labels, prototypeIndices, outlierIndex = -1):
    clusterSizes = {i: sum(np.array(labels) == i) for i in prototypeIndices}
    clusterSizes['outlier'] = sum(np.array(labels) == outlierIndex)
    return clusterSizes

def computeClusterStatistics(labels, prototypeIndices, instances, similarities, similarityFunc, clusters = None, outlierIndex = -1):
    if clusters is None:
        clusters = {protoId:[] for protoId in prototypeIndices+[-1]}
        for i,l in enumerate(labels):
            clusters[l].append(i)
        clusters = [clusters[protoId] for protoId in prototypeIndices]
    for i, cluster in enumerate(clusters):
        n = len(cluster)
        print('cluster {}: {} elements'.format(prototypeIndices[i], n))
        if n >=2:
            for j,k in enumerate(cluster):
                for l in cluster[:j]:
                    if similarities[k][l] < 0:
                        similarities[k][l] = similarityFunc(instances[k], instances[l])
                        similarities[l][k] = similarities[k][l]
            print('Mean similarity to prototype: {}'.format((similarities[prototypeIndices[i]][cluster].sum()+1)/(n-1)))
            print('Mean overall similarity: {}'.format((similarities[cluster][:,cluster].sum()+n)/(n*(n-1))))

# Gaussian Mixture Models
def plotGMM(mean, covariance, gmmId, fig, color, alpha = 0.3):
    v, w = np.linalg.eigh(covariance)
    angle = 180*np.arctan2(w[0][1], w[0][0])/np.pi
    v *= 4
    ell = mpl.patches.Ellipse(mean, v[0], v[1], 180+angle, color=color)
    ell.set_clip_box(fig.bbox)
    ell.set_alpha(alpha)
    fig.axes[0].add_artist(ell)
    plt.plot([mean[0]], [mean[1]], 'x'+color)
    plt.annotate(str(gmmId), xy=(mean[0]+1, mean[1]+1))

def plotGMMClusters(model, labels = None, dataset = None, fig = None, colors = utils.colors, nUnitsPerPixel = 1., alpha = 0.3):
    '''plot the ellipse corresponding to the Gaussians
    and the predicted classes of the instances in the dataset'''
    if fig is None:
        fig = plt.figure()
    if len(fig.get_axes()) == 0:
        fig.add_subplot(111)
    for i in range(model.n_components):
        mean = model.means_[i]/nUnitsPerPixel
        covariance = model.covariances_[i]/nUnitsPerPixel
        # plot points
        if dataset is not None:
            tmpDataset = dataset/nUnitsPerPixel
            plt.scatter(tmpDataset[labels == i, 0], tmpDataset[labels == i, 1], .8, color=colors[i])
        # plot an ellipse to show the Gaussian component
        plotGMM(mean, covariance, i, fig, colors[i], alpha)
    if dataset is None: # to address issues without points, the axes limits are not redrawn
        minima = model.means_.min(0)
        maxima = model.means_.max(0)
        xwidth = 0.5*(maxima[0]-minima[0])
        ywidth = 0.5*(maxima[1]-minima[1])
        plt.xlim(minima[0]-xwidth,maxima[0]+xwidth)
        plt.ylim(minima[1]-ywidth,maxima[1]+ywidth)

if __name__ == "__main__":
    import doctest
    import unittest
    suite = doctest.DocFileSuite('tests/ml.txt')
    unittest.TextTestRunner().run(suite)
#     #doctest.testmod()
#     #doctest.testfile("example.txt")
