#! /usr/bin/env python3

import numpy as np
import argparse
from cv2.ml import SVM_RBF, SVM_C_SVC, ROW_SAMPLE # row_sample for layout in cv2.ml.SVM_load

import cvutils, moving, ml, storage

parser = argparse.ArgumentParser(description='The program processes indicators for all pairs of road users in the scene')
parser.add_argument('-d', dest = 'directoryName', help = 'parent directory name for the directories containing the samples for the different road users', required = True)
parser.add_argument('--kernel', dest = 'kernelType', help = 'kernel type for the support vector machine (SVM)', default = SVM_RBF, type = long)
parser.add_argument('--svm', dest = 'svmType', help = 'SVM type', default = SVM_C_SVC, type = long)
parser.add_argument('--deg', dest = 'degree', help = 'SVM degree', default = 0, type = int)
parser.add_argument('--gamma', dest = 'gamma', help = 'SVM gamma', default = 1, type = int)
parser.add_argument('--coef0', dest = 'coef0', help = 'SVM coef0', default = 0, type = int)
parser.add_argument('--cvalue', dest = 'cvalue', help = 'SVM Cvalue', default = 1, type = int)
parser.add_argument('--nu', dest = 'nu', help = 'SVM nu', default = 0, type = int)
parser.add_argument('--svmp', dest = 'svmP', help = 'SVM p', default = 0, type = int)
parser.add_argument('--cfg', dest = 'configFilename', help = 'name of the classifier configuration file', required = True)
parser.add_argument('--confusion-matrix', dest = 'computeConfusionMatrix', help = 'compute the confusion matrix on the training data', action = 'store_true')

args = parser.parse_args()
classifierParams = storage.ClassifierParameters(args.configFilename)

imageDirectories = {moving.userTypeNames[2]: args.directoryName + "/Pedestrians/",
                    moving.userTypeNames[4]: args.directoryName + "/Cyclists/",
                    moving.userTypeNames[1]: args.directoryName + "/Vehicles/"}

trainingSamplesPBV = {}
trainingLabelsPBV = {}
trainingSamplesBV = {}
trainingLabelsBV = {}
trainingSamplesPB = {}
trainingLabelsPB = {}
trainingSamplesPV = {}
trainingLabelsPV = {}

for k, v in imageDirectories.items():
    print('Loading {} samples'.format(k))
    trainingSamples, trainingLabels = cvutils.createHOGTrainingSet(v, moving.userType2Num[k], classifierParams.hogRescaleSize, classifierParams.hogNOrientations, classifierParams.hogNPixelsPerCell, classifierParams.hogBlockNorm, classifierParams.hogNCellsPerBlock)
    trainingSamplesPBV[k], trainingLabelsPBV[k] = trainingSamples, trainingLabels
    if k != moving.userTypeNames[2]:
	trainingSamplesBV[k], trainingLabelsBV[k] = trainingSamples, trainingLabels
    if k != moving.userTypeNames[1]:
	trainingSamplesPB[k], trainingLabelsPB[k] = trainingSamples, trainingLabels
    if k != moving.userTypeNames[4]:
	trainingSamplesPV[k], trainingLabelsPV[k] = trainingSamples, trainingLabels

# Training the Support Vector Machine
print("Training Pedestrian-Cyclist-Vehicle Model")
model = ml.SVM(args.svmType, args.kernelType, args.degree, args.gamma, args.coef0, args.cvalue, args.nu, args.svmP)
classifications = model.train(np.concatenate(list(trainingSamplesPBV.values())), ROW_SAMPLE, np.concatenate(list(trainingLabelsPBV.values())), True)
if args.computeConfusionMatrix:
    print(classifications)
model.save(args.directoryName + "/modelPBV.xml")

print("Training Cyclist-Vehicle Model")
model = ml.SVM(args.svmType, args.kernelType, args.degree, args.gamma, args.coef0, args.cvalue, args.nu, args.svmP)
classifications = model.train(np.concatenate(list(trainingSamplesBV.values())), ROW_SAMPLE, np.concatenate(list(trainingLabelsBV.values())), True)
if args.computeConfusionMatrix:
    print(classifications)
model.save(args.directoryName + "/modelBV.xml")

print("Training Pedestrian-Cyclist Model")
model = ml.SVM(args.svmType, args.kernelType, args.degree, args.gamma, args.coef0, args.cvalue, args.nu, args.svmP)
classifications = model.train(np.concatenate(list(trainingSamplesPB.values())), ROW_SAMPLE, np.concatenate(list(trainingLabelsPB.values())), True)
if args.computeConfusionMatrix:
    print(classifications)
model.save(args.directoryName + "/modelPB.xml")

print("Training Pedestrian-Vehicle Model")
model = ml.SVM(args.svmType, args.kernelType, args.degree, args.gamma, args.coef0, args.cvalue, args.nu, args.svmP)
classifications = model.train(np.concatenate(list(trainingSamplesPV.values())), ROW_SAMPLE, np.concatenate(list(trainingLabelsPV.values())), True)
if args.computeConfusionMatrix:
    print(classifications)
model.save(args.directoryName + "/modelPV.xml")
