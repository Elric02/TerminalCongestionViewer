#! /usr/bin/env python3

import sys

import matplotlib.pyplot as plt
import numpy as np
import cv2

from trafficintelligence import cvutils, utils

if len(sys.argv) < 4:
   print('Usage: {} homography_filename original_size new_size (size can be width or height)'.format(sys.argv[0]))
   sys.exit()

homography = np.loadtxt(sys.argv[1])

imgPoints = np.array([[10,10],
                      [10,20],
                      [20,20],
                      [20,10]])

wldPoints = cvutils.homographyProject(imgPoints.T, homography).T

newSize = float(sys.argv[3])
originalSize = float(sys.argv[2])
imgPoints = imgPoints*newSize/originalSize

newHomography, mask = cv2.findHomography(imgPoints, wldPoints)

np.savetxt(sys.argv[1]+'.new', newHomography)

