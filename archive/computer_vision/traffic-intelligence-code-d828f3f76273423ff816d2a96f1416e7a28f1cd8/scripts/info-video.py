#! /usr/bin/env python3

import sys, argparse

from trafficintelligence import cvutils

parser = argparse.ArgumentParser(description='The program displays the video.')
parser.add_argument('-i', dest = 'videoFilename', help = 'name of the video file', required = True)

args = parser.parse_args()

videoProperties = cvutils.infoVideo(args.videoFilename)
for k,v in videoProperties.items():
    print('Video {}: {}'.format(k, v))
