#! /usr/bin/env python3

import sys, argparse, cv2, numpy as np

parser = argparse.ArgumentParser(description='''The program replays the video and allows to manually id vehicles and mark instants, eg when they cross given areas in the scene. Use this program in combination with a screen marker program (For example, Presentation Assistant) to draw multiple lines on the screen.''',
                                 epilog = '''The output should give you a .csv file with the same name as your video file with columns in this format:
vehicle number, frame number
You can easily spot mistakes in the csv file for a line with number, SKIP. If this happens, just delete the previous vehicle observation.''',
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('-i', dest = 'videoFilename', help = 'name of the video file', required = True)
parser.add_argument('-o', dest = 'outputFilename', help = 'name of the output file (csv file)')
parser.add_argument('-f', dest = 'firstFrameNum', help = 'number of first frame number to display', default = 0, type = int)
parser.add_argument('-n', dest = 'nAttributes', help = 'number of attributes characterizing users', default = 0, type = int)

args = parser.parse_args()

print('''Commands:
Press o when you make a mistake in input
Press d to skip 100 frames
Press s to skip 10 frames
Press c to go back 100 frames
Press x to go back 10 frames
Press spacebar to go forward one frame
Press l to skip to frame number
Press s to finish inputting user characteristics (if any in pop up window)
Press q to quit and end program''')
# configuration of keys and user types (see moving)
userTypeNames = ['unknown',
                 'car',
                 'pedestrian',
                 'motorcycle',
                 'cyclist',
                 'bus',
                 'truck',
                 'automated']
class UserConfiguration(object):
    def __init__(self, name, keyNew, keyAddInstant, nAttributes):
        self.name = name
        self.keyNew = ord(keyNew)
        self.keyAddInstant = ord(keyAddInstant)
        self.userNum = 0
        self.nAttributes = nAttributes
        self.resetAttributes()

    def getHelpStr(self):
        return 'Press {} for new {}, {} for new instant for current {}'.format(chr(self.keyNew), self.name, chr(self.keyAddInstant), self.name)
        
    def resetAttributes(self):
        self.userInstant = 0
        self.attributes = [-1]*self.nAttributes

    def setAttribute(self, i, value):
        self.attributes[i%self.nAttributes] = value

    def getAttributeStr(self):
        if self.nAttributes > 0:
            return ','.join([str(i) for i in self.attributes])+','
        else:
            return ''
        
    def isKeyNew(self, k):
        return (k == self.keyNew)

    def isKeyAddInstant(self, k):
        return (k == self.keyAddInstant)
    
    def isKey(self, k):
        return self.isKeyNew(k) or self.isKeyAddInstant(k)

    @staticmethod
    def getConfigurationWithKey(configurations, k):
        for c in configurations:
            if c.isKey(k):
                return c
        return None

userConfigurations = [UserConfiguration(userTypeNames[1],'u','i', args.nAttributes),
                      UserConfiguration(userTypeNames[2],'j','k', args.nAttributes)]

print(' ')
for c in userConfigurations:
    print(c.getHelpStr())

# start of program
cap = cv2.VideoCapture(args.videoFilename)
cap.set(cv2.CAP_PROP_POS_FRAMES, args.firstFrameNum)
fps = cap.get(cv2.CAP_PROP_FPS)
print('Video at {} frames/s'.format(fps))
cv2.namedWindow('Video', cv2.WINDOW_NORMAL)

# output filename
if args.outputFilename is None:
    i = args.videoFilename.rfind('.')
    if i>0:
        outputFilename = args.videoFilename[:i]+'.csv'
    else:
        outputFilename = args.videoFilename+'.csv'
else:
    outputFilename = args.outputFilename
vehNumber = 0
lineNum = -1
out = open(outputFilename, 'a')

while(cap.isOpened()):
    ret, frame = cap.read()
    frameNum = int(cap.get(cv2.CAP_PROP_POS_FRAMES))
    cv2.putText(frame, str(frameNum), (1,20), cv2.FONT_HERSHEY_PLAIN, 1, (255, 0,0))
    cv2.imshow('Video',frame)

    key= cv2.waitKey(0)

    if key == ord('q'):
        break
    else:
        config = UserConfiguration.getConfigurationWithKey(userConfigurations, key)
        if config is not None:
            if config.isKeyNew(key):
                config.userNum += 1
                config.resetAttributes()
                print('New {} {}'.format(config.name, config.userNum))
                if args.nAttributes > 0:
                    key2 = ord('1')
                    cv2.namedWindow('Input', cv2.WINDOW_NORMAL)
                    attributeNum = 0
                    while key2 != ord('s'):
                        attrImg = 255*np.ones((20*args.nAttributes, 20, 3))
                        for i in range(args.nAttributes):
                            if i == (attributeNum%args.nAttributes):
                                cv2.putText(attrImg, str(config.attributes[i]), (1,20*(i+1)), cv2.FONT_HERSHEY_PLAIN, 1, (0, 0, 255))
                            else:
                                cv2.putText(attrImg, str(config.attributes[i]), (1,20*(i+1)), cv2.FONT_HERSHEY_PLAIN, 1, (0, 255, 0))
                        cv2.imshow('Input', attrImg)
                        key2 = cv2.waitKey(0)
                        if chr(key2).isdigit():
                            config.setAttribute(attributeNum, chr(key2))
                            attributeNum += 1
                    cv2.destroyWindow('Input')
            elif config.isKeyAddInstant(key):
                config.userInstant += 1
                print('User {} no {} at line {}'.format(config.name, config.userNum, config.userInstant))
            out.write('{},{},{}{},{}\n'.format(config.userNum, config.name, config.getAttributeStr(), config.userInstant, frameNum))
            oldUserConfig = config
        if key == ord('o'):
            print('SKIPPED')
            out.write('{},{},SKIP\n'.format(oldUserConfig.userNum, oldUserConfig.name))
        elif key == ord('d'):
            cap.set(cv2.CAP_PROP_POS_FRAMES,frameNum+100)
        elif key == ord('s'):
            cap.set(cv2.CAP_PROP_POS_FRAMES,frameNum+10)
        elif key == ord('a'):
            cap.set(cv2.CAP_PROP_POS_FRAMES,frameNum+1)
        elif key == ord('x'):
            cap.set(cv2.CAP_PROP_POS_FRAMES,frameNum-10)
        elif key == ord('c'):
            cap.set(cv2.CAP_PROP_POS_FRAMES,frameNum-100)
        elif key == ord('l'):
            frameNum = int(input("Please enter the frame number you would like to skip to\n"))
            cap.set(cv2.CAP_PROP_POS_FRAMES,frameNum)
    
out.close()
cap.release()
cv2.destroyAllWindows()

#97a
#115s
#100d
#102f
#103g
#104h
#106j
#107k
#108l
