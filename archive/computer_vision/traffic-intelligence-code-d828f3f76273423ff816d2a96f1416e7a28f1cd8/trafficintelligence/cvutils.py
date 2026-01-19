#! /usr/bin/env python
'''Image/Video utilities'''

from sys import stdout
from os import listdir
from subprocess import run
from math import floor, log10, ceil, inf
from time import time

from numpy import dot, array, append, float32, loadtxt, savetxt, append, zeros, ones, identity, abs as npabs, logical_and, unravel_index, sum as npsum, isnan, mgrid, median, floor as npfloor, ceil as npceil, nonzero, dtype
from numpy.linalg import inv
from matplotlib.pyplot import imread, imsave, imshow, figure, subplot

try:
    import cv2
    opencvAvailable = True
except ImportError:
    print('OpenCV library could not be loaded (video replay functions will not be available)') # TODO change to logging module
    opencvAvailable = False
try:
    import skimage
    skimageAvailable = True
except ImportError:
    print('Scikit-image library could not be loaded (HoG-based classification methods will not be available)')
    skimageAvailable = False
    
from trafficintelligence import utils, moving

videoFilenameExtensions = ['mov', 'avi', 'mp4', 'MOV', 'AVI', 'MP4']
trackerExe = 'feature-based-tracking'
#importaggdraw # agg on top of PIL (antialiased drawing)

colors = 'rgbcymk'
cvRed = {'default': (0,0,255),
         'colorblind': (0,114,178)}
cvGreen = {'default': (0,255,0),
           'colorblind': (0,158,115)}
cvBlue = {'default': (255,0,0),
          'colorblind': (213,94,0)}
cvCyan = {'default': (255, 255, 0),
          'colorblind': (240,228,66)}
cvYellow = {'default': (0, 255, 255),
            'colorblind': (86,180,233)}
cvMagenta = {'default': (255, 0, 255),
             'colorblind': (204,121,167)}
cvWhite = {k: (255, 255, 255) for k in ['default', 'colorblind']}
cvBlack = {k: (0,0,0) for k in ['default', 'colorblind']}

cvColors3 = {k: utils.PlottingPropertyValues([cvRed[k], cvGreen[k], cvBlue[k]]) for k in ['default', 'colorblind']}
cvColors = {k: utils.PlottingPropertyValues([cvRed[k], cvGreen[k], cvBlue[k], cvCyan[k], cvYellow[k], cvMagenta[k], cvWhite[k], cvBlack[k]]) for k in ['default', 'colorblind']}

def quitKey(key):
    return chr(key&255)== 'q' or chr(key&255) == 'Q'

def saveKey(key):
    return chr(key&255) == 's'

def int2FOURCC(x):
    fourcc = ''
    for i in range(4):
        fourcc += chr((x >> 8*i)&255)
    return fourcc

def rgb2gray(rgb):
    return dot(rgb[...,:3], [0.299, 0.587, 0.144])

def matlab2PointCorrespondences(filename):
    '''Loads and converts the point correspondences saved 
    by the matlab camera calibration tool'''
    points = loadtxt(filename, delimiter=',')
    savetxt(utils.removeExtension(filename)+'-point-correspondences.txt',append(points[:,:2].T, points[:,3:].T, axis=0))

def loadPointCorrespondences(filename):
    '''Loads and returns the corresponding points in world (first 2 lines) and image spaces (last 2 lines)'''
    points = loadtxt(filename, dtype=dtype('float32'))
    return  (points[:2,:].T, points[2:,:].T) # (world points, image points)

def cvMatToArray(cvmat):
    '''Converts an OpenCV CvMat to numpy array.'''
    print('Deprecated, use new interface')
    a = zeros((cvmat.rows, cvmat.cols))#array([[0.0]*cvmat.width]*cvmat.height)
    for i in range(cvmat.rows):
        for j in range(cvmat.cols):
            a[i,j] = cvmat[i,j]
    return a

def createWhiteImage(height, width, filename):
    img = ones((height, width, 3), uint8)*255
    imsave(filename, img)

if opencvAvailable:
    def computeHomography(srcPoints, dstPoints, method=0, ransacReprojThreshold=3.0):
        '''Returns the homography matrix mapping from srcPoints to dstPoints (dimension Nx2)'''
        H, mask = cv2.findHomography(srcPoints, dstPoints, method, ransacReprojThreshold)
        return H

    def cvPlot(img, positions, color, lastCoordinate = None, **kwargs):
        if lastCoordinate is None:
            last = positions.length()-1
        elif lastCoordinate >=0:
            last = min(positions.length()-1, lastCoordinate)
        for i in range(0, last):
            cv2.line(img, positions[i].asint().astuple(), positions[i+1].asint().astuple(), color, **kwargs)

    def cvImshow(windowName, img, rescale = 1.0):
        'Rescales the image (in particular if too large)'
        if rescale != 1.:
            size = (int(round(img.shape[1]*rescale)), int(round(img.shape[0]*rescale)))
            resizedImg = cv2.resize(img, size)
            cv2.imshow(windowName, resizedImg)
        else:
            cv2.imshow(windowName, img)

    def computeUndistortMaps(width, height, undistortedImageMultiplication, intrinsicCameraMatrix, distortionCoefficients):
        newImgSize = (int(round(width*undistortedImageMultiplication)), int(round(height*undistortedImageMultiplication)))
        newCameraMatrix = cv2.getDefaultNewCameraMatrix(intrinsicCameraMatrix, newImgSize, True)
        return cv2.initUndistortRectifyMap(intrinsicCameraMatrix, array(distortionCoefficients), None, newCameraMatrix, newImgSize, cv2.CV_32FC1), newCameraMatrix

    def playVideo(filenames, windowNames = None, firstFrameNums = None, frameRate = -1, interactive = False, printFrames = True, text = None, rescale = 1., step = 1, colorBlind = False):
        '''Plays the video(s)'''
        if colorBlind:
            colorType = 'colorblind'
        else:
            colorType = 'default'
        if len(filenames) == 0:
            print('Empty filename list')
            return
        if windowNames is None:
            windowNames = ['frame{}'.format(i) for i in range(len(filenames))]
        wait = 5
        if rescale == 1.:
            for windowName in windowNames:
                cv2.namedWindow(windowName, cv2.WINDOW_NORMAL)
        if frameRate > 0:
            wait = int(round(1000./frameRate))
        if interactive:
            wait = 0
        captures = [cv2.VideoCapture(fn) for fn in filenames]
        if array([cap.isOpened() for cap in captures]).all():
            key = -1
            ret = True
            nFramesShown = 0
            if firstFrameNums is not None:
                for i in range(len(captures)):
                    captures[i].set(cv2.CAP_PROP_POS_FRAMES, firstFrameNums[i])
            while ret and not quitKey(key):
                rets = []
                images = []
                for cap in captures:
                    ret, img = cap.read()
                    rets.append(ret)
                    images.append(img)
                ret = array(rets).all()
                if ret:
                    if printFrames:
                        print('frame shown {0}'.format(nFramesShown))
                    for i in range(len(filenames)):
                        if text is not None:
                            cv2.putText(images[i], text, (10,50), cv2.FONT_HERSHEY_PLAIN, 1, cvRed[colorType])
                        cvImshow(windowNames[i], images[i], rescale) # cv2.imshow('frame', img)
                    key = cv2.waitKey(wait)
                    if saveKey(key):
                        cv2.imwrite('image-{}.png'.format(frameNum), img)
                    nFramesShown += step
                    if step > 1:
                        for i in range(len(captures)):
                            captures[i].set(cv2.CAP_PROP_POS_FRAMES, firstFrameNums[i]+nFramesShown)
            cv2.destroyAllWindows()
        else:
            print('Video captures for {} failed'.format(filenames))

    def infoVideo(filename):
        '''Provides all available info on video '''
        cvPropertyNames = {cv2.CAP_PROP_FORMAT: "format",
                           cv2.CAP_PROP_FOURCC: "codec (fourcc)",
                           cv2.CAP_PROP_FPS: "fps",
                           cv2.CAP_PROP_FRAME_COUNT: "number of frames",
                           cv2.CAP_PROP_FRAME_HEIGHT: "heigh",
                           cv2.CAP_PROP_FRAME_WIDTH: "width",
                           cv2.CAP_PROP_RECTIFICATION: "rectification",
                           cv2.CAP_PROP_SATURATION: "saturation"}
        capture = cv2.VideoCapture(filename)
        videoProperties = {}
        if capture.isOpened():
            for cvprop in [#cv2.CAP_PROP_BRIGHTNESS
                    #cv2.CAP_PROP_CONTRAST
                    #cv2.CAP_PROP_CONVERT_RGB
                    #cv2.CAP_PROP_EXPOSURE
                    cv2.CAP_PROP_FORMAT,
                    cv2.CAP_PROP_FOURCC,
                    cv2.CAP_PROP_FPS,
                    cv2.CAP_PROP_FRAME_COUNT,
                    cv2.CAP_PROP_FRAME_HEIGHT,
                    cv2.CAP_PROP_FRAME_WIDTH,
                    #cv2.CAP_PROP_GAIN,
                    #cv2.CAP_PROP_HUE
                    #cv2.CAP_PROP_MODE
                    #cv2.CAP_PROP_POS_AVI_RATIO
                    #cv2.CAP_PROP_POS_FRAMES
                    #cv2.CAP_PROP_POS_MSEC
                    #cv2.CAP_PROP_RECTIFICATION,
                    #cv2.CAP_PROP_SATURATION
            ]:
                prop = capture.get(cvprop)
                if cvprop == cv2.CAP_PROP_FOURCC and prop > 0:
                    prop = int2FOURCC(int(prop))
                videoProperties[cvPropertyNames[cvprop]] = prop
        else:
            print('Video capture for {} failed'.format(filename))
        return videoProperties

    def getImagesFromVideo(videoFilename, firstFrameNum = 0, lastFrameNum = 1, step = 1, saveImage = False, outputPrefix = 'image'):
        '''Returns nFrames images from the video sequence'''
        images = []
        capture = cv2.VideoCapture(videoFilename)
        if capture.isOpened():
            rawCount = capture.get(cv2.CAP_PROP_FRAME_COUNT)
            if rawCount < 0:
                rawCount = lastFrameNum+1
            nDigits = int(floor(log10(rawCount)))+1
            ret = False
            capture.set(cv2.CAP_PROP_POS_FRAMES, firstFrameNum)
            frameNum = firstFrameNum
            while frameNum<lastFrameNum and frameNum<rawCount:
                ret, img = capture.read()
                i = 0
                while not ret and i<10:
                    ret, img = capture.read()
                    i += 1
                if img is not None and img.size>0:
                    if saveImage:
                        frameNumStr = format(frameNum, '0{}d'.format(nDigits))
                        cv2.imwrite(outputPrefix+frameNumStr+'.png', img)
                    else:
                        images.append(img)
                    frameNum +=step
                    if step > 1:
                        capture.set(cv2.CAP_PROP_POS_FRAMES, frameNum)
            capture.release()
        else:
            print('Video capture for {} failed'.format(videoFilename))
        return images
    
    def getFPS(videoFilename):
        capture = cv2.VideoCapture(videoFilename)
        if capture.isOpened():
            fps = capture.get(cv2.CAP_PROP_FPS)
            capture.release()
            return fps
        else:
            print('Video capture for {} failed'.format(videoFilename))
            return None
        
    def imageBoxSize(obj, frameNum, width, height, px = 0.2, py = 0.2):
        'Computes the bounding box size of object at frameNum'
        x = []
        y = []
        if obj.hasFeatures():
            for f in obj.getFeatures():
                if f.existsAtInstant(frameNum):
                    p = f.getPositionAtInstant(frameNum)
                    x.append(p.x)
                    y.append(p.y)
        xmin = min(x)
        xmax = max(x)
        ymin = min(y)
        ymax = max(y)
        xMm = px * (xmax - xmin)
        yMm = py * (ymax - ymin)
        a = max(ymax - ymin + (2 * yMm), xmax - xmin + (2 * xMm))
        yCropMin = int(max(0, .5 * (ymin + ymax - a)))
        yCropMax = int(min(height - 1, .5 * (ymin + ymax + a)))
        xCropMin = int(max(0, .5 * (xmin + xmax - a)))
        xCropMax = int(min(width - 1, .5 * (xmin + xmax + a)))
        return yCropMin, yCropMax, xCropMin, xCropMax
        
    def imageBox(img, obj, frameNum, width, height, px = 0.2, py = 0.2, minNPixels = 800):
        'Computes the bounding box of object at frameNum'
        yCropMin, yCropMax, xCropMin, xCropMax = imageBoxSize(obj, frameNum, width, height, px, py)
        if yCropMax > yCropMin and xCropMax > xCropMin and (yCropMax - yCropMin) * (xCropMax - xCropMin) > minNPixels:
            return img[yCropMin : yCropMax, xCropMin : xCropMax]
        else:
            return None

    def tracking(configFilename, grouping, videoFilename = None, dbFilename = None, homographyFilename = None, maskFilename = None, undistort = False, intrinsicCameraMatrix = None, distortionCoefficients = None, dryRun = False):
        '''Runs the tracker in a subprocess
        if grouping is True, it is feature grouping
        otherwise it is feature tracking'''
        if grouping:
            trackingMode = '--gf'
        else:
            trackingMode = '--tf'
        cmd = [trackerExe, configFilename, trackingMode, '--quiet']
        
        if videoFilename is not None:
            cmd += ['--video-filename', videoFilename]
        if dbFilename is not None:
            cmd += ['--database-filename', dbFilename]
        if homographyFilename is not None:
            cmd += ['--homography-filename', homographyFilename]
        if maskFilename is not None:
            cmd += ['--mask-filename', maskFilename]
        if undistort:
            cmd += ['--undistort', 'true']
            if intrinsicCameraMatrix is not None: # we currently have to save a file
                intrinsicCameraFilename = '/tmp/intrinsic-{}.txt'.format(time())
                savetxt(intrinsicCameraFilename, intrinsicCameraMatrix)
                cmd += ['--intrinsic-camera-filename', intrinsicCameraFilename]
            if distortionCoefficients is not None:
                cmd += ['--distortion-coefficients '+' '.join([str(x) for x in distortionCoefficients])]
        if dryRun:
            print(cmd)
        else:
            run(cmd)
        
    def displayTrajectories(videoFilename, objects, boundingBoxes = {}, homography = None, firstFrameNum = 0, lastFrameNum = None, printFrames = True, rescale = 1., nFramesStep = 1, saveAllImages = False, nZerosFilenameArg = None, undistort = False, intrinsicCameraMatrix = None, distortionCoefficients = None, undistortedImageMultiplication = 1., annotations = [], gtMatches = {}, toMatches = {}, colorBlind = False, **kwargs):
        '''Displays the objects overlaid frame by frame over the video '''
        if colorBlind:
            colorType = 'colorblind'
        else:
            colorType = 'default'

        capture = cv2.VideoCapture(videoFilename)
        width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

        windowName = 'frame'
        if rescale == 1.:
            cv2.namedWindow(windowName, cv2.WINDOW_NORMAL)

        if undistort: # setup undistortion
            [map1, map2], newCameraMatrix = computeUndistortMaps(width, height, undistortedImageMultiplication, intrinsicCameraMatrix, distortionCoefficients)
        if capture.isOpened():
            key = -1
            ret = True
            frameNum = firstFrameNum
            capture.set(cv2.CAP_PROP_POS_FRAMES, firstFrameNum)
            if nZerosFilenameArg is None:
                if lastFrameNum is None or lastFrameNum == inf:
                    nZerosFilename = int(ceil(log10(objects[-1].getLastInstant())))
                else:
                    nZerosFilename = int(ceil(log10(lastFrameNum)))
            else:
                nZerosFilename = nZerosFilenameArg
            while ret and not quitKey(key) and frameNum <= lastFrameNum:
                ret, img = capture.read()
                if ret:
                    if undistort:
                        img = cv2.remap(img, map1, map2, interpolation=cv2.INTER_LINEAR)
                    if printFrames:
                        print('frame {0}'.format(frameNum))
                    # plot objects
                    for obj in objects[:]:
                        if obj.existsAtInstant(frameNum):
                            if not hasattr(obj, 'projectedPositions'):
                                obj.projectedPositions = obj.getPositions().homographyProject(homography)
                                if undistort:
                                    obj.projectedPositions = obj.projectedPositions.newCameraProject(newCameraMatrix)
                            cvPlot(img, obj.projectedPositions, cvColors[colorType][obj.getNum()], frameNum-obj.getFirstInstant(), **kwargs)
                            if frameNum not in boundingBoxes and obj.hasFeatures():
                                yCropMin, yCropMax, xCropMin, xCropMax = imageBoxSize(obj, frameNum, homography, width, height)
                                cv2.rectangle(img, (xCropMin, yCropMin), (xCropMax, yCropMax), cvBlue[colorType], 1)
                            objDescription = '{} '.format(obj.num)
                            if moving.userTypeNames[obj.userType] != 'unknown':
                                s = moving.userTypeNames[obj.userType]
                                objDescription += s[0].upper()+s[1]
                            if len(annotations) > 0: # if we loaded annotations, but there is no match
                                if frameNum not in toMatches[obj.getNum()]:
                                    objDescription += " FA"
                            cv2.putText(img, objDescription, obj.projectedPositions[frameNum-obj.getFirstInstant()].asint().astuple(), cv2.FONT_HERSHEY_PLAIN, 1, cvColors[colorType][obj.getNum()])
                        if obj.getLastInstant() == frameNum:
                            objects.remove(obj)
                    # plot object bounding boxes
                    if frameNum in boundingBoxes:
                        for rect in boundingBoxes[frameNum]:
                            cv2.rectangle(img, rect[0].asint().astuple(), rect[1].asint().astuple(), cvColors[colorType][obj.getNum()])
                    # plot ground truth
                    if len(annotations) > 0:
                        for gt in annotations:
                            if gt.existsAtInstant(frameNum):
                                if frameNum in gtMatches[gt.getNum()]:
                                    color = cvColors[colorType][gtMatches[gt.getNum()][frameNum]] # same color as object
                                else:
                                    color = cvRed[colorType]
                                    cv2.putText(img, 'Miss', gt.topLeftPositions[frameNum-gt.getFirstInstant()].asint().astuple(), cv2.FONT_HERSHEY_PLAIN, 1, color)
                                cv2.rectangle(img, gt.topLeftPositions[frameNum-gt.getFirstInstant()].asint().astuple(), gt.bottomRightPositions[frameNum-gt.getFirstInstant()].asint().astuple(), color)
                    # saving images and going to next
                    if not saveAllImages:
                        cvImshow(windowName, img, rescale)
                        key = cv2.waitKey()
                    if saveAllImages or saveKey(key):
                        cv2.imwrite('image-{{:0{}}}.png'.format(nZerosFilename).format(frameNum), img)
                    frameNum += nFramesStep
                    if nFramesStep > 1:
                        capture.set(cv2.CAP_PROP_POS_FRAMES, frameNum)
            cv2.destroyAllWindows()
        else:
            print('Cannot load file ' + videoFilename)

    def computeHomographyFromPDTV(camera):
        '''Returns the homography matrix at ground level from PDTV camera
        https://bitbucket.org/hakanardo/pdtv'''
        # camera = pdtv.load(cameraFilename)
        srcPoints = [[x,y] for x, y in zip([1.,2.,2.,1.],[1.,1.,2.,2.])] # need floats!!
        dstPoints = []
        for srcPoint in srcPoints:
            projected = camera.image_to_world(tuple(srcPoint))
            dstPoints.append([projected[0], projected[1]])
        H, mask = cv2.findHomography(array(srcPoints), array(dstPoints), method = 0) # No need for different methods for finding homography
        return H

    def getIntrinsicCameraMatrix(cameraData):
        return array([[cameraData['f']*cameraData['Sx']/cameraData['dx'], 0, cameraData['Cx']],
                      [0, cameraData['f']/cameraData['dy'], cameraData['Cy']],
                      [0, 0, 1.]])

    def getDistortionCoefficients(cameraData):
        return array([cameraData['k']]+4*[0])
    
    def undistortedCoordinates(map1, map2, x, y, maxDistance = 1.):
        '''Returns the coordinates of a point in undistorted image
        map1 and map2 are the mapping functions from undistorted image
        to distorted (original image)
        map1(x,y) = originalx, originaly'''
        distx = npabs(map1-x)
        disty = npabs(map2-y)
        indices = logical_and(distx<maxDistance, disty<maxDistance)
        closeCoordinates = unravel_index(nonzero(indices), distx.shape) # returns i,j, ie y,x
        xWeights = 1-distx[indices]
        yWeights = 1-disty[indices]
        return dot(xWeights, closeCoordinates[1])/npsum(xWeights), dot(yWeights, closeCoordinates[0])/npsum(yWeights)

    def undistortTrajectoryFromCVMapping(map1, map2, t):
        '''test 'perfect' inversion'''
        undistortedTrajectory = moving.Trajectory()
        for i,p in enumerate(t):
            res = undistortedCoordinates(map1, map2, p.x,p.y)
            if not isnan(res).any():
                undistortedTrajectory.addPositionXY(res[0], res[1])
            else:
                print('{} {} {}'.format(i,p,res))
        return undistortedTrajectory

    def computeInverseMapping(originalImageSize, map1, map2):
        'Computes inverse mapping from maps provided by cv2.initUndistortRectifyMap'
        invMap1 = -ones(originalImageSize)
        invMap2 = -ones(originalImageSize)
        for x in range(0,originalImageSize[1]):
            for y in range(0,originalImageSize[0]):
                res = undistortedCoordinates(x,y, map1, map2)
                if not isnan(res).any():
                    invMap1[y,x] = res[0]
                    invMap2[y,x] = res[1]
        return invMap1, invMap2

    def intrinsicCameraCalibration(path, checkerBoardSize=[6,7], secondPassSearch=False, display=False, fixK2 = True, fixK3 = True, zeroTangent = True):
        ''' Camera calibration searches through all the images (jpg or png) located
        in _path_ for matches to a checkerboard pattern of size checkboardSize.
        These images should all be of the same camera with the same resolution.
        
        For best results, use an asymetric board and ensure that the image has
        very high contrast, including the background. 

        cherckerBoardSize is the number of internal corners (7x10 squares have 6x9 internal corners) 
        
        The code below is based off of:
        https://opencv-python-tutroals.readthedocs.org/en/latest/py_tutorials/py_calib3d/py_calibration/py_calibration.html
        Modified by Paul St-Aubin
        '''
        import glob, os

        # termination criteria
        criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 30, 0.001)

        # prepare object points, like (0,0,0), (1,0,0), (2,0,0) ....,(6,5,0)
        objp = zeros((checkerBoardSize[0]*checkerBoardSize[1],3), float32)
        objp[:,:2] = mgrid[0:checkerBoardSize[1],0:checkerBoardSize[0]].T.reshape(-1,2)

        # Arrays to store object points and image points from all the images.
        objpoints = [] # 3d point in real world space
        imgpoints = [] # 2d points in image plane.

        ## Loop throuhg all images in _path_
        images = glob.glob(os.path.join(path,'*.[jJ][pP][gG]'))+glob.glob(os.path.join(path,'*.[jJ][pP][eE][gG]'))+glob.glob(os.path.join(path,'*.[pP][nN][gG]'))
        for fname in images:
            img = cv2.imread(fname)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

            # Find the chess board corners
            ret, corners = cv2.findChessboardCorners(gray, (checkerBoardSize[1],checkerBoardSize[0]), None)

            # If found, add object points, image points (after refining them)
            if ret:
                print('Found pattern in '+fname)
                
                if secondPassSearch:
                    corners = cv2.cornerSubPix(gray, corners, (11,11), (-1,-1), criteria)

                objpoints.append(objp)
                imgpoints.append(corners)

                # Draw and display the corners
                if display:
                    cv2.drawChessboardCorners(img, (checkerBoardSize[1],checkerBoardSize[0]), corners, ret)
                    if img is not None:
                        cv2.imshow('img',img)
                        cv2.waitKey(0)
            else:
                print('Pattern not found in '+fname)
        ## Close up image loading and calibrate
        cv2.destroyAllWindows()
        if len(objpoints) == 0 or len(imgpoints) == 0: 
            return None
        try:
            flags = 0
            if fixK2:
                flags += cv2.CALIB_FIX_K2
            if fixK3:
                flags += cv2.CALIB_FIX_K3
            if zeroTangent:
                flags += cv2.CALIB_ZERO_TANGENT_DIST
            ret, camera_matrix, dist_coeffs, rvecs, tvecs = cv2.calibrateCamera(objpoints, imgpoints, gray.shape[::-1], None, None, flags = flags)
        except NameError:
            return None
        savetxt('intrinsic-camera.txt', camera_matrix)
        print('error: {}'.format(ret))
        return camera_matrix, dist_coeffs

    def undistortImage(img, intrinsicCameraMatrix = None, distortionCoefficients = None, undistortedImageMultiplication = 1., interpolation=cv2.INTER_LINEAR):
        '''Undistorts the image passed in argument'''
        width = img.shape[1]
        height = img.shape[0]
        [map1, map2] = computeUndistortMaps(width, height, undistortedImageMultiplication, intrinsicCameraMatrix, distortionCoefficients)
        return cv2.remap(img, map1, map2, interpolation=interpolation)

def cartesian2Homogeneous(m):
    'Transforms n x m matrix to n x (m+1) by adding last column of 1s'
    if len(m.shape) == 2:
        homoM = ones((m.shape[0], m.shape[1]+1))
    else:
        homoM = ones((1, len(m)+1))
    homoM[:,:-1] = m
    return homoM
    
def homographyProject(points, homography, output3D = False):
    '''Returns the coordinates of the points (2xN array) projected through homography'''
    if points.shape[0] != 2:
        raise Exception('points of dimension {}'.format(points.shape))

    if homography is not None and homography.size>0:
        if output3D:
            outputDim = 3
        else:
            outputDim = 2
        augmentedPoints = append(points,[[1]*points.shape[1]], 0) # 3xN
        prod = dot(homography, augmentedPoints)
        return prod[:outputDim,:]/prod[2]
    elif output3D:
        return append(points,[[1]*points.shape[1]], 0) # 3xN
    else:
        return points

def imageToWorldProject(points, intrinsicCameraMatrix = None, distortionCoefficients = None, homography = None):
    '''Projects points (2xN array) from image (video) space to world space
    1. through undistorting if provided by intrinsic camera matrix and distortion coefficients
    2. through homograph projection (from ideal point (no camera) to world)'''
    if points.shape[0] != 2:
        raise Exception('points of dimension {}'.format(points.shape))
    if points.dtype != dtype('float64'):
        points = array(points, dtype = dtype('float64'))
        
    if intrinsicCameraMatrix is not None and distortionCoefficients is not None:
        undistortedPoints = cv2.undistortPoints(points.T.reshape(1,points.shape[1], 2), intrinsicCameraMatrix, distortionCoefficients).reshape(-1,2)
        return homographyProject(undistortedPoints.T, homography)
    else:
        return homographyProject(points, homography)

def worldToImageProject(points, intrinsicCameraMatrix = None, distortionCoefficients = None, homography = None):
    '''Projects points (2xN array) from world space to image (video) space
    1. through undistorting if provided by intrinsic camera matrix and distortion coefficients
    2. through homograph projection (from ideal point (no camera) to world)

    Warning: this usually means to invert the homography if computer from image (ideal point) to world space, eg using numpy.linalg.inv'''
    if points.shape[0] != 2:
        raise Exception('points of dimension {}'.format(points.shape))

    if intrinsicCameraMatrix is not None and distortionCoefficients is not None:
        projected3D = homographyProject(points, homography, True)
        projected, jacobian = cv2.projectPoints(projected3D.T, (0.,0.,0.), (0.,0.,0.), intrinsicCameraMatrix, distortionCoefficients) # in: 3xN, out: 2x1xN
        return projected.reshape(-1,2).T
    else:
        return homographyProject(points, homography)
    
def newCameraProject(points, newCameraMatrix):
    '''Projects points (2xN array) as if seen by camera
    (or reverse by inverting the camera matrix)'''
    if points.shape[0] != 2:
        raise Exception('points of dimension {}'.format(points.shape))

    if newCameraMatrix is not None:
        augmentedPoints = append(points,[[1]*points.shape[1]], 0) # 3xN
        projected = dot(newCameraMatrix, augmentedPoints)
        return projected[:2,:]
    else:
        return points

if opencvAvailable:
    def computeTranslation(img1, img2, img1Points, maxTranslation2, minNMatches, windowSize = (5,5), level = 5, criteria = (cv2.TERM_CRITERIA_EPS, 0, 0.01)):
        '''Computes the translation of img2 with respect to img1
        (loaded using OpenCV as numpy arrays)
        img1Points are used to compute the translation

        TODO add diagnostic if data is all over the place, and it most likely is not a translation (eg zoom, other non linear distortion)'''

        nextPoints = array([])
        (img2Points, status, track_error) = cv2.calcOpticalFlowPyrLK(img1, img2, img1Points, nextPoints, winSize=windowSize, maxLevel=level, criteria=criteria)
        # calcOpticalFlowPyrLK(prevImg, nextImg, prevPts[, nextPts[, status[, err[, winSize[, maxLevel[, criteria[, derivLambda[, flags]]]]]]]]) -> nextPts, status, err
        delta = []
        for (k, (p1,p2)) in enumerate(zip(img1Points, img2Points)):
            if status[k] == 1:
                dp = p2-p1
                d = npsum(dp**2)
                if d < maxTranslation2:
                    delta.append(dp)
        if len(delta) >= minNMatches:
            return median(delta, axis=0)
        else:
            print(dp)
            return None

if skimageAvailable:
    from skimage.feature import hog
    from skimage import color, transform
    
    def HOG(image, rescaleSize = (64, 64), orientations = 9, pixelsPerCell = (8,8), cellsPerBlock = (2,2), blockNorm = 'L1', visualize = False, transformSqrt = False):
        bwImg = color.rgb2gray(image)
        inputImg = transform.resize(bwImg, rescaleSize)
        features = hog(inputImg, orientations, pixelsPerCell, cellsPerBlock, blockNorm, visualize, transformSqrt, True)
        if visualize:
            hogViz = features[1]
            features = features[0]
            figure()
            subplot(1,2,1)
            imshow(inputImg)
            subplot(1,2,2)
            imshow(hogViz)
        return float32(features)

    def createHOGTrainingSet(imageDirectory, classLabel, rescaleSize = (64,64), orientations = 9, pixelsPerCell = (8,8), blockNorm = 'L1', cellsPerBlock = (2, 2), visualize = False, transformSqrt = False):
        inputData = []
        for filename in listdir(imageDirectory):
            img = imread(imageDirectory+filename)
            features = HOG(img, rescaleSize, orientations, pixelsPerCell, cellsPerBlock, blockNorm, visualize, transformSqrt)
            inputData.append(features)

        nImages = len(inputData)
        return array(inputData, dtype = dtype('float32')), array([classLabel]*nImages)

        
#########################
# running tests
#########################

if __name__ == "__main__":
    import doctest
    import unittest
    suite = doctest.DocFileSuite('tests/cvutils.txt')
    #suite = doctest.DocTestSuite()
    unittest.TextTestRunner().run(suite)
    #doctest.testmod()
    #doctest.testfile("example.txt")
