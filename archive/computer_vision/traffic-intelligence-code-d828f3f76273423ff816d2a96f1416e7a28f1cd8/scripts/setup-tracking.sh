version="$(wget -q -O - http://sourceforge.net/projects/opencvlibrary/files/opencv-unix | egrep -m1 -o '\"[0-2](\.[0-9]+)+' | cut -c2-)"
#'\"[0-9](\.[0-9])+'
echo "Removing any pre-installed ffmpeg and x264"
sudo apt -qq remove ffmpeg x264 libx264-dev
echo "Installing Dependencies"
sudo apt -qq install build-essential checkinstall cmake pkg-config yasm libtiff5-dev libjpeg-dev libjasper-dev libavcodec-dev libavformat-dev libswscale-dev libgstreamer0.10-dev libgstreamer-plugins-base0.10-dev libv4l-dev python-dev libtbb-dev libgtk2.0-dev libfaac-dev libmp3lame-dev libtheora-dev libvorbis-dev libxvidcore-dev x264
#  libdc1394-22-dev libxine-dev python-numpy libqt4-dev libopencore-amrnb-dev libopencore-amrwb-dev v4l-utils ffmpeg libboost-all-dev
sudo apt -qq install libavfilter-dev libboost-dev libboost-program-options-dev libboost-graph-dev python-pip sqlite3 libsqlite3-dev cmake-qt-gui libgeos-dev
echo "Installing OpenCV" $version
cd
mkdir OpenCV
cd OpenCV
echo "Downloading OpenCV" $version
wget -O OpenCV-$version.zip http://sourceforge.net/projects/opencvlibrary/files/opencv-unix/$version/opencv-"$version".zip/download
echo "Installing OpenCV" $version
unzip OpenCV-$version.tar.gz
#tar -xvf
cd opencv-$version
mkdir release
cd release
cmake -D CMAKE_BUILD_TYPE=RELEASE -D CMAKE_INSTALL_PREFIX=/usr/local ..
make
sudo make -j4 install
echo "OpenCV" $version "ready to be used"

echo "Installing Traffic Intelligence..."
cd
mkdir Research
cd Research
mkdir Code
cd Code
hg clone https://Nicolas@bitbucket.org/trajectories/trajectorymanagementandanalysis
hg clone https://Nicolas@bitbucket.org/Nicolas/trafficintelligence
cd trajectorymanagementandanalysis/trunk/src/TrajectoryManagementAndAnalysis/
cmake .
make TrajectoryManagementAndAnalysis
cd
wget https://bootstrap.pypa.io/get-pip.py
sudo -H python3 get-pip.py
sudo -H pip3 install -r trafficintelligence/python-requirements.txt --upgrade
cd trafficintelligence/c/
make feature-based-tracking
cd

