#!/bin/sh
echo "------------"
echo "Python tests"
cd trafficintelligence
./run-tests.sh
cd ..
echo "------------"
echo "C++ tests"
if [ -f ./bin/tests ]
then
    ./bin/tests
else
    echo "The test executable has not been compiled"
fi
echo "------------"
echo "Script tests"
./scripts/run-tests.sh
echo "------------"
echo "Documentation tests"
TIWIKI_HOME=~/Research/Code/bb-traffic-intelligence/
if [ -f $TIWIKI_HOME/docs/run-tests.sh ]
then
    cd $TIWIKI_HOME/docs/
    ./run-tests.sh
else
    echo "The documentation repository (BitBucket Traffic Intelligence repository is not accessible"
    echo "Please clone the BitBucket Traffic Intelligence repository:"
    echo "$ git clone https://bitbucket.org/Nicolas/trafficintelligence.git"
fi
