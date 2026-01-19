CMAKE_MINIMUM_REQUIRED( VERSION 3.1 )

PROJECT( TrajectoryManagementAndAnalysis )
FIND_PACKAGE(OpenCV REQUIRED)

FIND_LIBRARY(SQLite3_LIBS sqlite3)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

#SET(CMAKE_CXX_FLAGS "-g -Wall -DNDEBUG -fpermissive -std=c++11")

include_directories( ${OpenCV_INCLUDE_DIRS} )

ADD_LIBRARY(TrajectoryManagementAndAnalysis src/DBSQLiteAccess.cpp)
