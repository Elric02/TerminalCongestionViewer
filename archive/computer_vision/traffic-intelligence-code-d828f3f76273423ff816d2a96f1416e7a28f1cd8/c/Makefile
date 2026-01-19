EXE_DIR=../bin
SCRIPTS_DIR=../scripts
TRAJECTORYMANAGEMENT_DIR=../trajectorymanagement

CXX = g++

INCLUDE = -I../include -I$(TRAJECTORYMANAGEMENT_DIR)

LIBS = -L../../klt -L$(TRAJECTORYMANAGEMENT_DIR)

#LDFLAGS = -Wl,-Bstatic -lm
LDFLAGS = -lm
LDFLAGS += -lTrajectoryManagementAndAnalysis -lsqlite3
LDFLAGS += -lboost_program_options -lboost_filesystem -lboost_system
#LDFLAGS += -lfltk

CFLAGS = -Wall -W -Wextra -std=c++11
UNAME = $(shell uname)

ifneq ($(OPENCV), 0)
	CFLAGS += -DUSE_OPENCV
	LDFLAGS += -lopencv_highgui -lopencv_core -lopencv_video -lopencv_features2d -lopencv_imgproc -lopencv_imgcodecs -lopencv_videoio -lopencv_calib3d
endif

#LDFLAGS += -Wl,--as-needed -Wl,-Bdynamic,-lgcc_s,-Bstatic

ifeq ($(UNAME), Linux)
	INCLUDE+= -I/usr/include/opencv4
	LIBS += -L/usr/lib
#	LINUX_BOOST_PREFIX = /usr/local
	CFLAGS += -DLINUX
	EXE_EXTENSION=''
else ifeq ($(UNAME), Darwin)
	OPENCV_HOME=/usr/local
	INCLUDE+= -I$(OPENCV_HOME)/include -I$(OPENCV_HOME)/include/opencv
	LIBS += -L$(OPENCV_HOME)/lib
#	LINUX_BOOST_PREFIX = /usr/local
	CFLAGS += -DLINUX
	EXE_EXTENSION=''
else # windows
	MINGW_HOME = 'C:\MinGW'
	LDFLAGS += -mconsole -mwindows -lole32 -lwsock32 -luuid
	EXE_EXTENSION='.exe'
endif

ifeq ($(DEBUG), 1)
	CFLAGS += -g -gstabs+ -DDEBUG
else
    ifeq ($(UNAME), Darwin)
        CFLAGS += -O3
    else
	CFLAGS += -O3 --fast-math
    endif
	CFLAGS += -DNDEBUG
endif

ifeq ($(PROFILING), 1)
	CFLAGS += -pg
endif

CXXFLAGS = $(INCLUDE) $(CFLAGS)

#GUI_OBJS = 
CV_OBJS = cvutils.o
COMMON_OBJS = utils.o Motion.o Parameters.o utils.o
OBJS = $(COMMON_OBJS) $(CV_OBJS)
TESTS_OBJS = test_feature.o test_graph.o


default: builddir tests feature-based-tracking

optional: test-pixels optical-flow

builddir:
	@$(SCRIPTS_DIR)/createdirectory.sh $(EXE_DIR)

test:
	echo "coucou $(HOME)"

tests: $(TESTS_OBJS) $(OBJS)
	@$(SCRIPTS_DIR)/createdirectory.sh $(EXE_DIR)
	@$(SCRIPTS_DIR)/createdirectory.sh test_data
	$(CXX) $(CFLAGS) $(LIBS) -o $(EXE_DIR)/$@ $^ $(LDFLAGS)
	$(EXE_DIR)/$@

feature-based-tracking: feature-based-tracking.o $(OBJS)
	@$(SCRIPTS_DIR)/createdirectory.sh $(EXE_DIR)
	$(CXX) $(CFLAGS) $(LIBS) $^ -o $(EXE_DIR)/$@ $(LDFLAGS)

optical-flow: optical-flow.o $(OBJS)
	$(CXX) $(CFLAGS) $(LIBS) $^ -o $(EXE_DIR)/$@ $(LDFLAGS)

test-pixels: test-pixels.o $(OBJS)
	$(CXX) $(CFLAGS) $(LIBS) $^ -o $(EXE_DIR)/$@ $(LDFLAGS)

clean:
	rm -f *.gch */*.o *.o *.a  $(EXE_DIR)/*
# $(DISTFILE_OUTPUT)
