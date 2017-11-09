SOURCES := subscribe.cpp durable-subscribe.cpp shared-subscribe.cpp
TARGETS := ${SOURCES:%.cpp=%}

build: ${TARGETS}

clean:
	rm -f ${TARGETS}

%: %.cpp
	g++ -Os -g -std=c++11 -lqpid-proton-cpp $< -o $@
