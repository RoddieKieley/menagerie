cmake_minimum_required(VERSION 3.1)
project(menagerie)

set(CMAKE_CXX_STANDARD 11)

if(${CMAKE_GENERATOR} STREQUAL "Xcode")
    include_directories("/usr/local/include")
endif()

find_library(QPID_PROTON_CPP qpid-proton-cpp)

add_subdirectory(qpid-proton-cpp-message-groups)
add_subdirectory(qpid-proton-cpp-multithreading-el6)
add_subdirectory(qpid-proton-cpp-subscriptions)
