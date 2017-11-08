add_executable(send send.cpp)
add_definitions(-DPN_CPP_HAS_LAMBDAS=0)
target_link_libraries(send ${QPID_PROTON_CPP})
target_link_libraries(send pthread)
