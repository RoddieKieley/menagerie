add_executable(message-groups receiver.cpp message-groups.cpp)
target_link_libraries(message-groups ${QPID_PROTON_CPP})
