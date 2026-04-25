cmake_minimum_required(VERSION 3.16)
project(hackupc_location_prototype)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(CURL REQUIRED)

add_executable(location_prototype main.cpp)
target_link_libraries(location_prototype CURL::libcurl)
