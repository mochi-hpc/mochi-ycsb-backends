cmake_minimum_required (VERSION 3.0)
project (HelloWorld)

set (CMAKE_CXX_STANDARD 14)

find_package (Java REQUIRED)
find_package (JNI REQUIRED)
include (UseJava)

find_jar (YCSB_JAR NAMES core VERSIONS 0.17.0 PATHS ${YCSB_ROOT}/lib ENV CLASSPATH)
if ("${YCSB_JAR}" STREQUAL "YCSB_JAR-NOTFOUND")
    message (FATAL_ERROR "Could not find YCSB's core jar, please set CLASSPATH or YCSB_ROOT")
else ()
    message (STATUS "Found YCSB: ${YCSB_JAR}")
endif ()

set (CMAKE_JNI_TARGET TRUE)
add_jar (MochiDBClient
         VERSION 0.0.1
         GENERATE_NATIVE_HEADERS MochiDBClient-native DESTINATION ${CMAKE_CURRENT_BINARY_DIR}
         SOURCES src/java/gov/anl/mochi/MochiDBClient.java
         INCLUDE_JARS ${YCSB_JAR}
         OUTPUT_DIR ${PROJECT_BINARY_DIR}/lib)

add_library (mochi-ycsb SHARED
             src/cpp/MochiDBClient.cpp
             src/cpp/TestDB.cpp)
target_include_directories (mochi-ycsb PRIVATE ${JNI_INCLUDE_DIRS})
target_include_directories (mochi-ycsb PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mochi-ycsb BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries (mochi-ycsb PRIVATE MochiDBClient-native ${JNI_LIBRARIES})
set_target_properties (mochi-ycsb PROPERTIES
                       LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

configure_file (bin/mochi-ycsb-cli.in bin/mochi-ycsb-cli @ONLY USE_SOURCE_PERMISSIONS)

install (TARGETS mochi-ycsb
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install_jar (MochiDBClient DESTINATION lib)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/bin/mochi-ycsb-cli
         DESTINATION bin
         PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                     GROUP_EXECUTE GROUP_WRITE GROUP_READ)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/MochiYCSB.hpp
         DESTINATION include)
