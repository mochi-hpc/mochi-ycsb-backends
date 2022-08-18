cmake_minimum_required (VERSION 3.0)
project (HelloWorld)

find_package (Java REQUIRED)
find_package (JNI REQUIRED)
include (UseJava)

find_jar (YCSB_JAR NAMES core VERSIONS 0.17.0 PATHS ${YCSB_ROOT} ENV CLASSPATH)
if ("${YCSB_JAR}" STREQUAL "YCSB_JAR-NOTFOUND")
    message (FATAL_ERROR "Could not find YCSB's core jar, please set CLASSPATH or YCSB_ROOT")
else ()
    message (STATUS "Found YCSB: ${YCSB_JAR}")
endif ()

set (CMAKE_JNI_TARGET TRUE)
add_jar (YokanDBClient
         VERSION 0.0.1
         GENERATE_NATIVE_HEADERS YokanDBClient-native DESTINATION ${CMAKE_CURRENT_BINARY_DIR}
         SOURCES src/java/gov/anl/mochi/YokanDBClient.java
         INCLUDE_JARS ${YCSB_JAR})

add_library (YokanDBClient-cpp SHARED
             src/cpp/YokanDBClient-cpp.cpp)
target_include_directories (YokanDBClient-cpp PRIVATE ${JNI_INCLUDE_DIRS})
target_link_libraries (YokanDBClient-cpp PRIVATE YokanDBClient-native ${JNI_LIBRARIES})

configure_file (bin/mochi-ycsb-cli.in bin/mochi-ycsb-cli @ONLY USE_SOURCE_PERMISSIONS)

install (TARGETS YokanDBClient-cpp
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install_jar (YokanDBClient DESTINATION lib)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/bin/mochi-ycsb-cli
         DESTINATION bin
         PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                     GROUP_EXECUTE GROUP_WRITE GROUP_READ)
