cmake_minimum_required (VERSION 3.0)
project (YCSBCppInterface)

set (CMAKE_CXX_STANDARD 14)

find_package (Java REQUIRED)
find_package (JNI REQUIRED)
include (UseJava)

find_jar (YCSB_JAR NAMES core VERSIONS 0.17.0 0.18.0-SNAPSHOT PATHS ${YCSB_ROOT}/lib ENV CLASSPATH)
if ("${YCSB_JAR}" STREQUAL "YCSB_JAR-NOTFOUND")
    message (FATAL_ERROR "Could not find YCSB's core jar, please set CLASSPATH or YCSB_ROOT")
else ()
    message (STATUS "Found YCSB: ${YCSB_JAR}")
endif ()

set (CMAKE_JNI_TARGET TRUE)
add_jar (YcsbDBClient
         VERSION 0.0.1
         GENERATE_NATIVE_HEADERS YcsbDBClient-native DESTINATION ${CMAKE_CURRENT_BINARY_DIR}
         SOURCES src/java/cpp/ycsb/YcsbDBClient.java
         INCLUDE_JARS ${YCSB_JAR}
         OUTPUT_DIR ${PROJECT_BINARY_DIR}/lib)

add_library (ycsb-cpp-interface SHARED
             src/YcsbDBClient.cpp
             src/YcsbCppInterface.cpp
             src/TestDB.cpp)
target_include_directories (ycsb-cpp-interface PRIVATE ${JNI_INCLUDE_DIRS})
target_include_directories (ycsb-cpp-interface PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (ycsb-cpp-interface BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries (ycsb-cpp-interface PRIVATE YcsbDBClient-native ${JNI_LIBRARIES})
set_target_properties (ycsb-cpp-interface PROPERTIES
                       LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

configure_file (bin/ycsb-cpp-cli.in bin/ycsb-cpp-cli @ONLY USE_SOURCE_PERMISSIONS)
configure_file (bin/ycsb-cpp.in bin/ycsb-cpp @ONLY USE_SOURCE_PERMISSIONS)

install_jar (YcsbDBClient DESTINATION lib)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/bin/ycsb-cpp-cli
               ${CMAKE_CURRENT_BINARY_DIR}/bin/ycsb-cpp
         DESTINATION bin
         PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                     GROUP_EXECUTE GROUP_WRITE GROUP_READ)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/YCSBCppInterface.hpp
         DESTINATION include)

install(TARGETS
        ycsb-cpp-interface
    EXPORT ycsb-cpp-interface-config
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
export (TARGETS
        ycsb-cpp-interface
        FILE "${CMAKE_CURRENT_BINARY_DIR}/ycsb-cpp-interface-config.cmake")
install (EXPORT
         ycsb-cpp-interface-config
         DESTINATION "share/cmake/ycsb-cpp-interface")
