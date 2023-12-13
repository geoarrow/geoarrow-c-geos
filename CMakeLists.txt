message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.14)

# Avoids a warning about timestamps on downloaded files (prefer new policy if
# available))
if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.23")
  cmake_policy(SET CMP0135 NEW)
endif()

include(FetchContent)

if(NOT DEFINED CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 11)
endif()

set(GEOARROW_GEOS_VERSION "0.1.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" GEOARROW_GEOS_BASE_VERSION
             "${GEOARROW_GEOS_VERSION}")
project(geoarrow_geos VERSION "${GEOARROW_GEOS_BASE_VERSION}")

set(GEOARROW_GEOS_VERSION_MAJOR "${GEOARROW_GEOS_VERSION_MAJOR}")
set(GEOARROW_GEOS_VERSION_MINOR "${GEOARROW_GEOS_VERSION_MINOR}")
set(GEOARROW_GEOS_VERSION_PATCH "${GEOARROW_GEOS_VERSION_PATCH}")

option(GEOARROW_GEOS_BUILD_TESTS "Build tests" OFF)

# Ensure geoarrow_c
FetchContent_Declare(
  geoarrow_c
  URL https://github.com/geoarrow/geoarrow-c/archive/22794ce83fae1e2e99511508fa936c1e4cb115cb.zip
  URL_HASH
    SHA256=3631aa2c0883a76d18bfa56395f780f8ff6eec49b2fad21c24f72bb3e15ceed7)

FetchContent_MakeAvailable(geoarrow_c)

# Ensure GEOS
find_package(GEOS REQUIRED)

add_library(geoarrow_geos src/geoarrow_geos/geoarrow_geos.c)
target_link_libraries(
  geoarrow_geos
  PUBLIC GEOS::geos_c
  PRIVATE geoarrow)

target_include_directories(
  geoarrow_geos
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/geoarrow_geos>
         $<INSTALL_INTERFACE:include>)

install(TARGETS geoarrow_geos DESTINATION lib)
install(
  DIRECTORY src/
  DESTINATION include
  FILES_MATCHING
  PATTERN "*.h")

if(GEOARROW_GEOS_BUILD_TESTS)
  set(MEMORYCHECK_COMMAND_OPTIONS "--leak-check=full --error-exitcode=1")

  include(CTest)

  if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 14)
  endif()

  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/release-1.11.0.zip
    URL_HASH
      SHA256=353571c2440176ded91c2de6d6cd88ddd41401d14692ec1f99e35d013feda55a)

  FetchContent_MakeAvailable(googletest)

  FetchContent_Declare(
    nanoarrow
    URL https://github.com/apache/arrow-nanoarrow/archive/refs/tags/apache-arrow-nanoarrow-0.3.0.zip
    URL_HASH
      SHA256=ea7b946fd4604c6da7df80142636ddd686a0b31b29a2fc8ac2629bfc23ccf9c2)

  FetchContent_MakeAvailable(nanoarrow)

  enable_testing()

  add_executable(geoarrow_geos_test src/geoarrow_geos/geoarrow_geos_test.cc)

  target_link_libraries(geoarrow_geos_test geoarrow_geos nanoarrow gtest_main)

  include(GoogleTest)
  gtest_discover_tests(geoarrow_geos_test)

endif()
