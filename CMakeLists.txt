message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.14)

# Avoids a warning about timestamps on downloaded files (prefer new policy
# if available))
if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.23")
  cmake_policy(SET CMP0135 NEW)
endif()

include(FetchContent)

if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 11)
endif()

set(GEOARROW_GEOS_VERSION "0.1.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" GEOARROW_GEOS_BASE_VERSION
             "${GEOARROW_GEOS_VERSION}")
project(geoarrow_geos VERSION "${GEOARROW_GEOS_BASE_VERSION}")

set(GEOARROW_GEOS_VERSION_MAJOR "${GEOARROW_GEOS_VERSION_MAJOR}")
set(GEOARROW_GEOS_VERSION_MINOR "${GEOARROW_GEOS_VERSION_MINOR}")
set(GEOARROW_GEOS_VERSION_PATCH "${GEOARROW_GEOS_VERSION_PATCH}")

option(GEOARROW_GEOS_BUILD_TESTS "Build tests" OFF)
option(GEOARROW_GEOS_CODE_COVERAGE "Enable coverage reporting" OFF)

add_library(geoarrow_geos_coverage_config INTERFACE)

if(GEOARROW_GEOS_CODE_COVERAGE)
  target_compile_options(geoarrow_geos_coverage_config INTERFACE -O0 -g --coverage)
  target_link_options(geoarrow_geos_coverage_config INTERFACE --coverage)
endif()

# Ensure geoarrow_c
FetchContent_Declare(
  geoarrow_c
  URL https://github.com/geoarrow/geoarrow-c/archive/f7c3aa5986d90f9f660f8a99750b9c2e576d9532.zip
  URL_HASH
    SHA256=7d08c3ab7256855bde0e75f034e2c7973d2feff31266b3dd9c43bfdea219ae28)

FetchContent_MakeAvailable(geoarrow_c)

# Ensure GEOS
find_package(GEOS REQUIRED)

add_library(geoarrow_geos src/geoarrow_geos/geoarrow_geos.cc)
target_link_libraries(geoarrow_geos PRIVATE GEOS::geos_c geoarrow)

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
  include(CTest)

  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/release-1.11.0.zip
    URL_HASH
      SHA256=353571c2440176ded91c2de6d6cd88ddd41401d14692ec1f99e35d013feda55a)

  FetchContent_MakeAvailable(googletest)

  enable_testing()

  add_executable(geoarrow_geos_test src/geoarrow_geos/geoarrow_geos_test.cc)

  if(GEOARROW_CODE_COVERAGE)
    target_compile_options(geoarrow_geos_coverage_config INTERFACE -O0 -g --coverage)
    target_link_options(geoarrow_geos_coverage_config INTERFACE --coverage)
    target_link_libraries(geoarrow_geos geoarrow_geos_coverage_config)
  endif()

  target_link_libraries(geoarrow_geos_test geoarrow_geos gtest_main)

  include(GoogleTest)
  gtest_discover_tests(geoarrow_geos_test)

endif()
