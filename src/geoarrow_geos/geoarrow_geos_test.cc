
#include <gtest/gtest.h>

#include "geoarrow_geos.h"

class GEOSCppHandle {
 public:
  GEOSContextHandle_t handle;

  GEOSCppHandle() { handle = GEOS_init_r(); }

  ~GEOSCppHandle() { GEOS_finish_r(handle); }
};

class GEOSCppGeometry {
 public:
  GEOSGeometry* ptr;
  GEOSContextHandle_t handle;

  GEOSCppGeometry(GEOSContextHandle_t handle) : handle(handle), ptr(nullptr) {}

  ~GEOSCppGeometry() {
    if (ptr != nullptr) {
      GEOSGeom_destroy_r(handle, ptr);
    }
  }
};

class GEOSCppWKTReader {
 public:
  GEOSWKTReader* ptr;
  GEOSContextHandle_t handle;

  GEOSCppWKTReader(GEOSContextHandle_t handle) : handle(handle), ptr(nullptr) {
    ptr = GEOSWKTReader_create_r(handle);
  }

  GeoArrowGEOSErrorCode Read(const std::string& wkt, GEOSGeometry** out) {
    GEOSGeometry* result = GEOSWKTReader_read_r(handle, ptr, wkt.c_str());
    if (result == nullptr) {
      return EINVAL;
    }

    *out = result;
    return GEOARROW_GEOS_OK;
  }

  ~GEOSCppWKTReader() {
    if (ptr != NULL) {
      GEOSWKTReader_destroy_r(handle, ptr);
    }
  }
};

class GeoArrowGEOSCppArrayBuilder {
 public:
  GeoArrowGEOSArrayBuilder* ptr;
  GEOSContextHandle_t handle;

  GeoArrowGEOSCppArrayBuilder(GEOSContextHandle_t handle)
      : ptr(nullptr), handle(handle) {}

  GeoArrowGEOSErrorCode Init(ArrowSchema* schema) {
    return GeoArrowGEOSArrayBuilderCreate(handle, schema, &ptr);
  }

  ~GeoArrowGEOSCppArrayBuilder() {
    if (ptr != nullptr) {
      GeoArrowGEOSArrayBuilderDestroy(ptr);
    }
  }
};

TEST(GeoArrowGEOSTest, TestVersions) {
  ASSERT_EQ(std::string(GeoArrowGEOSVersionGEOS()).substr(0, 1), "3");
  ASSERT_STREQ(GeoArrowGEOSVersionGeoArrow(), "0.2.0-SNAPSHOT");
}

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripWKT) {
  GEOSCppHandle handle;
  GEOSCppWKTReader reader(handle.handle);
  GEOSCppGeometry geom(handle.handle);
  GeoArrowGEOSCppArrayBuilder builder(handle.handle);

  std::string wkt = "POINT (0 1)";
  ASSERT_EQ(reader.Read(wkt, &geom.ptr), GEOARROW_GEOS_OK);
}
