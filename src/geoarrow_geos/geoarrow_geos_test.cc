
#include <gtest/gtest.h>

#include <nanoarrow/nanoarrow.hpp>

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

void TestBuilderRoundtripWKT(const std::string& wkt) {
  GEOSCppHandle handle;
  GEOSCppWKTReader reader(handle.handle);
  GEOSCppGeometry geom(handle.handle);
  GeoArrowGEOSCppArrayBuilder builder(handle.handle);
  nanoarrow::UniqueSchema schema;

  ASSERT_EQ(GeoArrowGEOSMakeSchema(GEOARROW_GEOS_ENCODING_WKT, 0, schema.get()),
            GEOARROW_GEOS_OK);
  ASSERT_EQ(builder.Init(schema.get()), GEOARROW_GEOS_OK);

  ASSERT_EQ(reader.Read(wkt, &geom.ptr), GEOARROW_GEOS_OK);
  size_t n = 0;
  const GEOSGeometry* geom_const = geom.ptr;
  ASSERT_EQ(GeoArrowGEOSArrayBuilderAppend(builder.ptr, &geom_const, 1, &n),
            GEOARROW_GEOS_OK)
      << "WKT: " << wkt
      << " Error: " << GeoArrowGEOSArrayBuilderGetLastError(builder.ptr);
  ASSERT_EQ(n, 1);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(GeoArrowGEOSArrayBuilderFinish(builder.ptr, array.get()), GEOARROW_GEOS_OK);

  ASSERT_EQ(array->length, 1);
  ASSERT_EQ(array->n_buffers, 3);

  const auto offsets = reinterpret_cast<const int32_t*>(array->buffers[1]);
  const auto data = reinterpret_cast<const char*>(array->buffers[2]);

  std::string wkt_out(data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(wkt_out, wkt);
}

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripWKT) {
  TestBuilderRoundtripWKT("POINT EMPTY");
  TestBuilderRoundtripWKT("POINT (0 1)");
  TestBuilderRoundtripWKT("POINT Z EMPTY");
  TestBuilderRoundtripWKT("POINT Z (0 1 2)");

  TestBuilderRoundtripWKT("LINESTRING EMPTY");
  TestBuilderRoundtripWKT("LINESTRING (0 1, 2 3)");
  TestBuilderRoundtripWKT("LINESTRING Z EMPTY");
  TestBuilderRoundtripWKT("LINESTRING Z (0 1 2, 3 4 5)");

  TestBuilderRoundtripWKT("POLYGON EMPTY");
  TestBuilderRoundtripWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
  TestBuilderRoundtripWKT(
      "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))");
}

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripCollectionWKT) {
  TestBuilderRoundtripWKT("MULTIPOINT EMPTY");
  TestBuilderRoundtripWKT("MULTIPOINT (30 10)");
  TestBuilderRoundtripWKT("MULTIPOINT (30 10, 40 30, 20 20)");
}
