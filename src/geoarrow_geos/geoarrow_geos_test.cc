
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

class GeoArrowGEOSCppArrayReader {
 public:
  GeoArrowGEOSArrayReader* ptr;
  GEOSContextHandle_t handle;

  GeoArrowGEOSCppArrayReader(GEOSContextHandle_t handle) : ptr(nullptr), handle(handle) {}

  GeoArrowGEOSErrorCode Init(ArrowSchema* schema) {
    return GeoArrowGEOSArrayReaderCreate(handle, schema, &ptr);
  }

  ~GeoArrowGEOSCppArrayReader() {
    if (ptr != nullptr) {
      GeoArrowGEOSArrayReaderDestroy(ptr);
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
      << "\n Error: " << GeoArrowGEOSArrayBuilderGetLastError(builder.ptr);
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

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripWKTPoint) {
  TestBuilderRoundtripWKT("POINT EMPTY");
  TestBuilderRoundtripWKT("POINT (0 1)");
  TestBuilderRoundtripWKT("POINT Z EMPTY");
  TestBuilderRoundtripWKT("POINT Z (0 1 2)");
}

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripWKTLinestring) {
  TestBuilderRoundtripWKT("LINESTRING EMPTY");
  TestBuilderRoundtripWKT("LINESTRING (0 1, 2 3)");
  TestBuilderRoundtripWKT("LINESTRING Z EMPTY");
  TestBuilderRoundtripWKT("LINESTRING Z (0 1 2, 3 4 5)");
}

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripWKTPolygon) {
  TestBuilderRoundtripWKT("POLYGON EMPTY");
  TestBuilderRoundtripWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
  TestBuilderRoundtripWKT(
      "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))");
}

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripWKTCollection) {
  TestBuilderRoundtripWKT("MULTIPOINT EMPTY");
  TestBuilderRoundtripWKT("MULTIPOINT (30 10)");
  TestBuilderRoundtripWKT("MULTIPOINT (30 10, 40 30, 20 20)");
}

void TestReaderRoundtripWKT(const std::string& wkt, int wkb_type) {
  GEOSCppHandle handle;
  GeoArrowGEOSCppArrayBuilder builder(handle.handle);
  GeoArrowGEOSCppArrayReader reader(handle.handle);

  // Initialize builder + build a target array
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(
      GeoArrowGEOSMakeSchema(GEOARROW_GEOS_ENCODING_GEOARROW, wkb_type, schema.get()),
      GEOARROW_GEOS_OK);
  ASSERT_EQ(builder.Init(schema.get()), GEOARROW_GEOS_OK);

  GEOSCppWKTReader wkt_reader(handle.handle);
  GEOSCppGeometry geom(handle.handle);
  ASSERT_EQ(wkt_reader.Read(wkt, &geom.ptr), GEOARROW_GEOS_OK);

  size_t n = 0;
  const GEOSGeometry* geom_const = geom.ptr;
  ASSERT_EQ(GeoArrowGEOSArrayBuilderAppend(builder.ptr, &geom_const, 1, &n),
            GEOARROW_GEOS_OK);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(GeoArrowGEOSArrayBuilderFinish(builder.ptr, array.get()), GEOARROW_GEOS_OK);

  // Read it back!
  ASSERT_EQ(reader.Init(schema.get()), GEOARROW_GEOS_OK);

  GEOSCppGeometry geom_out(handle.handle);
  ASSERT_EQ(GeoArrowGEOSArrayReaderRead(reader.ptr, array.get(), 0, 1, &geom_out.ptr),
            GEOARROW_GEOS_OK)
      << "WKT: " << wkt
      << "\n Error: " << GeoArrowGEOSArrayReaderGetLastError(reader.ptr);

  // Check for GEOS equality
  EXPECT_EQ(GEOSEqualsExact_r(handle.handle, geom_out.ptr, geom.ptr, 0), 1)
      << "WKT: " << wkt
      << "\n Error: " << GeoArrowGEOSArrayReaderGetLastError(reader.ptr);
}

TEST(GeoArrowGEOSTest, TestArrayReaderPoint) {
  TestReaderRoundtripWKT("POINT EMPTY", 1);
  TestReaderRoundtripWKT("POINT (0 1)", 1);
  TestReaderRoundtripWKT("POINT Z EMPTY", 1001);
  TestReaderRoundtripWKT("POINT Z (0 1 2)", 1001);
}

TEST(GeoArrowGEOSTest, TestArrayReaderLinestring) {
  TestReaderRoundtripWKT("LINESTRING EMPTY", 2);
  TestReaderRoundtripWKT("LINESTRING (0 1, 2 3)", 2);
  // LINESTRING Z EMPTY doesn't seem to roundtrip through GEOSGeometry
  TestReaderRoundtripWKT("LINESTRING Z (0 1 2, 3 4 5)", 1002);
}

TEST(GeoArrowGEOSTest, TestArrayReaderPolygon) {
  TestReaderRoundtripWKT("POLYGON EMPTY", 3);
  TestReaderRoundtripWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 3);
  TestReaderRoundtripWKT(
      "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", 3);
  TestReaderRoundtripWKT("POLYGON Z EMPTY", 1003);
  TestReaderRoundtripWKT("POLYGON Z ((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40))",
                         1003);
  TestReaderRoundtripWKT(
      "POLYGON Z ((35 10 45, 45 45 90, 15 40 55, 10 20 30, 35 10 45), (20 30 50, 35 35 "
      "70, 30 20 50, 20 30 50))",
      1003);
  TestReaderRoundtripWKT(
      "POLYGON Z ((35 10 45, 45 45 90, 15 40 55, 10 20 30, 35 10 45), (20 30 50, 35 35 "
      "70, 30 20 50, 20 30 50))",
      1003);
}
