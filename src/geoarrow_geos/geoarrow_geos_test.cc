
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

class GEOSCppGeometryVec {
 public:
  std::vector<GEOSGeometry*> ptrs;
  GEOSContextHandle_t handle;

  GEOSCppGeometryVec(GEOSContextHandle_t handle) : handle(handle) {}

  GEOSGeometry** data() { return ptrs.data(); }

  const GEOSGeometry** const_data() { return const_cast<const GEOSGeometry**>(data()); }

  ~GEOSCppGeometryVec() {
    for (const auto& ptr : ptrs) {
      if (ptr != nullptr) {
        GEOSGeom_destroy_r(handle, ptr);
      }
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
    if (wkt == "") {
      *out = nullptr;
      return NANOARROW_OK;
    }

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

  if (wkt_out == "") {
    ASSERT_NE(array->buffers[0], nullptr);
    const auto validity = reinterpret_cast<const uint8_t*>(array->buffers[0]);
    EXPECT_EQ(validity[0] & (1 << 0), 0);
  }
}

TEST(GeoArrowGEOSTest, TestArrayBuilderRoundtripWKTNull) { TestBuilderRoundtripWKT(""); }

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

void TestReaderRoundtripWKTVec(
    const std::vector<std::string>& wkt, int wkb_type,
    GeoArrowGEOSEncoding encoding = GEOARROW_GEOS_ENCODING_GEOARROW) {
  GEOSCppHandle handle;
  GeoArrowGEOSCppArrayBuilder builder(handle.handle);
  GeoArrowGEOSCppArrayReader reader(handle.handle);

  // Initialize builder + build a target array
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(GeoArrowGEOSMakeSchema(encoding, wkb_type, schema.get()), GEOARROW_GEOS_OK);
  ASSERT_EQ(builder.Init(schema.get()), GEOARROW_GEOS_OK);

  GEOSCppWKTReader wkt_reader(handle.handle);

  GEOSCppGeometryVec geoms_in(handle.handle);
  GEOSCppGeometryVec geoms_out(handle.handle);
  for (const auto& wkt_item : wkt) {
    geoms_in.ptrs.push_back(nullptr);
    geoms_out.ptrs.push_back(nullptr);

    ASSERT_EQ(wkt_reader.Read(wkt_item, &geoms_in.ptrs.back()), GEOARROW_GEOS_OK)
        << "Failed to append " << wkt_item;
  }

  size_t n = 0;
  ASSERT_EQ(
      GeoArrowGEOSArrayBuilderAppend(builder.ptr, geoms_in.const_data(), wkt.size(), &n),
      GEOARROW_GEOS_OK);
  ASSERT_EQ(n, wkt.size());

  nanoarrow::UniqueArray array;
  ASSERT_EQ(GeoArrowGEOSArrayBuilderFinish(builder.ptr, array.get()), GEOARROW_GEOS_OK);

  // Read it back!
  ASSERT_EQ(reader.Init(schema.get()), GEOARROW_GEOS_OK);

  ASSERT_EQ(GeoArrowGEOSArrayReaderRead(reader.ptr, array.get(), 0, array->length,
                                        geoms_out.data()),
            GEOARROW_GEOS_OK)
      << "WKT[0]: " << wkt[0] << " n = " << n
      << "\n Error: " << GeoArrowGEOSArrayReaderGetLastError(reader.ptr);

  // Check for GEOS equality
  for (size_t i = 0; i < n; i++) {
    if (geoms_out.ptrs[i] == nullptr || geoms_in.ptrs[i] == nullptr) {
      EXPECT_EQ(geoms_out.ptrs[i], geoms_in.ptrs[i]);
    } else {
      EXPECT_EQ(GEOSEqualsExact_r(handle.handle, geoms_out.ptrs[i], geoms_in.ptrs[i], 0),
                1)
          << "WKT: " << wkt[i] << " at index " << i;
    }
  }
}

void TestReaderRoundtripWKT(
    const std::string& wkt, int wkb_type,
    GeoArrowGEOSEncoding encoding = GEOARROW_GEOS_ENCODING_GEOARROW) {
  TestReaderRoundtripWKTVec({wkt}, wkb_type, encoding);
}

class EncodingTestFixture : public ::testing::TestWithParam<GeoArrowGEOSEncoding> {
 protected:
  GeoArrowGEOSEncoding encoding;
};

TEST_P(EncodingTestFixture, TestArrayReaderPoint) {
  GeoArrowGEOSEncoding encoding = GetParam();

  TestReaderRoundtripWKT("", 1, encoding);
  TestReaderRoundtripWKT("POINT EMPTY", 1, encoding);
  TestReaderRoundtripWKT("POINT (0 1)", 1, encoding);
  TestReaderRoundtripWKT("POINT Z EMPTY", 1001, encoding);
  TestReaderRoundtripWKT("POINT Z (0 1 2)", 1001, encoding);

  TestReaderRoundtripWKTVec({}, 1, encoding);
  TestReaderRoundtripWKTVec({}, 1001, encoding);
  TestReaderRoundtripWKTVec(
      {"POINT EMPTY", "POINT (0 1)", "POINT (2 3)", "POINT EMPTY", ""}, 1, encoding);
  TestReaderRoundtripWKTVec(
      {"POINT Z EMPTY", "POINT Z (0 1 2)", "POINT Z (3 4 5)", "POINT Z EMPTY", ""}, 1001,
      encoding);
}

TEST_P(EncodingTestFixture, TestArrayReaderLinestring) {
  GeoArrowGEOSEncoding encoding = GetParam();

  TestReaderRoundtripWKT("", 2, encoding);
  TestReaderRoundtripWKT("LINESTRING EMPTY", 2, encoding);
  TestReaderRoundtripWKT("LINESTRING (0 1, 2 3)", 2, encoding);
  TestReaderRoundtripWKT("LINESTRING Z EMPTY", 2, encoding);
  TestReaderRoundtripWKT("LINESTRING Z (0 1 2, 3 4 5)", 1002, encoding);

  TestReaderRoundtripWKTVec({}, 2, encoding);
  TestReaderRoundtripWKTVec({}, 1002, encoding);
  TestReaderRoundtripWKTVec({"LINESTRING EMPTY", "LINESTRING (0 1, 2 3)",
                             "LINESTRING (4 5, 6 7, 8 9)", "LINESTRING EMPTY", ""},
                            2, encoding);
  TestReaderRoundtripWKTVec(
      {"LINESTRING Z EMPTY", "LINESTRING Z (0 1 2, 3 4 5)",
       "LINESTRING Z (6 7 8, 9 10 11, 12 13 14)", "LINESTRING Z EMPTY", ""},
      1002, encoding);
}

TEST_P(EncodingTestFixture, TestArrayReaderPolygon) {
  GeoArrowGEOSEncoding encoding = GetParam();

  TestReaderRoundtripWKT("", 3, encoding);
  TestReaderRoundtripWKT("POLYGON EMPTY", 3, encoding);
  TestReaderRoundtripWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 3);
  TestReaderRoundtripWKT(
      "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", 3);
  TestReaderRoundtripWKT("POLYGON Z EMPTY", 1003, encoding);
  TestReaderRoundtripWKT("POLYGON Z ((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40))",
                         1003, encoding);
  TestReaderRoundtripWKT(
      "POLYGON Z ((35 10 45, 45 45 90, 15 40 55, 10 20 30, 35 10 45), (20 30 50, 35 35 "
      "70, 30 20 50, 20 30 50))",
      1003, encoding);
  TestReaderRoundtripWKT(
      "POLYGON Z ((35 10 45, 45 45 90, 15 40 55, 10 20 30, 35 10 45), (20 30 50, 35 35 "
      "70, 30 20 50, 20 30 50))",
      1003, encoding);

  TestReaderRoundtripWKTVec({}, 3, encoding);
  TestReaderRoundtripWKTVec({}, 1003, encoding);
  TestReaderRoundtripWKTVec(
      {"POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
       "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
       "POLYGON EMPTY", ""},
      3, encoding);

  TestReaderRoundtripWKTVec(
      {"POLYGON Z ((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40))",
       "POLYGON Z ((35 10 45, 45 45 90, 15 40 55, 10 20 30, 35 10 45), (20 30 50, 35 35 "
       "70, 30 20 50, 20 30 50))",
       "POLYGON Z EMPTY", ""},
      1003, encoding);
}

TEST_P(EncodingTestFixture, TestArrayReaderMultipoint) {
  GeoArrowGEOSEncoding encoding = GetParam();

  TestReaderRoundtripWKT("", 4, encoding);
  TestReaderRoundtripWKT("MULTIPOINT EMPTY", 4, encoding);
  TestReaderRoundtripWKT("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", 4, encoding);
  TestReaderRoundtripWKT("MULTIPOINT (30 10)", 4, encoding);

  TestReaderRoundtripWKTVec({}, 4, encoding);
  TestReaderRoundtripWKTVec({}, 1004, encoding);
  TestReaderRoundtripWKTVec(
      {"MULTIPOINT ((30 10))", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
       "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", ""},
      4, encoding);

  TestReaderRoundtripWKTVec(
      {"MULTIPOINT Z ((30 10 40))",
       "MULTIPOINT Z ((10 40 50), (40 30 70), (20 20 40), (30 10 40))",
       "MULTIPOINT Z ((10 40 50), (40 30 70), (20 20 40), (30 10 40))",
       "MULTIPOINT Z EMPTY", ""},
      1004, encoding);
}

TEST_P(EncodingTestFixture, TestArrayReaderMultilinestring) {
  GeoArrowGEOSEncoding encoding = GetParam();

  TestReaderRoundtripWKT("", 5, encoding);
  TestReaderRoundtripWKT("MULTILINESTRING EMPTY", 5, encoding);
  TestReaderRoundtripWKT("MULTILINESTRING ((30 10, 10 30, 40 40))", 5, encoding);
  TestReaderRoundtripWKT(
      "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))", 5,
      encoding);

  TestReaderRoundtripWKTVec(
      {"MULTILINESTRING ((30 10, 10 30, 40 40))",
       "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
       "MULTILINESTRING EMPTY", ""},
      5, encoding);

  TestReaderRoundtripWKTVec({}, 5, encoding);
  TestReaderRoundtripWKTVec({}, 1005, encoding);
  TestReaderRoundtripWKTVec({"MULTILINESTRING Z ((30 10 40, 10 30 40, 40 40 80))",
                             "MULTILINESTRING Z ((10 10 20, 20 20 40, 10 40 50), (40 40 "
                             "80, 30 30 60, 40 20 60, 30 10 40))",
                             "MULTILINESTRING Z EMPTY", ""},
                            1005, encoding);
}

TEST_P(EncodingTestFixture, TestArrayReaderMultipolygon) {
  GeoArrowGEOSEncoding encoding = GetParam();

  TestReaderRoundtripWKT("", 6, encoding);
  TestReaderRoundtripWKT("MULTIPOLYGON EMPTY", 6, encoding);
  TestReaderRoundtripWKT(
      "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
      6, encoding);
  TestReaderRoundtripWKT(
      "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, "
      "20 35), (30 20, 20 15, 20 25, 30 20)))",
      6, encoding);

  TestReaderRoundtripWKTVec({}, 6, encoding);
  TestReaderRoundtripWKTVec({}, 1006, encoding);
  TestReaderRoundtripWKTVec(
      {"MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)))",
       "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 "
       "5)))",
       "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 "
       "20, 20 35), (30 20, 20 15, 20 25, 30 20)))",
       "MULTIPOLYGON EMPTY", ""},
      6, encoding);

  TestReaderRoundtripWKTVec(
      {"MULTIPOLYGON Z (((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40)))",
       "MULTIPOLYGON Z (((30 20 50, 45 40 85, 10 40 50, 30 20 50)), ((15 5 20, 40 10 50, "
       "10 20 30, 5 10 15, 15 5 20)))",
       "MULTIPOLYGON Z (((40 40 80, 20 45 65, 45 30 75, 40 40 80)), ((20 35 55, 10 30 "
       "40, 10 10 20, 30 5 35, 45 20 65, 20 35 55), (30 20 50, 20 15 35, 20 25 45, 30 20 "
       "50)))",
       "MULTIPOLYGON Z EMPTY", ""},
      1006, encoding);
}

INSTANTIATE_TEST_SUITE_P(GeoArrowGEOSTest, EncodingTestFixture,
                         ::testing::Values(GEOARROW_GEOS_ENCODING_GEOARROW,
                                           GEOARROW_GEOS_ENCODING_GEOARROW_INTERLEAVED,
                                           GEOARROW_GEOS_ENCODING_WKB,
                                           GEOARROW_GEOS_ENCODING_WKT));
