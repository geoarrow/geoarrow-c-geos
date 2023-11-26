
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

class ArrowCppSchema {
 public:
  ArrowSchema schema;

  ArrowCppSchema() { schema.release = nullptr; }

  ~ArrowCppSchema() {
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
  }
};

class ArrowCppArray {
 public:
  ArrowArray array;

  ArrowCppArray() { array.release = nullptr; }

  ~ArrowCppArray() {
    if (array.release != nullptr) {
      array.release(&array);
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
  ArrowCppSchema schema;

  ASSERT_EQ(GeoArrowGEOSMakeSchema(GEOARROW_GEOS_ENCODING_WKT, 0, &schema.schema),
            GEOARROW_GEOS_OK);
  ASSERT_EQ(builder.Init(&schema.schema), GEOARROW_GEOS_OK);

  std::string wkt = "POINT (0 1)";
  ASSERT_EQ(reader.Read(wkt, &geom.ptr), GEOARROW_GEOS_OK);
  size_t n = 0;
  const GEOSGeometry* geom_const = geom.ptr;
  EXPECT_EQ(GeoArrowGEOSArrayBuilderAppend(builder.ptr, &geom_const, 1, &n),
            GEOARROW_GEOS_OK);
  ASSERT_EQ(n, 1);

  ArrowCppArray array;
  ASSERT_EQ(GeoArrowGEOSArrayBuilderFinish(builder.ptr, &array.array), GEOARROW_GEOS_OK);

  ASSERT_EQ(array.array.length, 1);
  ASSERT_EQ(array.array.n_buffers, 3);

  const auto offsets = reinterpret_cast<const int32_t*>(array.array.buffers[1]);
  const auto data = reinterpret_cast<const char*>(array.array.buffers[2]);
  std::string wkt_out(data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(wkt_out, wkt);
}
