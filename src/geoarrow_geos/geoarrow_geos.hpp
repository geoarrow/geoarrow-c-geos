
#include "geoarrow_geos.h"

namespace geoarrow {

namespace geos {

class GeometryVector {
 public:
  GeometryVector(GEOSContextHandle_t handle) : handle_(handle) {}

  void reset(size_t offset, size_t length = 1) {
    for (size_t i = 0; i < length; i++) {
      GEOSGeometry* item = data_[offset + i];
      if (item != nullptr) {
        GEOSGeom_destroy_r(handle_, item);
      }
    }
  }

  ~GeometryVector() { reset(0, data_.size()); }

  void reserve(size_t n) { data_.reserve(n); }

  size_t size() { return data_.size(); }

  GEOSGeometry* take_ownership_of(size_t i) {
    GEOSGeometry* item = data_[i];
    reset(i);
    return item;
  }

  const GEOSGeometry* borrow(size_t i) { return data_[i]; }

  const GEOSGeometry** data() { return const_cast<const GEOSGeometry**>(data_.data()); }

  GEOSGeometry** mutable_data() { return data_.data(); }

  void resize(size_t n) {
    size_t current_size = size();
    if (n >= current_size) {
      data_.resize(n);
      for (size_t i = current_size; i < n; i++) {
        data_[i] = nullptr;
      }
    } else {
      reset(n, current_size - n);
      data_.resize(n);
    }
  }

 private:
  GEOSContextHandle_t handle_;
  std::vector<GEOSGeometry*> data_;
};

class ArrayBuilder {
 private:
  GeoArrowGEOSArrayBuilder* builder_;
};

class ArrayReader {
  ArrayReader() : reader_(nullptr) {}

  ~ArrayReader() {
    if (reader_ != nullptr) {
      GeoArrowGEOSArrayReaderDestroy(reader_);
    }
  }

  const char* GetLastError() {
    if (reader_ == nullptr) {
      return "";
    } else {
      return GeoArrowGEOSArrayReaderGetLastError(reader_);
    }
  }

  GeoArrowGEOSErrorCode InitFromEncoding(GEOSContextHandle_t handle,
                                         GeoArrowGEOSEncoding encoding,
                                         int wkb_type = 0) {
    ArrowSchema tmp_schema;
    tmp_schema.release = nullptr;
    int result = GeoArrowGEOSMakeSchema(encoding, wkb_type, &tmp_schema);
    if (result != GEOARROW_GEOS_OK) {
      return result;
    }

    result = InitFromSchema(handle, &tmp_schema);
    tmp_schema.release(&tmp_schema);
    return result;
  }

  GeoArrowGEOSErrorCode InitFromSchema(GEOSContextHandle_t handle, ArrowSchema* schema) {
    if (reader_ != nullptr) {
      GeoArrowGEOSArrayReaderDestroy(reader_);
    }

    return GeoArrowGEOSArrayReaderCreate(handle, schema, &reader_);
  }

  GeoArrowGEOSErrorCode Read(ArrowArray* array, int64_t offset, int64_t length,
                             GEOSGeometry** out) {
    return GeoArrowGEOSArrayReaderRead(reader_, array, offset, length, out);
  }

 private:
  GeoArrowGEOSArrayReader* reader_;
};

}  // namespace geos

}  // namespace geoarrow
