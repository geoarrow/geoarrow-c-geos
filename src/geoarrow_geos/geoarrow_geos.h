
#ifndef GEOARROW_GEOS_H_INCLUDED
#define GEOARROW_GEOS_H_INCLUDED

#include <geos_c.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Extra guard for versions of Arrow without the canonical guard
#ifndef ARROW_FLAG_DICTIONARY_ORDERED

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#endif

#define GEOARROW_GEOS_OK 0

enum GeoArrowGEOSEncoding {
  GEOARROW_GEOS_ENCODING_UNKNOWN = 0,
  GEOARROW_GEOS_ENCODING_WKT,
  GEOARROW_GEOS_ENCODING_WKB,
  GEOARROW_GEOS_ENCODING_GEOARROW,
  GEOARROW_GEOS_ENCODING_GEOARROW_INTERLEAVED
};

typedef int GeoArrowGEOSErrorCode;

const char* GeoArrowGEOSVersionGEOS(void);

const char* GeoArrowGEOSVersionGeoArrow(void);

struct GeoArrowGEOSArrayBuilder;

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderCreate(
    GEOSContextHandle_t handle, struct ArrowSchema* schema,
    struct GeoArrowGEOSArrayBuilder** out);

GeoArrowGEOSErrorCode GeoArrowGEOSMakeSchema(int32_t encoding, int32_t wkb_type,
                                             struct ArrowSchema* out);

void GeoArrowGEOSArrayBuilderDestroy(struct GeoArrowGEOSArrayBuilder* builder);

const char* GeoArrowGEOSArrayBuilderGetLastError(
    struct GeoArrowGEOSArrayBuilder* builder);

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderAppend(
    struct GeoArrowGEOSArrayBuilder* builder, const GEOSGeometry** geom, size_t geom_size,
    size_t* n_appended);

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderFinish(
    struct GeoArrowGEOSArrayBuilder* builder, struct ArrowArray* out);

struct GeoArrowGEOSArrayReader;

GeoArrowGEOSErrorCode GeoArrowGEOSArrayReaderCreate(GEOSContextHandle_t handle,
                                                    struct ArrowSchema* schema,
                                                    struct GeoArrowGEOSArrayReader** out);

const char* GeoArrowGEOSArrayReaderGetLastError(struct GeoArrowGEOSArrayReader* reader);

GeoArrowGEOSErrorCode GeoArrowGEOSArrayReaderRead(struct GeoArrowGEOSArrayReader* reader,
                                                  struct ArrowArray* array, size_t offset,
                                                  size_t length, GEOSGeometry* out);

void GeoArrowGEOSArrayReaderDestroy(struct GeoArrowGEOSArrayReader* reader);

#ifdef __cplusplus
}
#endif

#endif
