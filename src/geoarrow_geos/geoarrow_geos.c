
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define GEOS_USE_ONLY_R_API
#include <geoarrow.h>
#include <geos_c.h>

#include "geoarrow_geos.h"

const char* GeoArrowGEOSVersionGEOS(void) { return GEOSversion(); }

const char* GeoArrowGEOSVersionGeoArrow(void) { return GeoArrowVersion(); }

struct GeoArrowGEOSArrayBuilder {
  GEOSContextHandle_t handle;
  struct GeoArrowError error;
  struct GeoArrowBuilder builder;
};

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderCreate(
    GEOSContextHandle_t handle, struct ArrowSchema* schema,
    struct GeoArrowGEOSArrayBuilder** out) {
  struct GeoArrowGEOSArrayBuilder* builder =
      (struct GeoArrowGEOSArrayBuilder*)malloc(sizeof(struct GeoArrowGEOSArrayBuilder));
  if (builder == NULL) {
    return ENOMEM;
  }

  memset(builder, 0, sizeof(struct GeoArrowGEOSArrayBuilder));
  *out = builder;
  return GeoArrowBuilderInitFromSchema(&builder->builder, schema, &builder->error);
}

void GeoArrowGEOSArrayBuilderDestroy(struct GeoArrowGEOSArrayBuilder* builder) {
  GeoArrowBuilderReset(&builder->builder);
  free(builder);
}

const char* GeoArrowGEOSArrayBuilderGetLastError(
    struct GeoArrowGEOSArrayBuilder* builder) {
  return builder->error.message;
}

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderFinish(
    struct GeoArrowGEOSArrayBuilder* builder, struct ArrowArray* out) {
  return GeoArrowBuilderFinish(&builder->builder, out, &builder->error);
}

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderAppend(
    struct GeoArrowGEOSArrayBuilder* builder, GEOSGeometry** geom, size_t geom_size,
    size_t* n_appended) {
  return ENOTSUP;
}
