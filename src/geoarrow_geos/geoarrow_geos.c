
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define GEOS_USE_ONLY_R_API
#include <geoarrow.h>
#include <geos_c.h>

#include "geoarrow_geos.h"

// These should really be in the geoarrow header
#define _GEOARROW_CONCAT(x, y) x##y
#define _GEOARROW_MAKE_NAME(x, y) _GEOARROW_CONCAT(x, y)

#define _GEOARROW_RETURN_NOT_OK_IMPL(NAME, EXPR) \
  do {                                           \
    const int NAME = (EXPR);                     \
    if (NAME) return NAME;                       \
  } while (0)

#define GEOARROW_RETURN_NOT_OK(EXPR) \
  _GEOARROW_RETURN_NOT_OK_IMPL(_GEOARROW_MAKE_NAME(errno_status_, __COUNTER__), EXPR)

const char* GeoArrowGEOSVersionGEOS(void) { return GEOSversion(); }

const char* GeoArrowGEOSVersionGeoArrow(void) { return GeoArrowVersion(); }

struct GeoArrowGEOSArrayBuilder {
  GEOSContextHandle_t handle;
  struct GeoArrowError error;
  struct GeoArrowBuilder builder;
  struct GeoArrowWKTWriter wkt_writer;
  struct GeoArrowWKBWriter wkb_writer;
  struct GeoArrowVisitor v;
  struct GeoArrowCoordView coords_view;
  double* coords;
};

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderCreate(
    GEOSContextHandle_t handle, struct ArrowSchema* schema,
    struct GeoArrowGEOSArrayBuilder** out) {
  struct GeoArrowGEOSArrayBuilder* builder =
      (struct GeoArrowGEOSArrayBuilder*)malloc(sizeof(struct GeoArrowGEOSArrayBuilder));
  if (builder == NULL) {
    *out = NULL;
    return ENOMEM;
  }

  memset(builder, 0, sizeof(struct GeoArrowGEOSArrayBuilder));
  *out = builder;

  struct GeoArrowSchemaView schema_view;
  GEOARROW_RETURN_NOT_OK(GeoArrowSchemaViewInit(&schema_view, schema, &builder->error));
  switch (schema_view.type) {
    case GEOARROW_TYPE_WKT:
      GEOARROW_RETURN_NOT_OK(GeoArrowWKTWriterInit(&builder->wkt_writer));
      GeoArrowWKTWriterInitVisitor(&builder->wkt_writer, &builder->v);
      break;
    case GEOARROW_TYPE_WKB:
      GEOARROW_RETURN_NOT_OK(GeoArrowWKBWriterInit(&builder->wkb_writer));
      GeoArrowWKBWriterInitVisitor(&builder->wkb_writer, &builder->v);
      break;
    default:
      GEOARROW_RETURN_NOT_OK(
          GeoArrowBuilderInitFromSchema(&builder->builder, schema, &builder->error));
      GEOARROW_RETURN_NOT_OK(GeoArrowBuilderInitVisitor(&builder->builder, &builder->v));
      break;
  }

  builder->handle = handle;
  builder->v.error = &builder->error;
  return GEOARROW_OK;
}

GeoArrowGEOSErrorCode GeoArrowGEOSMakeSchema(int32_t encoding, int32_t wkb_type,
                                             struct ArrowSchema* out) {
  enum GeoArrowType type = GEOARROW_TYPE_UNINITIALIZED;
  enum GeoArrowGeometryType geometry_type = GEOARROW_GEOMETRY_TYPE_GEOMETRY;
  enum GeoArrowDimensions dimensions = GEOARROW_DIMENSIONS_UNKNOWN;
  enum GeoArrowCoordType coord_type = GEOARROW_COORD_TYPE_UNKNOWN;

  switch (encoding) {
    case GEOARROW_GEOS_ENCODING_WKT:
      type = GEOARROW_TYPE_WKT;
      break;
    case GEOARROW_GEOS_ENCODING_WKB:
      type = GEOARROW_TYPE_WKB;
      break;
    case GEOARROW_GEOS_ENCODING_GEOARROW:
      coord_type = GEOARROW_COORD_TYPE_SEPARATE;
      break;
    case GEOARROW_GEOS_ENCODING_GEOARROW_INTERLEAVED:
      coord_type = GEOARROW_COORD_TYPE_INTERLEAVED;
      break;
    default:
      return EINVAL;
  }

  if (type == GEOARROW_TYPE_UNINITIALIZED) {
    geometry_type = wkb_type % 1000;
    dimensions = wkb_type / 1000 + 1;
    type = GeoArrowMakeType(geometry_type, dimensions, GEOARROW_COORD_TYPE_SEPARATE);
  }

  GEOARROW_RETURN_NOT_OK(GeoArrowSchemaInitExtension(out, type));
  return GEOARROW_OK;
}

static GeoArrowErrorCode GeoArrowGEOSArrayBuilderEnsureCoords(
    struct GeoArrowGEOSArrayBuilder* builder, uint32_t n_coords, int n_dims) {
  int64_t n_required = n_coords * n_dims;
  int64_t n_current = builder->coords_view.n_coords * builder->coords_view.n_values;
  if (n_required > n_current) {
    if ((n_current * 2) > n_required) {
      n_required = n_current * 2;
    }

    builder->coords = (double*)realloc(builder->coords, n_required * sizeof(double));
    if (builder->coords == NULL) {
      builder->coords_view.n_coords = 0;
      return ENOMEM;
    }
  }

  builder->coords_view.n_values = n_dims;
  builder->coords_view.coords_stride = n_dims;
  for (int i = 0; i < n_dims; i++) {
    builder->coords_view.values[i] = builder->coords + i;
  }

  return GEOARROW_OK;
}

void GeoArrowGEOSArrayBuilderDestroy(struct GeoArrowGEOSArrayBuilder* builder) {
  if (builder->coords != NULL) {
    free(builder->coords);
  }

  if (builder->builder.private_data != NULL) {
    GeoArrowBuilderReset(&builder->builder);
  }

  if (builder->wkt_writer.private_data != NULL) {
    GeoArrowWKTWriterReset(&builder->wkt_writer);
  }

  if (builder->wkb_writer.private_data != NULL) {
    GeoArrowWKBWriterReset(&builder->wkb_writer);
  }

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

static GeoArrowErrorCode VisitCoords(struct GeoArrowGEOSArrayBuilder* builder,
                                     const GEOSCoordSequence* seq,
                                     struct GeoArrowVisitor* v) {
  unsigned int size = 0;
  int result = GEOSCoordSeq_getSize_r(builder->handle, seq, &size);
  if (result == 0) {
    GeoArrowErrorSet(v->error, "GEOSCoordSeq_getSize_r() failed");
    return ENOMEM;
  }

  if (size == 0) {
    return GEOARROW_OK;
  }

  unsigned int dims = 0;
  result = GEOSCoordSeq_getDimensions_r(builder->handle, seq, &dims);
  if (result == 0) {
    GeoArrowErrorSet(v->error, "GEOSCoordSeq_getDimensions_r() failed");
    return ENOMEM;
  }

  // Make sure we have enough space to copy the coordinates into
  GEOARROW_RETURN_NOT_OK(GeoArrowGEOSArrayBuilderEnsureCoords(builder, size, dims));

  // Not sure exactly how M coordinates work in GEOS yet
  result =
      GEOSCoordSeq_copyToBuffer_r(builder->handle, seq, builder->coords, dims == 3, 0);
  if (result == 0) {
    GeoArrowErrorSet(v->error, "GEOSCoordSeq_copyToBuffer_r() failed");
    return ENOMEM;
  }

  // Call the visitor method
  GEOARROW_RETURN_NOT_OK(v->coords(v, &builder->coords_view));

  return GEOARROW_OK;
}

static GeoArrowErrorCode VisitGeometry(struct GeoArrowGEOSArrayBuilder* builder,
                                       const GEOSGeometry* geom,
                                       struct GeoArrowVisitor* v) {
  if (geom == NULL) {
    GEOARROW_RETURN_NOT_OK(v->null_feat(v));
    return GEOARROW_OK;
  }

  int type_id = GEOSGeomTypeId_r(builder->handle, geom);
  int coord_dimension = GEOSGeom_getCoordinateDimension_r(builder->handle, geom);

  enum GeoArrowGeometryType geoarrow_type = GEOARROW_GEOMETRY_TYPE_GEOMETRY;
  enum GeoArrowDimensions geoarrow_dims = GEOARROW_DIMENSIONS_UNKNOWN;

  // Not sure how M dimensions work yet
  switch (coord_dimension) {
    case 2:
      geoarrow_dims = GEOARROW_DIMENSIONS_XY;
      break;
    case 3:
      geoarrow_dims = GEOARROW_DIMENSIONS_XYZ;
      break;
    default:
      GeoArrowErrorSet(v->error, "Unexpected GEOSGeom_getCoordinateDimension_r: %d",
                       coord_dimension);
      return EINVAL;
  }

  switch (type_id) {
    case GEOS_POINT:
      geoarrow_type = GEOARROW_GEOMETRY_TYPE_POINT;
      break;
    case GEOS_LINESTRING:
    case GEOS_LINEARRING:
      geoarrow_type = GEOARROW_GEOMETRY_TYPE_LINESTRING;
      break;
    case GEOS_POLYGON:
      geoarrow_type = GEOARROW_GEOMETRY_TYPE_POLYGON;
      break;
    case GEOS_MULTIPOINT:
      geoarrow_type = GEOARROW_GEOMETRY_TYPE_MULTIPOINT;
      break;
    case GEOS_MULTILINESTRING:
      geoarrow_type = GEOARROW_GEOMETRY_TYPE_MULTILINESTRING;
      break;
    case GEOS_MULTIPOLYGON:
      geoarrow_type = GEOARROW_GEOMETRY_TYPE_MULTIPOLYGON;
      break;
    case GEOS_GEOMETRYCOLLECTION:
      geoarrow_type = GEOARROW_GEOMETRY_TYPE_GEOMETRYCOLLECTION;
      break;
    default:
      GeoArrowErrorSet(v->error, "Unexpected GEOSGeomTypeId: %d", type_id);
      return EINVAL;
  }

  GEOARROW_RETURN_NOT_OK(v->geom_start(v, geoarrow_type, geoarrow_dims));

  switch (type_id) {
    case GEOS_POINT:
    case GEOS_LINESTRING:
    case GEOS_LINEARRING: {
      const GEOSCoordSequence* seq = GEOSGeom_getCoordSeq_r(builder->handle, geom);
      if (seq == NULL) {
        GeoArrowErrorSet(v->error, "GEOSGeom_getCoordSeq_r() failed");
        return ENOMEM;
      }

      GEOARROW_RETURN_NOT_OK(VisitCoords(builder, seq, v));
      break;
    }

    case GEOS_POLYGON: {
      const GEOSGeometry* ring = GEOSGetExteriorRing_r(builder->handle, geom);
      if (ring == NULL) {
        GeoArrowErrorSet(v->error, "GEOSGetExteriorRing_r() failed");
        return ENOMEM;
      }

      GEOARROW_RETURN_NOT_OK(v->ring_start(v));
      const GEOSCoordSequence* seq = GEOSGeom_getCoordSeq_r(builder->handle, geom);
      if (seq == NULL) {
        GeoArrowErrorSet(v->error, "GEOSGeom_getCoordSeq_r() failed");
        return ENOMEM;
      }

      GEOARROW_RETURN_NOT_OK(VisitCoords(builder, seq, v));
      GEOARROW_RETURN_NOT_OK(v->ring_end(v));

      int size = GEOSGetNumInteriorRings_r(builder->handle, geom);
      for (int i = 0; i < size; i++) {
        ring = GEOSGetInteriorRingN_r(builder->handle, geom, i);
        if (ring == NULL) {
          GeoArrowErrorSet(v->error, "GEOSGetInteriorRingN_r() failed");
          return ENOMEM;
        }

        GEOARROW_RETURN_NOT_OK(v->ring_start(v));
        seq = GEOSGeom_getCoordSeq_r(builder->handle, geom);
        if (seq == NULL) {
          GeoArrowErrorSet(v->error, "GEOSGeom_getCoordSeq_r() failed");
          return ENOMEM;
        }

        GEOARROW_RETURN_NOT_OK(VisitCoords(builder, seq, v));
        GEOARROW_RETURN_NOT_OK(v->ring_end(v));
      }

      break;
    }

    case GEOS_MULTIPOINT:
    case GEOS_MULTILINESTRING:
    case GEOS_MULTIPOLYGON:
    case GEOS_GEOMETRYCOLLECTION: {
      int size = GEOSGetNumGeometries_r(builder->handle, geom);
      for (int i = 0; i < size; i++) {
        const GEOSGeometry* child = GEOSGetGeometryN_r(builder->handle, child, i);
        if (child == NULL) {
          GeoArrowErrorSet(v->error, "GEOSGetGeometryN_r() failed");
          return ENOMEM;
        }

        GEOARROW_RETURN_NOT_OK(VisitGeometry(builder, child, v));
      }
    }
    default:
      GeoArrowErrorSet(v->error, "Unexpected GEOSGeomTypeId: %d", type_id);
      return EINVAL;
  }

  GEOARROW_RETURN_NOT_OK(v->geom_end(v));
  return GEOARROW_OK;
}

GeoArrowGEOSErrorCode GeoArrowGEOSArrayBuilderAppend(
    struct GeoArrowGEOSArrayBuilder* builder, const GEOSGeometry** geom, size_t geom_size,
    size_t* n_appended) {
  *n_appended = 0;

  for (size_t i = 0; i < geom_size; i++) {
    GEOARROW_RETURN_NOT_OK(builder->v.feat_start(&builder->v));
    GEOARROW_RETURN_NOT_OK(VisitGeometry(builder, geom[i], &builder->v));
    GEOARROW_RETURN_NOT_OK(builder->v.feat_start(&builder->v));
    *n_appended = i + 1;
  }

  return GEOARROW_OK;
}
