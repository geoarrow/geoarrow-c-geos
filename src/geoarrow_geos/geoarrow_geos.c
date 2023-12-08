
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

  builder->coords_view.n_coords = n_coords;
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
  if (builder->wkt_writer.private_data != NULL) {
    return GeoArrowWKTWriterFinish(&builder->wkt_writer, out, &builder->error);
  } else if (builder->wkb_writer.private_data != NULL) {
    return GeoArrowWKBWriterFinish(&builder->wkb_writer, out, &builder->error);
  } else if (builder->builder.private_data != NULL) {
    return GeoArrowBuilderFinish(&builder->builder, out, &builder->error);
  } else {
    GeoArrowErrorSet(&builder->error, "Invalid state");
    return EINVAL;
  }
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
      if (GEOSisEmpty_r(builder->handle, geom)) {
        break;
      }

      const GEOSGeometry* ring = GEOSGetExteriorRing_r(builder->handle, geom);
      if (ring == NULL) {
        GeoArrowErrorSet(v->error, "GEOSGetExteriorRing_r() failed");
        return ENOMEM;
      }

      GEOARROW_RETURN_NOT_OK(v->ring_start(v));
      const GEOSCoordSequence* seq = GEOSGeom_getCoordSeq_r(builder->handle, ring);
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
        seq = GEOSGeom_getCoordSeq_r(builder->handle, ring);
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
        const GEOSGeometry* child = GEOSGetGeometryN_r(builder->handle, geom, i);
        if (child == NULL) {
          GeoArrowErrorSet(v->error, "GEOSGetGeometryN_r() failed");
          return ENOMEM;
        }

        GEOARROW_RETURN_NOT_OK(VisitGeometry(builder, child, v));
      }

      break;
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
    GEOARROW_RETURN_NOT_OK(builder->v.feat_end(&builder->v));
    *n_appended = i + 1;
  }

  return GEOARROW_OK;
}

struct GeoArrowGEOSArrayReader {
  GEOSContextHandle_t handle;
  struct GeoArrowError error;
  struct GeoArrowArrayView array_view;
  // In order to use GeoArrow's read capability we need to write a visitor-based
  // constructor for GEOS geometries, which is complicated and may or may not be
  // faster than GEOS' own readers.
  GEOSWKTReader* wkt_reader;
  GEOSWKBReader* wkb_reader;
  // In-progress items that we might need to clean up if an error was returned
  int64_t n_geoms[2];
  GEOSGeometry** geoms[2];
};

static GeoArrowErrorCode GeoArrowGEOSArrayReaderEnsureScratch(
    struct GeoArrowGEOSArrayReader* reader, int64_t n_geoms, int level) {
  if (n_geoms <= reader->n_geoms[level]) {
    return GEOARROW_OK;
  }

  if ((reader->n_geoms[level] * 2) > n_geoms) {
    n_geoms = reader->n_geoms[level] * 2;
  }

  reader->geoms[level] =
      (GEOSGeometry**)realloc(reader->geoms[level], n_geoms * sizeof(GEOSGeometry*));
  if (reader->geoms[level] == NULL) {
    reader->n_geoms[level] = 0;
    return ENOMEM;
  }

  memset(reader->geoms[level], 0, n_geoms * sizeof(GEOSGeometry*));
  return GEOARROW_OK;
}

static void GeoArrowGEOSArrayReaderResetScratch(struct GeoArrowGEOSArrayReader* reader) {
  for (int level = 0; level < 2; level++) {
    for (int64_t i = 0; i < reader->n_geoms[level]; i++) {
      if (reader->geoms[level][i] != NULL) {
        GEOSGeom_destroy_r(reader->handle, reader->geoms[level][i]);
        reader->geoms[level][i] = NULL;
      }
    }
  }
}

GeoArrowGEOSErrorCode GeoArrowGEOSArrayReaderCreate(
    GEOSContextHandle_t handle, struct ArrowSchema* schema,
    struct GeoArrowGEOSArrayReader** out) {
  struct GeoArrowGEOSArrayReader* reader =
      (struct GeoArrowGEOSArrayReader*)malloc(sizeof(struct GeoArrowGEOSArrayReader));
  if (reader == NULL) {
    *out = NULL;
    return ENOMEM;
  }

  memset(reader, 0, sizeof(struct GeoArrowGEOSArrayReader));
  *out = reader;

  reader->handle = handle;
  GEOARROW_RETURN_NOT_OK(
      GeoArrowArrayViewInitFromSchema(&reader->array_view, schema, &reader->error));

  return GEOARROW_OK;
}

const char* GeoArrowGEOSArrayReaderGetLastError(struct GeoArrowGEOSArrayReader* reader) {
  return reader->error.message;
}

static GeoArrowErrorCode MakeCoordSeq(struct GeoArrowGEOSArrayReader* reader,
                                      size_t offset, size_t length,
                                      GEOSCoordSequence** out) {
  offset += reader->array_view.offset[reader->array_view.n_offsets];
  struct GeoArrowCoordView* coords = &reader->array_view.coords;
  const double* z = NULL;
  const double* m = NULL;

  switch (reader->array_view.schema_view.dimensions) {
    case GEOARROW_DIMENSIONS_XYZ:
      z = coords->values[2];
      break;
    case GEOARROW_DIMENSIONS_XYM:
      m = coords->values[2];
      break;
    case GEOARROW_DIMENSIONS_XYZM:
      z = coords->values[2];
      m = coords->values[3];
      break;
    default:
      break;
  }

  GEOSCoordSequence* seq;

  switch (reader->array_view.schema_view.coord_type) {
    case GEOARROW_COORD_TYPE_SEPARATE:
      seq = GEOSCoordSeq_copyFromArrays_r(reader->handle, coords->values[0] + offset,
                                          coords->values[1] + offset, z, m, length);
      break;
    case GEOARROW_COORD_TYPE_INTERLEAVED:
      seq = GEOSCoordSeq_copyFromBuffer_r(reader->handle,
                                          coords->values[0] + (offset * coords->n_values),
                                          length, z != NULL, m != NULL);
      break;
    default:
      GeoArrowErrorSet(&reader->error, "Unsupported coord type");
      return ENOTSUP;
  }

  if (seq == NULL) {
    GeoArrowErrorSet(&reader->error, "GEOSCoordSeq_copyFromArrays_r() failed");
    return ENOMEM;
  }

  *out = seq;
  return GEOARROW_OK;
}

static GeoArrowErrorCode MakePoints(struct GeoArrowGEOSArrayReader* reader, size_t offset,
                                    size_t length, GEOSGeometry** out) {
  GEOSCoordSequence* seq = NULL;
  for (size_t i = 0; i < length; i++) {
    GEOARROW_RETURN_NOT_OK(MakeCoordSeq(reader, offset + i, 1, &seq));
    out[i] = GEOSGeom_createPoint_r(reader->handle, seq);
    if (out[i] == NULL) {
      GEOSCoordSeq_destroy_r(reader->handle, seq);
      GeoArrowErrorSet(&reader->error, "[%ld] GEOSGeom_createPoint_r() failed", (long)i);
      return ENOMEM;
    }
  }

  return GEOARROW_OK;
}

static GeoArrowErrorCode MakeLinestrings(struct GeoArrowGEOSArrayReader* reader,
                                         size_t offset, size_t length,
                                         GEOSGeometry** out) {
  offset += reader->array_view.offset[reader->array_view.n_offsets - 1];
  const int32_t* coord_offsets =
      reader->array_view.offsets[reader->array_view.n_offsets - 1];

  GEOSCoordSequence* seq = NULL;
  for (size_t i = 0; i < length; i++) {
    GEOARROW_RETURN_NOT_OK(
        MakeCoordSeq(reader, coord_offsets[offset + i],
                     coord_offsets[offset + i + 1] - coord_offsets[offset + i], &seq));
    out[i] = GEOSGeom_createLineString_r(reader->handle, seq);
    if (out[i] == NULL) {
      GEOSCoordSeq_destroy_r(reader->handle, seq);
      GeoArrowErrorSet(&reader->error, "[%ld] GEOSGeom_createLineString_r() failed",
                       (long)i);
      return ENOMEM;
    }
  }

  return GEOARROW_OK;
}

static GeoArrowErrorCode MakeLinearrings(struct GeoArrowGEOSArrayReader* reader,
                                         size_t offset, size_t length,
                                         GEOSGeometry** out) {
  offset += reader->array_view.offset[reader->array_view.n_offsets - 1];
  const int32_t* coord_offsets =
      reader->array_view.offsets[reader->array_view.n_offsets - 1];

  GEOSCoordSequence* seq = NULL;
  for (size_t i = 0; i < length; i++) {
    GEOARROW_RETURN_NOT_OK(
        MakeCoordSeq(reader, coord_offsets[offset + i],
                     coord_offsets[offset + i + 1] - coord_offsets[offset + i], &seq));
    out[i] = GEOSGeom_createLinearRing_r(reader->handle, seq);
    if (out[i] == NULL) {
      GEOSCoordSeq_destroy_r(reader->handle, seq);
      GeoArrowErrorSet(&reader->error, "[%ld] GEOSGeom_createLinearRing_r() failed",
                       (long)i);
      return ENOMEM;
    }
  }

  return GEOARROW_OK;
}

static GeoArrowErrorCode MakePolygons(struct GeoArrowGEOSArrayReader* reader,
                                      size_t offset, size_t length, GEOSGeometry** out) {
  offset += reader->array_view.offset[reader->array_view.n_offsets - 1];
  const int32_t* ring_offsets =
      reader->array_view.offsets[reader->array_view.n_offsets - 2];

  for (size_t i = 0; i < length; i++) {
    int64_t ring_offset = ring_offsets[offset + i];
    int64_t n_rings = ring_offsets[offset + i + 1] - ring_offset;

    if (n_rings == 0) {
      out[i] = GEOSGeom_createEmptyPolygon_r(reader->handle);
    } else {
      GEOARROW_RETURN_NOT_OK(GeoArrowGEOSArrayReaderEnsureScratch(reader, n_rings, 0));
      GEOARROW_RETURN_NOT_OK(
          MakeLinearrings(reader, ring_offset, n_rings, reader->geoms[0]));
      out[i] = GEOSGeom_createPolygon_r(reader->handle, reader->geoms[0][0],
                                        reader->geoms[0] + 1, n_rings - 1);
      memset(reader->geoms, 0, n_rings * sizeof(GEOSGeometry*));
    }

    if (out[i] == NULL) {
      GeoArrowErrorSet(&reader->error, "[%ld] GEOSGeom_createPolygon_r() failed",
                       (long)i);
      return ENOMEM;
    }
  }

  return GEOARROW_OK;
}

GeoArrowGEOSErrorCode GeoArrowGEOSArrayReaderRead(struct GeoArrowGEOSArrayReader* reader,
                                                  struct ArrowArray* array, size_t offset,
                                                  size_t length, GEOSGeometry** out) {
  GeoArrowGEOSArrayReaderResetScratch(reader);

  GEOARROW_RETURN_NOT_OK(
      GeoArrowArrayViewSetArray(&reader->array_view, array, &reader->error));

  memset(out, 0, sizeof(GEOSGeometry*) * length);

  GeoArrowErrorCode result;
  switch (reader->array_view.schema_view.geometry_type) {
    case GEOARROW_GEOMETRY_TYPE_POINT:
      result = MakePoints(reader, offset, length, out);
      break;
    case GEOARROW_GEOMETRY_TYPE_LINESTRING:
      result = MakeLinestrings(reader, offset, length, out);
      break;
    case GEOARROW_GEOMETRY_TYPE_POLYGON:
      result = MakePolygons(reader, offset, length, out);
      break;
    default:
      GeoArrowErrorSet(&reader->error,
                       "GeoArrowGEOSArrayReaderRead not implemented for geometry type");
      return ENOTSUP;
  }

  // If we failed, clean up any allocated geometries
  if (result != GEOARROW_OK) {
    for (size_t i = 0; i < length; i++) {
      if (out[i] != NULL) {
        GEOSGeom_destroy_r(reader->handle, out[i]);
        out[i] = NULL;
      }
    }
  }

  return result;
}

void GeoArrowGEOSArrayReaderDestroy(struct GeoArrowGEOSArrayReader* reader) {
  if (reader->wkt_reader != NULL) {
    GEOSWKTReader_destroy_r(reader->handle, reader->wkt_reader);
  }

  if (reader->wkb_reader != NULL) {
    GEOSWKBReader_destroy_r(reader->handle, reader->wkb_reader);
  }

  GeoArrowGEOSArrayReaderResetScratch(reader);

  for (int i = 0; i < 2; i++) {
    if (reader->geoms[i] != NULL) {
      free(reader->geoms[i]);
    }
  }

  free(reader);
}
