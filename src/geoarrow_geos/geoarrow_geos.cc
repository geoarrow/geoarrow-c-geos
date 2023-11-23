
#define GEOS_USE_ONLY_R_API

#include <geos_c.h>
#include <geoarrow.h>

#include "geoarrow_geos.h"

const char* GeoArrowGEOSVersionGEOS(void) { return GEOSversion(); }

const char* GeoArrowGEOSVersionGeoArrow(void) { return GeoArrowVersion(); }
