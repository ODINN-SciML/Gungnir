"""Dated DEMs with their uncertainty, so that they can be used as observations of the surface."""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.merge import merge
from rasterio.warp import Resampling, reproject
from shapely.geometry import Polygon
from shapely.ops import unary_union
from oggm import utils

log = logging.getLogger(__name__)

# Public mirror of the Copernicus GLO-90 DEM. Each tile has its own folder with the DEM, the
# Height Error Mask and a map of the TanDEM-X acquisitions with their dates.
_copdem90_url = "https://copernicus-dem-90m.s3.amazonaws.com/"

_kml = "{http://www.opengis.net/kml/2.2}"


def decimal_year(date):
    """Date as a decimal year, e.g. 16 February 2000 -> 2000.126."""
    start = datetime(date.year, 1, 1)
    length = datetime(date.year + 1, 1, 1) - start
    return date.year + (date - start) / length


# NASADEM is the SRTM mission, 11 to 22 February 2000
nasadem_date = decimal_year(datetime(2000, 2, 16, 12))


def _copdem90_tiles(lon_ex, lat_ex):
    """Names of the GLO-90 tiles of an extent, like `Copernicus_DSM_30_N46_00_E008_00`.

    OGGM 1.6.2 returns the names. OGGM 1.6.3 returns the pairs `(url, name)`, with a `_DEM`
    suffix in the name and `None` as url when the tile does not exist (no file over the ocean).
    """
    names = []
    for tile in utils.copdem_zone(lon_ex, lat_ex, "COPDEM90"):
        if isinstance(tile, tuple):
            url, tile = tile
            if url is None:
                continue
        names.append(tile.removesuffix("_DEM"))
    return names


def _copdem90_files(tile):
    """URLs of the DEM, the Height Error Mask and the acquisition map of a tile."""
    cog = tile.replace("_DSM_30_", "_DSM_COG_30_")
    folder = f"{_copdem90_url}{cog}_DEM/"
    return (
        f"{folder}{cog}_DEM.tif",
        f"{folder}AUXFILES/{cog}_HEM.tif",
        f"{folder}PREVIEW/{tile}_SRC.kml",
    )


def _to_glacier_grid(gdir, files, resampling):
    """Mosaic of the tiles, reprojected onto the glacier grid."""
    datasets = [rasterio.open(f) for f in files]
    try:
        nodata = datasets[0].nodata
        data, transform = merge(datasets, nodata=nodata)
        crs = datasets[0].crs
    finally:
        for ds in datasets:
            ds.close()
    with rasterio.open(gdir.get_filepath("dem")) as tpl:
        out = np.full((tpl.height, tpl.width), np.nan, dtype=np.float32)
        reproject(
            source=data[0],
            destination=out,
            src_transform=transform,
            src_crs=crs,
            src_nodata=nodata,
            dst_transform=tpl.transform,
            dst_crs=tpl.crs,
            dst_nodata=np.nan,
            resampling=resampling,
        )
    return out


def _acquisitions(kml_files):
    """Footprint and date of each TanDEM-X acquisition. Neighbour tiles share acquisitions."""
    acquisitions = {}
    for kml in kml_files:
        for pm in ET.parse(kml).getroot().iter(f"{_kml}Placemark"):
            desc = pm.find(f"{_kml}description")
            if desc is None or "acquisition" not in desc.text:
                continue
            date = datetime.fromisoformat(desc.text.strip().split("\n")[-1])
            polygons = []
            for c in pm.iter(f"{_kml}coordinates"):
                points = [tuple(map(float, p.split(",")[:2])) for p in c.text.split()]
                if len(points) >= 3:
                    polygons.append(Polygon(points).buffer(0))
            name = pm.find(f"{_kml}name").text
            footprint = unary_union(polygons)
            if name in acquisitions:
                footprint = footprint.union(acquisitions[name][1])
            acquisitions[name] = (date, footprint)
    return acquisitions.values()


def _acquisition_dates(gdir, kml_files):
    """Mean date (decimal year) of the acquisitions that cover each cell of the glacier grid."""
    with rasterio.open(gdir.get_filepath("dem")) as tpl:
        shape, transform, crs = (tpl.height, tpl.width), tpl.transform, tpl.crs
    total = np.zeros(shape)
    count = np.zeros(shape)
    for date, footprint in _acquisitions(kml_files):
        geometry = gpd.GeoSeries([footprint], crs="EPSG:4326").to_crs(crs)
        covered = rasterize(geometry, out_shape=shape, transform=transform).astype(bool)
        total[covered] += decimal_year(date)
        count[covered] += 1
    with np.errstate(invalid="ignore"):
        return np.where(count > 0, total / count, np.nan).astype(np.float32)


@utils.entity_task(log, writes=["gridded_data"])
def copdem90_to_gdir(gdir):
    """Add the Copernicus GLO-90 DEM, its Height Error Mask and its dates to `gridded_data`.

    The Copernicus DEM is made of TanDEM-X acquisitions from 2011 to 2015. Each cell is the
    combination of several acquisitions, so `topo_copdem90_date` is the mean date of the
    acquisitions that cover the cell. `topo_copdem90_err` is the Height Error Mask (one
    standard deviation, random error only: it does not include radar penetration).
    Gaps of TanDEM-X (often steep slopes) are filled with other DEMs of other dates. They
    have no Height Error Mask, so they get no date either and should not be used.
    The date of `topo` is also written when it is NASADEM.
    """
    lon_ex, lat_ex = gdir.extent_ll
    dems, errors, maps = [], [], []
    for tile in _copdem90_tiles(lon_ex, lat_ex):
        dem, error, acquisition_map = _copdem90_files(tile)
        dem = utils.file_downloader(dem)
        # No file over the ocean
        if dem is None:
            continue
        error = utils.file_downloader(error)
        acquisition_map = utils.file_downloader(acquisition_map)
        if error is None or acquisition_map is None:
            raise RuntimeError(
                f"The Height Error Mask or the acquisition map of the tile {tile} "
                f"is missing for {gdir.rgi_id}"
            )
        dems.append(dem)
        errors.append(error)
        maps.append(acquisition_map)
    if not dems:
        raise RuntimeError(f"No Copernicus GLO-90 tile found for {gdir.rgi_id}")

    error = _to_glacier_grid(gdir, errors, Resampling.bilinear)
    date = _acquisition_dates(gdir, maps)
    date[np.isnan(error)] = np.nan
    variables = {
        "topo_copdem90": (
            _to_glacier_grid(gdir, dems, Resampling.bilinear),
            "m",
            "Copernicus GLO-90 DEM topography",
            dems,
        ),
        "topo_copdem90_err": (
            error,
            "m",
            "Height Error Mask of the Copernicus GLO-90 DEM (one standard deviation)",
            errors,
        ),
        "topo_copdem90_date": (
            date,
            "yr",
            "Mean date of the TanDEM-X acquisitions of the Copernicus GLO-90 DEM (decimal year)",
            maps,
        ),
    }
    with utils.ncDataset(gdir.get_filepath("gridded_data"), "a") as nc:
        for name, (data, units, long_name, sources) in variables.items():
            if name in nc.variables:
                v = nc.variables[name]
            else:
                v = nc.createVariable(name, "f4", ("y", "x"), zlib=True)
            v.units = units
            v.long_name = long_name
            v.data_source = " ".join(sources)
            v[:] = data
        dem_source = gdir.get_diagnostics().get("dem_source")
        if dem_source == "NASADEM":
            nc.variables["topo"].date = nasadem_date
        else:
            log.warning(
                "The DEM of %s is %s, so `topo` has no date and only the Copernicus DEM "
                "is a dated surface",
                gdir.rgi_id,
                dem_source,
            )
