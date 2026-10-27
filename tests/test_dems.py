"""
Tests of the dated DEMs (`gungnir.dems`) that need no network: the names of the tiles of both
OGGM versions, the files of a tile, the dates, and the acquisition map of a tile.
"""

from datetime import datetime

from gungnir import dems

TILE = "Copernicus_DSM_30_N46_00_E008_00"


def test_tiles_of_both_oggm_versions(monkeypatch):
    # OGGM 1.6.2 returns the names
    monkeypatch.setattr(dems.utils, "copdem_zone", lambda lon, lat, source: [TILE])
    assert dems._copdem90_tiles((8, 9), (46, 47)) == [TILE]
    # OGGM 1.6.3 returns (url, name) with a `_DEM` suffix, and no url over the ocean
    pairs = [
        ("https://a/tile.tif", TILE + "_DEM"),
        (None, "Copernicus_DSM_30_N47_00_E008_00_DEM"),
    ]
    monkeypatch.setattr(dems.utils, "copdem_zone", lambda lon, lat, source: pairs)
    assert dems._copdem90_tiles((8, 9), (46, 47)) == [TILE]


def test_files_of_a_tile():
    dem, error, acquisitions = dems._copdem90_files(TILE)
    cog = "Copernicus_DSM_COG_30_N46_00_E008_00"
    assert dem.endswith(f"{cog}_DEM/{cog}_DEM.tif")
    assert error.endswith(f"{cog}_DEM/AUXFILES/{cog}_HEM.tif")
    assert acquisitions.endswith(f"{cog}_DEM/PREVIEW/{TILE}_SRC.kml")


def test_dates():
    assert abs(dems.decimal_year(datetime(2000, 1, 1)) - 2000.0) < 1e-12
    assert abs(dems.decimal_year(datetime(2001, 7, 2, 12)) - 2001.5) < 1e-3
    # The SRTM mission, 11 to 22 February 2000
    assert 2000.10 < dems.nasadem_date < 2000.15


def test_acquisitions_of_a_tile(tmp_path):
    kml = tmp_path / "tile_SRC.kml"
    kml.write_text("""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Document>
  <Placemark><name>acq_1</name>
    <description>TanDEM-X acquisition
2012-05-04</description>
    <Polygon><outerBoundaryIs><LinearRing><coordinates>
      8.0,46.0,0 9.0,46.0,0 9.0,47.0,0 8.0,47.0,0 8.0,46.0,0
    </coordinates></LinearRing></outerBoundaryIs></Polygon>
  </Placemark>
  <Placemark><name>not_an_acquisition</name><description>mosaic</description></Placemark>
</Document></kml>""")
    acquisitions = list(dems._acquisitions([str(kml)]))
    assert len(acquisitions) == 1
    date, footprint = acquisitions[0]
    assert date == datetime(2012, 5, 4)
    assert abs(footprint.area - 1.0) < 1e-9
