"""
ERA5 integration tests.

These tests are skipped automatically when CDS API credentials are not available
(i.e. no ~/.cdsapirc and no CDSAPI_KEY environment variable). To run them locally:

    1. Create ~/.cdsapirc with your CDS credentials:
           url: https://cds.climate.copernicus.eu/api
           key: <your-api-key>
    2. Accept the ERA5-Land licence at:
           https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land-monthly-means?tab=download#manage-licences

In CI, store your CDS key as a repository secret (CDSAPI_KEY / CDSAPI_URL) and add a
workflow step to write ~/.cdsapirc before running pytest.
"""

import os
import tempfile
import pytest
import xarray as xr
from gungnir import preprocessing_glaciers, emptyDir
from gungnir.preprocessing import _cds_credentials_available


pytestmark = pytest.mark.skipif(
    not _cds_credentials_available(),
    reason="No CDS API credentials (~/.cdsapirc or CDSAPI_KEY env var) — skipping ERA5 tests",
)


def test_era5_file_generated(monkeypatch):
    """ERA5 preprocessing produces a monthly ERA5 file for a lightweight one-year request."""
    import glob

    # Keep integration test lightweight while still exercising the full CDS path.
    monkeypatch.setenv("GUNGNIR_ERA5_START_YEAR", "2020")
    monkeypatch.setenv("GUNGNIR_ERA5_END_YEAR", "2020")

    folderName = "Gungnir_era5_tests"
    working_dir = os.path.join(tempfile.gettempdir(), folderName)
    emptyDir(working_dir)

    rgi_ids = ["RGI60-11.03646"]
    preprocessing_glaciers(rgi_ids, working_dir=working_dir, include_era5=True)

    era5_files = glob.glob(working_dir + "/**/climate_historical_monthly_ERA5.nc", recursive=True)
    assert len(era5_files) == len(rgi_ids), (
        f"Expected one ERA5 file per glacier, found: {era5_files}"
    )

    with xr.open_dataset(era5_files[0]) as ds:
        assert int(ds.attrs["hydro_yr_0"]) == 2020
        assert int(ds.attrs["hydro_yr_1"]) == 2020


if __name__ == "__main__":
    test_era5_file_generated()
