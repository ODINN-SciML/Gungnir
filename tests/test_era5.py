"""
ERA5 integration tests.

These tests rely on the committed netCDF files in .cache_sample/.
"""

import os
import tempfile
import pytest
import glob
import xarray as xr

from gungnir import preprocessing_glaciers, emptyDir


def test_era5_file_generated():
    """ERA5 preprocessing produces a monthly ERA5 file for a lightweight one-year request."""

    folderName = "Gungnir_era5_tests"
    working_dir = os.path.join(tempfile.gettempdir(), folderName)
    emptyDir(working_dir)

    # Keep integration test lightweight while still exercising the full CDS path.
    rgi_ids = ["RGI60-11.03646"]
    years = [2020, 2020]
    preprocessing_glaciers(rgi_ids, working_dir=working_dir, include_era5=True, years=years, test=True)

    era5_files = glob.glob(working_dir + "/**/climate_historical_monthly_ERA5.nc", recursive=True)
    assert len(era5_files) == len(rgi_ids), (
        f"Expected one ERA5 file per glacier, found: {era5_files}"
    )

    with xr.open_dataset(era5_files[0]) as ds:
        assert int(ds.attrs["hydro_yr_0"]) == 2020
        assert int(ds.attrs["hydro_yr_1"]) == 2020


if __name__ == "__main__":
    test_era5_file_generated()
