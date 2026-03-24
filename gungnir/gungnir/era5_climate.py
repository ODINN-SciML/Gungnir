"""
ERA5-Land climate data download and processing for Sleipnir.

This module provides functions to download ERA5-Land climate data from the CDS API
in two modes:
  1. Monthly (default, lightweight): Downloads monthly averaged reanalysis data
  2. Daily (optional): Downloads hourly data and aggregates to daily

Both modes produce daily NetCDF files compatible with Sleipnir's Climate2D schema.

Inspired by MassBalanceMachine's climate data processing architecture.
"""

import os
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr


# ERA5-Land variables required by Sleipnir.
ERA5_MONTHLY_REQUEST_VARS = [
    "2m_temperature",
    "total_precipitation",
    "forecast_albedo",
    "surface_latent_heat_flux",
    "surface_sensible_heat_flux",
    "surface_solar_radiation_downwards",
    "surface_net_thermal_radiation",
]

ERA5_HOURLY_REQUEST_VARS = [
    "2m_temperature",
    "total_precipitation",
    "forecast_albedo",
    "surface_latent_heat_flux",
    "surface_sensible_heat_flux",
    "surface_solar_radiation_downwards",
    "surface_net_thermal_radiation",
]


def _get_cdsapi_client() -> Any:
    """Create a CDS API client with a clear setup error if credentials are missing."""
    try:
        import cdsapi

        return cdsapi.Client()
    except Exception as exc:
        message = str(exc)
        if "Missing/incomplete configuration file" in message:
            raise RuntimeError(
                "ERA5 download requires a valid CDS API configuration. "
                "Create ~/.cdsapirc with your CDS credentials, for example:\n\n"
                "url: https://cds.climate.copernicus.eu/api\n"
                "key: <your-uid>:<your-api-key>\n\n"
                "Then rerun preprocessing. Existing W5E5 files will be reused, so only ERA5 will be attempted."
            ) from exc
        if exc.__class__.__name__ == "ModuleNotFoundError":
            raise RuntimeError(
                "ERA5 download requires the optional 'cdsapi' dependency. "
                "Install it in the active environment before enabling ERA5 preprocessing."
            ) from exc
        raise


def _normalize_era5_coords(ds: xr.Dataset) -> xr.Dataset:
    """Normalize coordinate and dimension names for ERA5 datasets.
    
    ERA5 CDS API sometimes returns 'valid_time' instead of 'time', and may
    use 'lat'/'lon' instead of 'latitude'/'longitude'. This function standardizes
    to the latter for consistent downstream processing.
    """
    renamed = ds
    if "valid_time" in renamed.dims:
        renamed = renamed.rename({"valid_time": "time"})
    if "valid_time" in renamed.coords:
        renamed = renamed.rename({"valid_time": "time"})
    if "latitude" not in renamed.coords and "lat" in renamed.coords:
        renamed = renamed.rename({"lat": "latitude"})
    if "longitude" not in renamed.coords and "lon" in renamed.coords:
        renamed = renamed.rename({"lon": "longitude"})
    return renamed


def _extract_first_netcdf(zip_path: Path, out_dir: Path) -> Path:
    """Extract the first NetCDF file from a CDS-downloaded zip archive.
    
    CDS downloads are zipped; this helper unzips and returns the first .nc file found.
    """
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(out_dir)

    nc_files = sorted(out_dir.glob("*.nc"))
    if not nc_files:
        raise FileNotFoundError(f"No NetCDF file found in {zip_path}.")
    return nc_files[0]


def _cds_area_for_point(lat: float, lon: float, half_span_deg: float = 0.25) -> list:
    """Convert a point (lat, lon) to a CDS area bounding box.
    
    CDS expects [north, west, south, east]. We buffer the point by half_span_deg.
    """
    north = float(np.clip(lat + half_span_deg, -90.0, 90.0))
    south = float(np.clip(lat - half_span_deg, -90.0, 90.0))
    west = float(np.clip(lon - half_span_deg, -180.0, 180.0))
    east = float(np.clip(lon + half_span_deg, -180.0, 180.0))
    return [north, west, south, east]


def _download_era5_land_monthly_year(lat: float, lon: float, year: int, output_nc: Path):
    """Download monthly averaged ERA5-Land data for a given year.
    
    Downloads from reanalysis-era5-land-monthly-means product. This is the lightweight
    default, as monthly data is much smaller than hourly.
    
    Args:
        lat, lon: Glacier center coordinates
        year: Year to download (1950–present)
        output_nc: Path to save extracted NetCDF
    """
    area = _cds_area_for_point(lat, lon)
    request = {
        "product_type": ["monthly_averaged_reanalysis"],
        "variable": ERA5_MONTHLY_REQUEST_VARS,
        "year": [str(year)],
        "month": [f"{m:02d}" for m in range(1, 13)],
        "time": ["00:00"],
        "data_format": "netcdf",
        "download_format": "zip",
        "area": area,
    }

    with tempfile.TemporaryDirectory(prefix=f"era5_monthly_{year}_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        zip_path = tmp_dir / f"era5_land_monthly_{year}.zip"
        client = _get_cdsapi_client()
        client.retrieve("reanalysis-era5-land-monthly-means", request, str(zip_path))
        extracted_nc = _extract_first_netcdf(zip_path, tmp_dir)
        ds = xr.open_dataset(extracted_nc)
        ds = _normalize_era5_coords(ds)
        ds.to_netcdf(output_nc)


def _download_era5_land_hourly_year(lat: float, lon: float, year: int, output_nc: Path):
    """Download hourly ERA5-Land data for a given year.
    
    Downloads from reanalysis-era5-land product (hourly). More data-intensive than
    monthly but useful for high temporal resolution analysis. Used only when
    use_daily=True is specified.
    
    Args:
        lat, lon: Glacier center coordinates
        year: Year to download (1950–present)
        output_nc: Path to save extracted NetCDF
    """
    area = _cds_area_for_point(lat, lon)
    request = {
        "product_type": ["reanalysis"],
        "variable": ERA5_HOURLY_REQUEST_VARS,
        "year": [str(year)],
        "month": [f"{m:02d}" for m in range(1, 13)],
        "day": [f"{d:02d}" for d in range(1, 32)],
        "time": [f"{h:02d}:00" for h in range(24)],
        "data_format": "netcdf",
        "download_format": "zip",
        "area": area,
    }

    with tempfile.TemporaryDirectory(prefix=f"era5_hourly_{year}_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        zip_path = tmp_dir / f"era5_land_hourly_{year}.zip"
        client = _get_cdsapi_client()
        client.retrieve("reanalysis-era5-land", request, str(zip_path))
        extracted_nc = _extract_first_netcdf(zip_path, tmp_dir)
        ds = xr.open_dataset(extracted_nc)
        ds = _normalize_era5_coords(ds)
        ds.to_netcdf(output_nc)


def _download_era5_land_geopotential(lat: float, lon: float, output_nc: Path):
    """Download ERA5-Land geopotential for altitude computation.
    
    Retrieves monthly averaged geopotential from reanalysis-era5-land-monthly-means.
    This is used to compute the reference height (ref_hgt) for the glacier location.
    Only one year needed as it varies minimally; uses 2000 as reference.
    """
    area = _cds_area_for_point(lat, lon)
    request = {
        "product_type": ["monthly_averaged_reanalysis"],
        "variable": ["geopotential"],
        "year": ["2000"],
        "month": ["01"],
        "time": ["00:00"],
        "data_format": "netcdf",
        "download_format": "zip",
        "area": area,
    }

    with tempfile.TemporaryDirectory(prefix="era5_geopotential_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        zip_path = tmp_dir / "era5_land_geopotential.zip"
        client = _get_cdsapi_client()
        client.retrieve("reanalysis-era5-land-monthly-means", request, str(zip_path))
        extracted_nc = _extract_first_netcdf(zip_path, tmp_dir)
        ds = xr.open_dataset(extracted_nc)
        ds = _normalize_era5_coords(ds)
        ds.to_netcdf(output_nc)


def _monthly_to_monthly_point(monthly_ds: xr.Dataset, lat: float, lon: float) -> xr.Dataset:
    """Process monthly ERA5-Land data into a Sleipnir-compatible monthly climate dataset.

    Selects nearest grid point and applies unit conversions while keeping the monthly
    time axis intact. The resulting dataset can be written directly as
    ``climate_historical_monthly_ERA5.nc`` which Sleipnir reads via its ERA5 monthly path.

    Unit conversions applied:
      - ``temp``:  t2m [K] → [°C]  (monthly mean)
      - ``prcp``:  tp  [m]  kept as monthly total
      - ``fal``:   forecast albedo [0-1]  kept as monthly mean
      - ``slhf/sshf/ssrd/str``: energy fluxes [J/m²] kept as monthly totals
      - ``gradient``: constant lapse rate -0.0065 K/m

    Args:
        monthly_ds: Monthly ERA5 dataset with dims=(time,) where time is monthly.
        lat, lon: Glacier centre coordinates.

    Returns:
        xr.Dataset with monthly time dimension and Sleipnir-compatible variables.
    """
    ds = _normalize_era5_coords(monthly_ds)

    # Handle ensemble dimensions if present
    if "expver" in ds.dims:
        ds = ds.reduce(np.nansum, dim="expver")
    if "number" in ds.dims:
        ds = ds.reduce(np.nanmean, dim="number")

    # Select nearest grid point
    point = ds.sel(latitude=lat, longitude=lon, method="nearest")

    # Unit conversions
    temp = (point["t2m"] - 273.15).astype(np.float32)  # K → °C
    prcp = point["tp"].astype(np.float32)               # m, monthly total
    fal  = point["fal"].astype(np.float32)              # 0-1, monthly mean
    slhf = point["slhf"].astype(np.float32)             # J/m², monthly total
    sshf = point["sshf"].astype(np.float32)             # J/m², monthly total
    ssrd = point["ssrd"].astype(np.float32)             # J/m², monthly total
    str_ = point["str"].astype(np.float32)              # J/m², monthly total

    # Constant lapse rate (same value used in the daily path)
    gradient = xr.full_like(temp, fill_value=np.float32(-0.0065), dtype=np.float32)

    out = xr.Dataset(
        {
            "temp":     (["time"], temp.values,     {"units": "°C"}),
            "prcp":     (["time"], prcp.values,     {"units": "m"}),
            "gradient": (["time"], gradient.values, {"units": "K/m"}),
            "fal":      (["time"], fal.values,      {"units": "0-1"}),
            "slhf":     (["time"], slhf.values,     {"units": "J/m²"}),
            "sshf":     (["time"], sshf.values,     {"units": "J/m²"}),
            "ssrd":     (["time"], ssrd.values,     {"units": "J/m²"}),
            "str":      (["time"], str_.values,     {"units": "J/m²"}),
        },
        coords={"time": point.time.values},
    )
    return out


def _hourly_to_daily_point(hourly_ds: xr.Dataset, lat: float, lon: float) -> xr.Dataset:
    """Convert hourly ERA5-Land data to daily forcing for Sleipnir.
    
    Selects nearest grid point and aggregates hourly to daily using appropriate
    summary statistics (mean for temps/albedo, sum for precip/fluxes).
    
    Args:
        hourly_ds: Hourly ERA5 dataset with dims=(time,) where time is hourly
        lat, lon: Glacier center coordinates
        
    Returns:
        xr.Dataset with daily time dimension and Sleipnir-compatible variables
    """
    ds = _normalize_era5_coords(hourly_ds)
    
    # Handle ensemble dimensions if present
    if "expver" in ds.dims:
        ds = ds.reduce(np.nansum, dim="expver")
    if "number" in ds.dims:
        ds = ds.reduce(np.nanmean, dim="number")

    # Select nearest grid point
    point = ds.sel(latitude=lat, longitude=lon, method="nearest")

    # Aggregate hourly to daily with appropriate statistics
    temp = point["t2m"].resample(time="1D").mean() - 273.15  # K → °C, mean
    prcp = point["tp"].resample(time="1D").sum()  # Accumulated
    fal = point["fal"].resample(time="1D").mean()  # Mean
    slhf = point["slhf"].resample(time="1D").sum()  # Accumulated
    sshf = point["sshf"].resample(time="1D").sum()  # Accumulated
    ssrd = point["ssrd"].resample(time="1D").sum()  # Accumulated
    str_ = point["str"].resample(time="1D").sum()  # Accumulated

    # Create constant gradient
    gradient = xr.full_like(temp, fill_value=np.float32(-0.0065), dtype=np.float32)

    # Build output dataset
    out = xr.Dataset(
        {
            "temp": (["time"], temp.astype(np.float32), {"units": "°C"}),
            "prcp": (["time"], prcp.astype(np.float32), {"units": "m"}),
            "gradient": (["time"], gradient, {"units": "K/m"}),
            "fal": (["time"], fal.astype(np.float32), {"units": "0-1"}),
            "slhf": (["time"], slhf.astype(np.float32), {"units": "J/m²"}),
            "sshf": (["time"], sshf.astype(np.float32), {"units": "J/m²"}),
            "ssrd": (["time"], ssrd.astype(np.float32), {"units": "J/m²"}),
            "str": (["time"], str_.astype(np.float32), {"units": "J/m²"}),
        }
    )
    return out


def _compute_ref_hgt_from_geopotential(geopotential_ds: xr.Dataset, lat: float, lon: float) -> float:
    """Compute reference height from ERA5 geopotential field.
    
    Converts ERA5 geopotential (m²/s²) to altitude (m) using the formula:
        altitude = r_earth * (z/g) / (r_earth - z/g)
    
    This matches MassBalanceMachine's implementation for consistency.
    
    Args:
        geopotential_ds: Dataset containing geopotential variable 'z'
        lat, lon: Glacier center coordinates
        
    Returns:
        Altitude in meters
    """
    ds = _normalize_era5_coords(geopotential_ds)
    
    # Handle ensemble dimension
    if "expver" in ds.dims:
        ds = ds.reduce(np.nansum, dim="expver")

    # Select nearest grid point and average over time
    point = ds.sel(latitude=lat, longitude=lon, method="nearest")
    z = float(point["z"].mean().values)

    # Standard conversion from geopotential to altitude (same as MBM)
    r_earth = 6367.47e3  # meters
    g = 9.80665  # m/s²
    altitude = r_earth * (z / g) / (r_earth - (z / g))
    return float(altitude)


def _get_era5_year_range():
    """Get the range of available ERA5 years.
    
    ERA5-Land data is available from 1950 to the current year.
    Returns the start and end year for data download.
    """
    start_year = 1950
    end_year = datetime.utcnow().year
    return start_year, end_year


def ensure_era5_file_for_gdir(gdir, use_daily: bool = False, overwrite: bool = False) -> str:
    """Generate an ERA5 climate NetCDF file compatible with Sleipnir.

    Downloads ERA5-Land data and writes a file that Sleipnir's ``get_raw_climate_data``
    can ingest directly.

    * ``use_daily=False`` (default): downloads monthly averaged reanalysis data and
      writes ``climate_historical_monthly_ERA5.nc``.  Monthly values are kept as-is;
      no interpolation to daily is performed.  Sleipnir checks for this file first
      and processes it natively at monthly resolution.

    * ``use_daily=True``: downloads hourly reanalysis data, aggregates to daily, and
      writes ``climate_historical_daily_ERA5.nc``.  Use this when daily resolution is
      required (e.g. for W5E5-compatible workflows).

    Both output files include:
      - Variables: temp (°C), prcp (m), gradient (K/m), fal, slhf, sshf, ssrd, str
      - Metadata: ref_hgt, ref_pix_lat, ref_pix_lon, climate_source, hydro_yr_0/1

    Args:
        gdir: GlacierDirectory from OGGM with .dir, .cenlat, .cenlon attributes.
        use_daily: If False (default), produce a lightweight monthly file.
                   If True, produce a daily file from hourly downloads.
        overwrite: If False (default), skip if output file already exists.

    Returns:
        Path to the generated climate NetCDF file (as string).
    """
    if use_daily:
        out_era5 = Path(gdir.dir) / "climate_historical_daily_ERA5.nc"
    else:
        out_era5 = Path(gdir.dir) / "climate_historical_monthly_ERA5.nc"

    if out_era5.exists() and not overwrite:
        return str(out_era5)

    # Create cache directory for intermediate downloads
    cache_dir = Path(gdir.dir) / "era5_cds_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    lat = float(gdir.cenlat)
    lon = float(gdir.cenlon)
    start_year, end_year = _get_era5_year_range()

    yearly_datasets = []

    if use_daily:
        # High-resolution hourly → daily mode
        for year in range(start_year, end_year + 1):
            yearly_nc = cache_dir / f"era5_land_hourly_{year}.nc"
            if overwrite or not yearly_nc.exists():
                _download_era5_land_hourly_year(lat, lon, year, yearly_nc)
            hourly_ds = xr.open_dataset(yearly_nc)
            yearly_datasets.append(_hourly_to_daily_point(hourly_ds, lat, lon))
    else:
        # Default lightweight monthly mode — no daily expansion
        for year in range(start_year, end_year + 1):
            yearly_nc = cache_dir / f"era5_land_monthly_{year}.nc"
            if overwrite or not yearly_nc.exists():
                _download_era5_land_monthly_year(lat, lon, year, yearly_nc)
            monthly_ds = xr.open_dataset(yearly_nc)
            yearly_datasets.append(_monthly_to_monthly_point(monthly_ds, lat, lon))

    # Concatenate yearly datasets into single timeseries
    era5_daily = xr.concat(yearly_datasets, dim="time").sortby("time")

    # Compute reference height from geopotential
    geopotential_nc = cache_dir / "era5_land_geopotential.nc"
    if overwrite or not geopotential_nc.exists():
        _download_era5_land_geopotential(lat, lon, geopotential_nc)
    
    geopotential_ds = xr.open_dataset(geopotential_nc)
    ref_hgt = _compute_ref_hgt_from_geopotential(geopotential_ds, lat, lon)

    # Extract year range from data
    hydro_yr_0 = int(pd.to_datetime(era5_daily.time.values).year.min())
    hydro_yr_1 = int(pd.to_datetime(era5_daily.time.values).year.max())
    
    # Set metadata attributes (Sleipnir expects these)
    era5_daily.attrs = {
        "author": "Gungnir",
        "author_info": "ODINN-SciML",
        "climate_source": "ERA5 CDS",
        "hydro_yr_0": hydro_yr_0,
        "hydro_yr_1": hydro_yr_1,
        "ref_hgt": np.float32(ref_hgt),
        "ref_pix_lat": np.float32(lat),
        "ref_pix_lon": np.float32(lon),
        "ref_pix_dis": np.float32(0.0),
    }

    # Write to NetCDF with compression
    encoding = {name: {"zlib": True, "complevel": 4} for name in era5_daily.data_vars}
    era5_daily.to_netcdf(out_era5, encoding=encoding)

    return str(out_era5)
