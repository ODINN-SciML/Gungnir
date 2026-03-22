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
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import cdsapi
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
        client = cdsapi.Client()
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
        client = cdsapi.Client()
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
        client = cdsapi.Client()
        client.retrieve("reanalysis-era5-land-monthly-means", request, str(zip_path))
        extracted_nc = _extract_first_netcdf(zip_path, tmp_dir)
        ds = xr.open_dataset(extracted_nc)
        ds = _normalize_era5_coords(ds)
        ds.to_netcdf(output_nc)
        ds.to_netcdf(output_nc)


def _monthly_to_daily_point(monthly_ds: xr.Dataset, lat: float, lon: float) -> xr.Dataset:
    """Convert monthly ERA5-Land data to daily forcing for Sleipnir.
    
    Selects nearest grid point and forward-fills monthly values to all days in each month.
    Monthly fluxes (heat, radiation) are divided by the number of days in each month to
    produce daily averages, consistent with daily aggregation from hourly data.
    
    Args:
        monthly_ds: Monthly ERA5 dataset with dims=(time,) where time is monthly
        lat, lon: Glacier center coordinates
        
    Returns:
        xr.Dataset with daily time dimension and Sleipnir-compatible variables
    """
    ds = _normalize_era5_coords(monthly_ds)
    
    # Handle ensemble dimension if present
    if "expver" in ds.dims:
        ds = ds.reduce(np.nansum, dim="expver")
    if "number" in ds.dims:
        ds = ds.reduce(np.nanmean, dim="number")

    # Select nearest grid point
    point = ds.sel(latitude=lat, longitude=lon, method="nearest")
    
    # Extract variables with appropriate unit conversions
    temp = point["t2m"] - 273.15  # K → °C
    prcp = point["tp"]  # Already in m
    fal = point["fal"]  # 0-1 scale, no conversion
    slhf = point["slhf"]  # J/m²
    sshf = point["sshf"]  # J/m²
    ssrd = point["ssrd"]  # J/m²
    str_ = point["str"]  # J/m²
    
    # Create daily time index by forward-filling each month's value across all days
    time_index = pd.to_datetime(point.time.values)
    daily_index = pd.date_range(
        start=time_index[0].replace(day=1),
        end=(time_index[-1] + pd.DateOffset(months=1)).replace(day=1) - pd.Timedelta(days=1),
        freq="D"
    )
    
    # Function to forward-fill monthly values to daily
    def _monthly_to_daily_forward_fill(monthly_var):
        """Forward-fill monthly values to daily, accounting for days per month."""
        daily_values = []
        for month_idx, month_val in enumerate(monthly_var.values):
            # Get the month and year from time index
            year = time_index[month_idx].year
            month = time_index[month_idx].month
            days_in_month = monthrange(year, month)[1]
            
            # Find all daily indices that fall in this month
            month_start = pd.Timestamp(year=year, month=month, day=1)
            month_end = month_start + pd.DateOffset(months=1) - pd.Timedelta(days=1)
            mask = (daily_index >= month_start) & (daily_index <= month_end)
            daily_values.extend([month_val] * mask.sum())
        
        return np.array(daily_values[:len(daily_index)])
    
    # Convert temperature, albedo, and fluxes to daily
    temp_daily = _monthly_to_daily_forward_fill(temp)
    fal_daily = _monthly_to_daily_forward_fill(fal)
    
    # For precipitation, forward-fill (monthly total over all days)
    prcp_daily = _monthly_to_daily_forward_fill(prcp)
    
    # For fluxes, divide by days in month to get daily average
    slhf_daily = []
    sshf_daily = []
    ssrd_daily = []
    str_daily = []
    
    for month_idx, (slhf_val, sshf_val, ssrd_val, str_val) in enumerate(
        zip(slhf.values, sshf.values, ssrd.values, str_.values)
    ):
        year = time_index[month_idx].year
        month = time_index[month_idx].month
        days_in_month = monthrange(year, month)[1]
        
        # Average daily value = monthly total / days in month
        month_start = pd.Timestamp(year=year, month=month, day=1)
        month_end = month_start + pd.DateOffset(months=1) - pd.Timedelta(days=1)
        mask = (daily_index >= month_start) & (daily_index <= month_end)
        
        slhf_daily.extend([float(slhf_val) / days_in_month] * mask.sum())
        sshf_daily.extend([float(sshf_val) / days_in_month] * mask.sum())
        ssrd_daily.extend([float(ssrd_val) / days_in_month] * mask.sum())
        str_daily.extend([float(str_val) / days_in_month] * mask.sum())
    
    # Create constant gradient
    gradient = np.full(len(daily_index), -0.0065, dtype=np.float32)
    
    # Build output dataset
    out = xr.Dataset(
        {
            "temp": (["time"], temp_daily, {"units": "°C"}),
            "prcp": (["time"], prcp_daily, {"units": "m"}),
            "gradient": (["time"], gradient, {"units": "K/m"}),
            "fal": (["time"], fal_daily, {"units": "0-1"}),
            "slhf": (["time"], np.array(slhf_daily), {"units": "J/m²"}),
            "sshf": (["time"], np.array(sshf_daily), {"units": "J/m²"}),
            "ssrd": (["time"], np.array(ssrd_daily), {"units": "J/m²"}),
            "str": (["time"], np.array(str_daily), {"units": "J/m²"}),
        },
        coords={"time": daily_index},
    )
    
    return out.astype(np.float32)


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
    """Generate a daily climate NetCDF file compatible with Sleipnir.
    
    Downloads ERA5-Land data (monthly by default for lightweight downloads, or hourly
    if use_daily=True) and processes to daily forcing data that Sleipnir can ingest.
    
    The output file is named `climate_historical_daily_ERA5.nc` and includes:
    - Variables: temp (°C), prcp (m), gradient (K/m), fal, slhf, sshf, ssrd, str
    - Metadata: ref_hgt, ref_pix_lat, ref_pix_lon, climate_source, hydro_yr_0/1
    - Time: Daily from 1950 to current year
    
    Args:
        gdir: GlacierDirectory from OGGM with .dir, .cenlat, .cenlon attributes
        use_daily: If False (default), download monthly data (lightweight).
                   If True, download hourly data (more I/O).
        overwrite: If False (default), skip if file exists. If True, re-download.
        
    Returns:
        Path to the generated climate NetCDF file (as string)
    """
    out_era5 = Path(gdir.dir) / "climate_historical_daily_ERA5.nc"
    if out_era5.exists() and not overwrite:
        return str(out_era5)

    # Create cache directory for intermediate downloads
    cache_dir = Path(gdir.dir) / "era5_cds_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    lat = float(gdir.cenlat)
    lon = float(gdir.cenlon)
    start_year, end_year = _get_era5_year_range()

    # Download and process climate data for each year
    daily_yearly = []
    
    if use_daily:
        # High-resolution hourly → daily mode
        for year in range(start_year, end_year + 1):
            yearly_nc = cache_dir / f"era5_land_hourly_{year}.nc"
            if overwrite or not yearly_nc.exists():
                _download_era5_land_hourly_year(lat, lon, year, yearly_nc)
            
            hourly_ds = xr.open_dataset(yearly_nc)
            daily_yearly.append(_hourly_to_daily_point(hourly_ds, lat, lon))
    else:
        # Default lightweight monthly → daily mode
        for year in range(start_year, end_year + 1):
            yearly_nc = cache_dir / f"era5_land_monthly_{year}.nc"
            if overwrite or not yearly_nc.exists():
                _download_era5_land_monthly_year(lat, lon, year, yearly_nc)
            
            monthly_ds = xr.open_dataset(yearly_nc)
            daily_yearly.append(_monthly_to_daily_point(monthly_ds, lat, lon))

    # Concatenate yearly datasets into single timeseries
    era5_daily = xr.concat(daily_yearly, dim="time").sortby("time")

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
