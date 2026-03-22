# Gungnir
Preprocessing of topographical and climate data for ODINN.jl using OGGM

## Climate Sources

Gungnir prepares climate files compatible with Sleipnir for both sources:

- W5E5: default OGGM daily climate file.
- ERA5: fully independent ERA5 CDS daily climate file.

The script is intentionally argument-free and always builds both W5E5 and ERA5.

## ERA5 Download Modes

Gungnir provides two ERA5 download strategies:

### Monthly (Default - Lightweight)
By default, Gungnir downloads **monthly averaged** ERA5-Land data from CDS, then processes it to daily resolution for Sleipnir. This is significantly more lightweight in terms of data download volume and processing requirements.

Monthly data is:
- **Downloaded**: From `reanalysis-era5-land-monthly-means` product
- **Processed**: Forward-filled to daily (each month's values span all days in that month)
- **Fluxes**: Divided by days in month to compute daily averages (e.g., 1000 J/m²/month → ~33 J/m²/day for a 30-day month)

### Daily (Optional - High Resolution)
For future high-temporal-resolution work, you can enable **hourly → daily aggregation** by setting `use_daily=True` in `ensure_era5_file_for_gdir()`.

Hourly data is:
- **Downloaded**: From `reanalysis-era5-land` product (24 timesteps/day)
- **Processed**: Aggregated to daily via resampling (mean for temps/albedo, sum for fluxes)
- **Heavier**: ~7–8x more data than monthly mode

## ERA5 Output

Gungnir creates: `climate_historical_daily_ERA5.nc`

This file contains **daily** time series for:
- `temp` — 2m temperature (°C)
- `prcp` — total precipitation (m)
- `gradient` — temperature lapse rate (K/m, constant -0.0065)
- `fal` — forecast albedo (0–1)
- `slhf` — surface latent heat flux (J/m²)
- `sshf` — surface sensible heat flux (J/m²)
- `ssrd` — surface solar radiation downwards (J/m²)
- `str` — surface net thermal radiation (J/m²)

All ERA5 fields are downloaded from CDS and processed independently. ERA5 files are pure ERA5 and do not mix with W5E5 fields.

Metadata includes:
- `ref_hgt`: Reference altitude derived from geopotential
- `ref_pix_lat`, `ref_pix_lon`: Glacier center coordinates
- `hydro_yr_0`, `hydro_yr_1`: Year range of available data (1950–present)
- `climate_source`: "ERA5 CDS"

## Code Structure

The refactored `era5_climate.py` follows MassBalanceMachine's patterns:

### Download Functions
- `_download_era5_land_monthly_year()` — CDS monthly reanalysis download
- `_download_era5_land_hourly_year()` — CDS hourly reanalysis download (optional)
- `_download_era5_land_geopotential()` — Geopotential for altitude reference

### Processing Functions
- `_monthly_to_daily_point()` — Forward-fill monthly → daily
- `_hourly_to_daily_point()` — Aggregate hourly → daily
- `_compute_ref_hgt_from_geopotential()` — Convert geopotential to altitude

### Main Orchestrator
- `ensure_era5_file_for_gdir(gdir, use_daily=False, overwrite=False)` — Unified entry point selecting monthly or daily mode

## Usage

Run preprocessing:

```bash
python glacier_prepro.py
```

To explicitly request daily (hourly) resolution, edit `glacier_prepro.py` and change:
```python
ensure_era5_file_for_gdir(gdir, use_daily=False)  # Monthly (default)
```
to
```python
ensure_era5_file_for_gdir(gdir, use_daily=True)   # Hourly→daily (heavier)
```

## CDS API Setup

ERA5 download requires a CDS API key configured in `~/.cdsapirc`. See:

https://cds.climate.copernicus.eu/how-to-api
