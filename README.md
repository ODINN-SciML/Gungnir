[![Build Status](https://github.com/ODINN-SciML/Gungnir/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/ODINN-SciML/Gungnir/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/ODINN-SciML/Gungnir/branch/main/graph/badge.svg)](https://app.codecov.io/gh/ODINN-SciML/Gungnir)

<img src="https://github.com/ODINN-SciML/Gungnir/blob/main/data/gungnir_logo.png" width="250">

Preprocessing of topographical and climate data for [ODINN.jl](https://github.com/ODINN-SciML/ODINN.jl) using [OGGM](https://github.com/OGGM/oggm).

Gungnir uses OGGM to generate all necessary files for the initial state and climate forcings to run simulations with ODINN.jl. Before running any simulations for specific glaciers with ODINN.jl, Gungnir needs to initialize those glaciers. We will progressively initialize glacier regions and store them in a server so they are readily available to all users. If you find that some glaciers or a region is missing, please contact us!

## Installation

All the notebooks inside this notebook can be executed after properly setting the environment. The `environment.yml` file can be used to
install all the required dependencies. Beside some standard Python dependencies, the `environment.yml` file include the installation of the module `gungnir` (included in this repository). The package `gungnir` includes all the code required to download the glacier data using OGGM.

In order to install the environment, you can use conda or mamba (see [Managing Environments](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) for more information) with `conda env create -f environment.yml`. Once the environment is created, you can create the associated iPython kernel with
```
python -m ipykernel install --user --name oggm_env_gungnir --display-name "IPython - Gungnir"
```
This will allow you to execute this environment directly from Jupyter notebooks.

Alternatively, we included a `Makefile` that creates the conda environment and installs the associated iPython kernel so this environment can be accessible though Jupyter notebooks all at once. In order to use the Makefile, you need to open a terminal where the repository is located and enter
```
make env
```

Alternatively, if you just want to install the `gungnir` module, you can clone this repository and do
```
pip install gungnir
```
or
```
pip install -e gungnir
```
if you are working in developer mode.

## Usage

We included an example notebook of how to retrieve data using OGGM data in `notebooks/Example.ipynb`

You can also use Gungnir directly from the terminal. If you are using the remote OGGM cluster as working directory, in a new terminal after doing `conda activate oggm_env_gungnir`, proceed with
```bash
python gungnir/gungnir/preprocessing.py glaciers.txt
```
If no working directory is provided, data is written to `~/.ODINN/ODINN_prepro` (the default location expected by Sleipnir).

You can still provide any explicit local/custom output directory:
```bash
python gungnir/gungnir/preprocessing.py glaciers.txt --working_dir <working-dir>
```

Note: if `<working-dir>` is set to `~/.ODINN` or `~/.ODINN/per_glacier`, Gungnir automatically normalizes it to `~/.ODINN/ODINN_prepro` to avoid path mismatches with Sleipnir.

## Climate Sources

Gungnir prepares climate files compatible with Sleipnir for two independent sources:

- **W5E5**: default OGGM daily climate file.
- **ERA5**: fully independent ERA5-Land climate file downloaded from the [CDS API](https://cds.climate.copernicus.eu/).

Both sources are always generated for each glacier, allowing selection via Sleipnir's `climate_data_source` parameter.

## ERA5 Download Modes

### Monthly (Default — Lightweight)
By default, Gungnir downloads **monthly averaged** ERA5-Land data from CDS and keeps it at monthly resolution. This is significantly more lightweight in terms of data volume.

Monthly data is:
- **Downloaded**: From `reanalysis-era5-land-monthly-means`
- **Stored**: As `climate_historical_monthly_ERA5.nc` for downstream monthly workflows (Sleipnir/MassBalanceMachine)

### Daily (Optional — High Resolution)
To enable hourly→daily aggregation, use the `use_daily` option:

```python
python gungnir/gungnir/preprocessing.py glaciers.txt --working_dir <working-dir> --use_daily
```

Hourly data is downloaded from `reanalysis-era5-land` (24 timesteps/day) and resampled to daily.

## ERA5 Output

Gungnir writes one of the following, depending on mode:
- `climate_historical_monthly_ERA5.nc` (default monthly mode)
- `climate_historical_daily_ERA5.nc` (`use_daily` option)

Both files contain:
- `temp` — 2m temperature (°C)
- `prcp` — total precipitation (m)
- `gradient` — temperature lapse rate (K/m, constant −0.0065)
- `fal` — forecast albedo (0–1)
- `slhf` — surface latent heat flux (J/m²)
- `sshf` — surface sensible heat flux (J/m²)
- `ssrd` — surface solar radiation downwards (J/m²)
- `str` — surface net thermal radiation (J/m²)

Metadata includes `ref_hgt` (derived from CDS geopotential), `ref_pix_lat/lon`, and `hydro_yr_0/1`.

## CDS API Setup

ERA5 download requires a CDS API key configured in `~/.cdsapirc`. See:
https://cds.climate.copernicus.eu/how-to-api

## Code formatting

We use `black` to format the code of Gungnir.
Please refer to the [Mass Balance Machine documentation](https://massbalancemachine.readthedocs.io/en/latest/contributing.html#formatting-the-code) for instructions on how to install the code formatter locally.
