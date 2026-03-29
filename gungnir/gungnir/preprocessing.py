import sys
from oggm import cfg, utils, workflow, tasks, global_tasks
from oggm.shop import bedtopo, millan22, glathida
from MBsandbox.mbmod_daily_oneflowline import process_w5e5_data
import json, os
import argparse
from pathlib import Path

import gungnir.utils
from gungnir.era5_climate import _default_years
import gungnir.era5_climate


def _cds_credentials_available() -> bool:
    """Return True if a CDS API key file or env-var credentials are present."""
    cdsapirc = os.path.expanduser("~/.cdsapirc")
    if os.path.isfile(cdsapirc):
        return True
    # The ecmwf/cdsapi library also accepts CDSAPI_URL + CDSAPI_KEY env vars
    if os.environ.get("CDSAPI_KEY"):
        return True
    return False


def _sleipnir_default_working_dir() -> str:
    """Canonical preprocessed data location expected by Sleipnir."""
    return os.path.join(os.path.expanduser("~"), ".ODINN", "ODINN_prepro")


_default_working_dir = _sleipnir_default_working_dir()


def _normalize_working_dir(working_dir: str) -> str:
    """Normalize output path to keep Gungnir/Sleipnir data layout consistent."""
    if not working_dir:
        return _default_working_dir

    wd = os.path.abspath(os.path.expanduser(working_dir))
    odinn_root = os.path.join(os.path.expanduser("~"), ".ODINN")

    # Common misconfiguration: users pass ~/.ODINN or ~/.ODINN/per_glacier.
    # Redirect to ~/.ODINN/ODINN_prepro, which is where Sleipnir looks.
    if wd == odinn_root or wd == os.path.join(odinn_root, "per_glacier"):
        target = _default_working_dir
        print(f"Normalizing working_dir from '{wd}' to '{target}' for Sleipnir compatibility.")
        return target

    return wd

def preprocessing_file(file, working_dir=_default_working_dir, include_era5=None, use_daily=False, years=_default_years):
    """
    Preprocess glaciers directly from file
    """

    rgi_ids = gungnir.utils.read_glacier_names(file)
    preprocessing_glaciers(rgi_ids, working_dir=working_dir, include_era5=include_era5, use_daily=use_daily, years=years)

def preprocessing_glaciers(rgi_ids, working_dir=_default_working_dir, include_era5=None, use_daily=False, years=_default_years, test=False):
    """
    Preprocessing of glaciers from a list of glaciers

    Arguments:
        - rgi_ids: List of glaciers and/or regions to process. E.g., rgi_ids = ['RGI60-11.00897', 'RGI60-11.01270']
    """

    # Check ERA5 capability if this is needed
    if include_era5 and not test:
        assert _cds_credentials_available(), "ERA5 download skipped: no CDS API credentials found (~/.cdsapirc or CDSAPI_KEY env var)."

    working_dir = _normalize_working_dir(working_dir)
    os.makedirs(working_dir, exist_ok=True)

    base_url = 'https://cluster.klima.uni-bremen.de/~oggm/gdirs/oggm_v1.6/L1-L2_files/elev_bands/'

    print("Working directory:", working_dir)

    cfg.initialize()

    # Settings
    cfg.PATHS['working_dir'] = working_dir
    cfg.PARAMS['use_multiprocessing'] = True # use all available CPUs
    cfg.PARAMS['border'] = 10
    cfg.PARAMS['hydro_month_nh'] = 1
    cfg.PARAMS['dl_verify'] = False
    cfg.PARAMS['continue_on_error'] = True

    # Now we initialize the glacier directories
    gdirs = workflow.init_glacier_directories(rgi_ids,
                                              prepro_base_url=base_url,
                                              from_prepro_level=2)

    # We execute the entity tasks
    list_tasks = [tasks.gridded_attributes,
                  tasks.glacier_masks,
                  tasks.compute_centerlines,
                  tasks.initialize_flowlines,
                  bedtopo.add_consensus_thickness,
                  millan22.thickness_to_gdir,
                  millan22.velocity_to_gdir,
                  glathida.glathida_to_gdir]

    for task in list_tasks:
        workflow.execute_entity_task(task, gdirs)

    if include_era5 and not use_daily:
        regions = []
        for gdir in gdirs:
            rgi_id = gdir.rgi_id
            rgi_version = rgi_id.split('-')[0].replace("RGI", "")
            assert rgi_version=="60", "RGI version must be RGI60"
            region_id = rgi_id.split('-')[1].split('.')[0]
            if region_id not in regions:
                regions.append(region_id)
        for region in regions:
            if not test:
                gungnir.era5_climate.ensure_era5_file_for_region(region, years_range=years)
            else:
                cache_path = os.path.join(gungnir.era5_climate.gungnir_path, ".cache_sample")
                monthly_nc = Path(cache_path) / "ERA5" / f"era5_land_monthly_region_{region}.nc"
                geopotential_nc = Path(cache_path) / "ERA5" / f"era5_land_geopotential_region_{region}.nc"
                assert monthly_nc.exists(), f"The monthly netcdf does not exist in {cache_path}"
                assert geopotential_nc.exists(), f"The geopotential netcdf does not exist in {cache_path}"

    ### Then we retrieve all the necessary climate data ###
    rgi_paths = {}
    rgi_names = {}
    for gdir in gdirs: # TODO: change to parallel processing by creating an entity task
        # We store all the paths for each RGI ID to be retrieved later on in ODINN
        rgi_paths[gdir.rgi_id] = os.path.relpath(gdir.dir, working_dir)
        rgi_names[gdir.rgi_id] = gungnir.utils.remove_id_from_string(gdir.name)

        process_w5e5_data(gdir, climate_type='W5E5', temporal_resol='daily')

        # Build an ERA5 climate file directly from CDS.
        # Default (use_daily=False): downloads monthly data and writes
        #   climate_historical_monthly_ERA5.nc  (lightweight, read natively by Sleipnir).
        # For daily resolution (e.g. W5E5-compatible workflows) use:
        #   era5_path = gungnir.era5_climate.ensure_era5_file_for_gdir(gdir, use_daily=True, overwrite=False)
        # which downloads hourly data, aggregates to daily and writes
        #   climate_historical_daily_ERA5.nc instead.
        if include_era5:
            print(f"Generating ERA5 climate file for {gdir.rgi_id}")
            kwargs = {"use_daily": use_daily, "years_range": years}
            if test:
                kwargs["cache_path"] = cache_path
            era5_path = gungnir.era5_climate.ensure_era5_file_for_gdir(gdir, **kwargs)
            print("ERA5 climate path:", era5_path)

        print("dem path: " , gdir.get_filepath("dem"))

    with open(working_dir + '/rgi_paths.json', 'w') as f:
        json.dump(rgi_paths, f)
    with open(working_dir + '/rgi_names.json', 'w') as f:
        json.dump(rgi_names, f)

    # Verify that glaciers have no missing data
    task_log = global_tasks.compile_task_log(gdirs,
                                            task_names=["gridded_attributes", "velocity_to_gdir", "thickness_to_gdir"])

    task_log.to_csv(os.path.join(working_dir, "task_log.csv"))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("glacier_file", type=str, help="File containing list of glaciers to process.")
    parser.add_argument(
        "--working_dir",
        type=str,
        default=_default_working_dir,
        help="Train on GPU. By default training runs on CPU.",
    )
    parser.add_argument(
        "--noera5",
        action="store_true",
        help="Do not include ERA5 data.",
    )
    parser.add_argument(
        "--use_daily",
        action="store_true",
        help="Generate daily ERA5 data.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=_default_years,
        nargs=2,
        help="Years for the climate data.",
    )
    args = parser.parse_args()

    glacier_file = args.glacier_file
    working_dir = args.working_dir
    include_era5 = not args.noera5
    use_daily = args.use_daily
    years = args.years

    preprocessing_file(glacier_file, working_dir=working_dir, include_era5=include_era5, use_daily=use_daily, years=years)
