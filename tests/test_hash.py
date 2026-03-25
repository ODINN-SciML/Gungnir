import os
import tempfile
import hashlib
import glob
import json
from gungnir import preprocessing_glaciers, emptyDir


# Files that are ERA5-specific and excluded from the stable hash.
# ERA5 is tested separately in test_era5.py.
_ERA5_SKIP_SUFFIXES = (
    "climate_historical_daily_ERA5.nc",
    "climate_historical_monthly_ERA5.nc",
)
_ERA5_SKIP_DIRS = ("era5_cds_cache",)


def _is_era5_file(path: str) -> bool:
    if any(path.endswith(s) for s in _ERA5_SKIP_SUFFIXES):
        return True
    if any(seg in path.split(os.sep) for seg in _ERA5_SKIP_DIRS):
        return True
    return False


def test_hash():
    """
    This test checks the results of the preprocessing by comparing the content of
    the generated files to a checksum of reference.

    ERA5 files are intentionally excluded from the checksum since they require CDS
    API credentials that are not available in CI. ERA5 is tested separately in
    test_era5.py.
    """

    def hashFile(path:str):
        return hashlib.md5(open(path,'rb').read()).hexdigest()

    folderName = 'Gungnir_tests'
    working_dir = os.path.join(tempfile.gettempdir(), folderName)
    emptyDir(working_dir)

    rgi_ids = ["RGI60-11.03646"]
    # ERA5 is disabled so the hash is stable in CI without CDS credentials.
    preprocessing_glaciers(rgi_ids, working_dir=working_dir, include_era5=False)

    files = glob.glob(working_dir+"/**/*", recursive=True)
    checksums = {}
    for f in files:
        if f.endswith('.pkl'): print(f)
        if f.endswith('log.txt') or f.endswith('geometries.pkl') or f.endswith('inversion_flowlines.pkl') or f.endswith('centerlines.pkl'):
            # Log output is run dependent
            # Geometry file is pickled and depends on the Python session
            continue
        if _is_era5_file(f):
            continue
        if not os.path.isdir(f):
            chk = hashFile(f)
            checksums[f.split(folderName)[1]] = chk
    globalChecksum = hashlib.md5(json.dumps(checksums, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

    refChecksum = "91c9365a885a26792a5497d4d6d788f7"
    if globalChecksum!=refChecksum:
        raise Exception(f"Computed checksum is '{globalChecksum}' but reference is '{refChecksum}'. This likely means that the result of the preprocessing has changed. Update of the reference should be carefully tracked and the root cause of that change must be understood since this might impact the subsequent processing steps. In order to ease debugging, you can compare the checksums per file for different executions or for different heads of the repository. \n\n{checksums=}")


if __name__=="__main__":
    test_hash()
