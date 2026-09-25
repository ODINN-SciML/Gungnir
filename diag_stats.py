"""THROWAWAY diagnostic: dump gridded_data.nc per-variable stats at full precision.

Used to measure how much the two observed test_hash states actually differ, so we
can tell float/SIMD noise from a substantive difference. Not meant to be merged.
"""

import hashlib
import os

import numpy as np
import xarray as xr

NC = (
    "/tmp/Gungnir_tests/per_glacier/RGI60-11/RGI60-11.03/RGI60-11.03646/gridded_data.nc"
)


def main():
    if not os.path.exists(NC):
        print("MISSING:", NC)
        return

    with open(NC, "rb") as f:
        print("file_md5", hashlib.md5(f.read()).hexdigest())

    ds = xr.open_dataset(NC)
    print("dims", dict(ds.sizes))
    for name in sorted(ds.variables):
        arr = np.asarray(ds[name].values)
        if arr.dtype.kind in "fiub":
            a = arr.astype("float64")
            # %.17g round-trips float64 exactly, so last-bit differences show up
            print(
                f"{name:28s} shape={str(arr.shape):14s} dtype={str(arr.dtype):8s} "
                f"sum={np.nansum(a):.17g} mean={np.nanmean(a):.17g} "
                f"min={np.nanmin(a):.17g} max={np.nanmax(a):.17g} "
                f"nan={int(np.isnan(a).sum())}"
            )
        else:
            print(f"{name:28s} dtype={arr.dtype} (non-numeric, skipped)")


if __name__ == "__main__":
    main()
