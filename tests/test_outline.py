import math

import netCDF4
import numpy as np
import pandas as pd
import pytest

from gungnir.outline import outline_date_to_gdir, rgi_date_to_decimal_year


class FakeGdir:
    """Glacier directory with only what the task uses: the RGI entry and `gridded_data.nc`."""

    def __init__(self, path, begin, end):
        self.rgi_id = "RGI60-11.00000"
        self.path = str(path)
        self.entity = pd.DataFrame({"BgnDate": [begin], "EndDate": [end]})
        with netCDF4.Dataset(self.path, "w") as nc:
            nc.createDimension("x", 2)
            nc.createDimension("y", 2)
            nc.createVariable("glacier_mask", "i1", ("y", "x"))[:] = np.ones((2, 2))

    def read_shapefile(self, name):
        return self.entity

    def get_filepath(self, name):
        return self.path


def mask_attributes(path):
    with netCDF4.Dataset(path) as nc:
        v = nc.variables["glacier_mask"]
        return {name: v.getncattr(name) for name in v.ncattrs()}


# The body of the task, without the OGGM wrapper (logging, settings)
write_outline_date = outline_date_to_gdir.__wrapped__


def test_task_mean_of_begin_and_end(tmp_path):
    gdir = FakeGdir(tmp_path / "gridded_data.nc", "20030799", "20030999")
    write_outline_date(gdir)
    a = mask_attributes(gdir.path)
    assert a["date"] == pytest.approx((a["date_begin"] + a["date_end"]) / 2)
    assert a["date_begin"] == pytest.approx(2003.5356, abs=0.001)


def test_task_only_one_date_known(tmp_path):
    gdir = FakeGdir(tmp_path / "gridded_data.nc", "-9999999", "20030999")
    write_outline_date(gdir)
    a = mask_attributes(gdir.path)
    assert math.isnan(a["date_begin"])
    assert a["date"] == pytest.approx(a["date_end"])


def test_task_unknown_date_removes_old_attributes(tmp_path):
    gdir = FakeGdir(tmp_path / "gridded_data.nc", "20030799", "20030999")
    write_outline_date(gdir)
    gdir.entity = pd.DataFrame({"BgnDate": ["-9999999"], "EndDate": ["-9999999"]})
    write_outline_date(gdir)
    a = mask_attributes(gdir.path)
    assert not any(
        name in a for name in ("date", "date_begin", "date_end", "date_source")
    )


def test_known_date_without_day():
    # July 2003, the day is unknown (99): the 15th
    assert rgi_date_to_decimal_year(20030799) == pytest.approx(2003.5356, abs=0.001)


def test_unknown_month_and_day():
    # Only the year is known: the middle of July
    assert rgi_date_to_decimal_year(20039999) == pytest.approx(2003.5356, abs=0.001)


def test_complete_date():
    assert rgi_date_to_decimal_year(20000216) == pytest.approx(2000.126, abs=0.01)


def test_text_date():
    # The RGI 6 outlines store the date as text
    assert rgi_date_to_decimal_year("20030799") == pytest.approx(2003.5356, abs=0.001)


def test_last_day_of_the_month():
    # The real day is kept: February 29 of a leap year, and the 31st
    assert rgi_date_to_decimal_year(20040229) > rgi_date_to_decimal_year(20040228)
    assert rgi_date_to_decimal_year(20031231) == pytest.approx(2003.9986, abs=0.001)


@pytest.mark.parametrize("value", [-9999999, 99999999, 0, None, float("nan"), ""])
def test_unknown_year(value):
    assert rgi_date_to_decimal_year(value) is None
