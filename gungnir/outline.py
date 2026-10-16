"""Date of the glacier outline, to check that it matches the date of the DEM of the glacier."""

import logging
from datetime import datetime

from oggm import utils

from gungnir.dems import decimal_year

log = logging.getLogger(__name__)


def rgi_date_to_decimal_year(value):
    """RGI date `YYYYMMDD` as a decimal year, or `None` if the year is unknown.

    The RGI writes 99 for an unknown day or month (`20030799` is July 2003, with no day),
    and a negative number or 9999 for an unknown year. An unknown day is the 15th and an
    unknown month is July, so that the date is in the middle of the period it can be in.
    """
    value = int(value)
    if value < 0:
        return None
    year, month, day = value // 10000, (value // 100) % 100, value % 100
    if year < 1800 or year > 2100:
        return None
    month = 7 if month < 1 or month > 12 else month
    # The 28th at most, so that every month has the day (the error is at most 3 days)
    day = 15 if day < 1 or day > 31 else min(day, 28)
    return decimal_year(datetime(year, month, day, 12))


@utils.entity_task(log, writes=["gridded_data"])
def outline_date_to_gdir(gdir):
    """Write the date of the RGI outline as the attribute `date` of `glacier_mask`.

    The date is the mean of the begin and the end dates of the RGI entry (`BgnDate` and
    `EndDate`), or the begin date if the end date is unknown. `date_begin` and `date_end` are
    also written (NaN if unknown). Without a known date, no attribute is written.
    """
    entity = gdir.read_shapefile("outlines")
    begin = rgi_date_to_decimal_year(entity["BgnDate"].iloc[0])
    end = rgi_date_to_decimal_year(entity["EndDate"].iloc[0])
    known = [d for d in (begin, end) if d is not None]
    with utils.ncDataset(gdir.get_filepath("gridded_data"), "a") as nc:
        v = nc.variables["glacier_mask"]
        if not known:
            log.warning("The date of the outline of %s is unknown", gdir.rgi_id)
            # No stale date from an earlier run
            for name in ("date", "date_begin", "date_end", "date_source"):
                if name in v.ncattrs():
                    v.delncattr(name)
            return
        v.date = sum(known) / len(known)
        v.date_begin = float("nan") if begin is None else begin
        v.date_end = float("nan") if end is None else end
        v.date_source = "RGI BgnDate and EndDate"
