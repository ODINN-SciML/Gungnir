import pytest

from gungnir.outline import rgi_date_to_decimal_year


def test_known_date_without_day():
    # July 2003, the day is unknown (99): the 15th
    assert rgi_date_to_decimal_year(20030799) == pytest.approx(2003.5356, abs=0.001)


def test_unknown_month_and_day():
    # Only the year is known: the middle of July
    assert rgi_date_to_decimal_year(20039999) == pytest.approx(2003.5356, abs=0.001)


def test_complete_date():
    assert rgi_date_to_decimal_year(20000216) == pytest.approx(2000.126, abs=0.01)


@pytest.mark.parametrize("value", [-9999999, 99999999, 0])
def test_unknown_year(value):
    assert rgi_date_to_decimal_year(value) is None
