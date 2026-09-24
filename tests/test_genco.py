from datetime import date, datetime
from decimal import Decimal

import pytest

from nesi.genco import ScrapeError, days_to_scrape, hour_label_time, parse_readings

DAY = date(2026, 9, 24)
HEADERS = ["#", "Genco", "01:00", "02:00", "03:00", "24:00", "TotalGeneration"]


def test_hour_label_time_handles_24_00_as_end_of_day():
    assert hour_label_time(DAY, "01:00") == datetime(2026, 9, 24, 1)
    assert hour_label_time(DAY, "24:00") == datetime(2026, 9, 25, 0)
    assert hour_label_time(DAY, "TotalGeneration") is None


def test_parse_readings_unpivots_and_cleans():
    rows = [
        ["1", "EGBIN (STEAM)", "415", "1,020.5", "", "400", "9999"],
        ["2", "zTOTAL", "1", "1", "1", "1", "4"],
        ["3", "", "1", "1", "1", "1", "4"],
        ["4", "KAINJI (HYDRO)", "354", "n/a", "300", "0", "654"],
    ]
    got = {(r.hour, r.genco): r.mwh for r in parse_readings(HEADERS, rows, DAY)}
    assert got == {
        ("01:00", "EGBIN (STEAM)"): Decimal("415"),
        ("02:00", "EGBIN (STEAM)"): Decimal("1020.5"),
        ("24:00", "EGBIN (STEAM)"): Decimal("400"),
        ("01:00", "KAINJI (HYDRO)"): Decimal("354"),
        ("03:00", "KAINJI (HYDRO)"): Decimal("300"),
        ("24:00", "KAINJI (HYDRO)"): Decimal("0"),
    }


def test_parse_readings_skips_future_hours():
    rows = [["1", "EGBIN (STEAM)", "1", "2", "3", "4", "10"]]
    got = parse_readings(HEADERS, rows, DAY, not_after=datetime(2026, 9, 24, 2))
    assert [r.hour for r in got] == ["01:00", "02:00"]


def test_parse_readings_skips_misaligned_rows():
    rows = [["only", "three", "cells"], ["1", "EGBIN (STEAM)", "1", "", "", "", "1"]]
    assert len(parse_readings(HEADERS, rows, DAY)) == 1


def test_parse_readings_requires_genco_column():
    with pytest.raises(ScrapeError):
        parse_readings(["#", "Plant", "01:00"], [], DAY)


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 9, 24, 0, 5), [date(2026, 9, 23), date(2026, 9, 24)]),
        (datetime(2026, 9, 24, 6, 5), [date(2026, 9, 23), date(2026, 9, 24)]),
        (datetime(2026, 9, 24, 7, 5), [date(2026, 9, 24)]),
    ],
)
def test_days_to_scrape(now, expected):
    assert days_to_scrape(now, revalidate_until_hour=7) == expected
