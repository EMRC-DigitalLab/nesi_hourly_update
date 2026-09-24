from datetime import datetime
from decimal import Decimal

from nesi.disco import parse_allocations

AT = datetime(2026, 9, 24, 14, 5, 12)


def test_parse_allocations_keeps_disco_rows_only():
    rows = [
        ["Distribution Company", "Load Allocation (MW)"],
        ["Abuja Disco", "1,234.5 MW"],
        ["Ikeja Disco", "(987)"],
        ["TCN", "50"],
        ["Kano Disco", "not available"],
        ["Total"],
    ]
    got = parse_allocations(rows, AT)
    assert [(a.company, a.mw) for a in got] == [
        ("Abuja Disco", Decimal("1234.5")),
        ("Ikeja Disco", Decimal("987")),
    ]
    assert all(a.at == AT for a in got)
