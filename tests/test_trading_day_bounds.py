"""ET extended-session window used by /candles and /chart."""

from datetime import date, datetime, timezone

import pytest

from apps.api.trading_day_bounds import extended_session_utc_bounds, parse_trade_date


def test_parse_trade_date() -> None:
    assert parse_trade_date("2024-01-15") == date(2024, 1, 15)


def test_parse_trade_date_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        parse_trade_date("15-01-2024")


def test_extended_bounds_est_winter() -> None:
    start, end = extended_session_utc_bounds(date(2024, 1, 15))
    assert start == datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
    assert end == datetime(2024, 1, 16, 1, 0, tzinfo=timezone.utc)


def test_extended_bounds_edt_summer() -> None:
    start, end = extended_session_utc_bounds(date(2024, 7, 15))
    assert start == datetime(2024, 7, 15, 8, 0, tzinfo=timezone.utc)
    assert end == datetime(2024, 7, 16, 0, 0, tzinfo=timezone.utc)
