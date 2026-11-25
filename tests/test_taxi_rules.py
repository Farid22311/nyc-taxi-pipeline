import pandas as pd
import pytest

from taxi_rules import check_parquet_file, clean_frame, is_not_published_status


def make_trips(rows):
    df = pd.DataFrame(
        rows,
        columns=["tpep_pickup_datetime", "passenger_count", "fare_amount", "total_amount"],
    )
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    return df


def test_pickups_before_2020_are_dropped():
    df = make_trips([("2009-01-01 08:00", 1, 10.0, 12.0), ("2026-08-01 08:00", 1, 10.0, 12.0)])
    assert len(clean_frame(df)) == 1


def test_july_pickup_in_august_file_is_kept():
    df = make_trips([("2026-07-31 23:50", 1, 10.0, 12.0)])
    assert len(clean_frame(df)) == 1


def test_missing_passenger_count_is_flagged_but_kept():
    df = make_trips([("2026-08-01 08:00", None, 10.0, 12.0), ("2026-08-01 09:00", 2, 10.0, 12.0)])
    out = clean_frame(df)
    assert len(out) == 2
    assert out["has_passenger_count"].tolist() == [False, True]


def test_zero_passengers_counts_as_having_a_passenger_count():
    df = make_trips([("2026-08-01 08:00", 0, 10.0, 12.0)])
    assert clean_frame(df)["has_passenger_count"].tolist() == [True]


def test_reversal_rule_uses_fare_or_total():
    df = make_trips(
        [
            ("2026-08-01 08:00", 1, -5.0, 3.0),
            ("2026-08-01 08:00", 1, 5.0, -3.0),
            ("2026-08-01 08:00", 1, 5.0, 3.0),
            ("2026-08-01 08:00", 1, 0.0, 0.0),
        ]
    )
    assert clean_frame(df)["is_reversal"].tolist() == [True, True, False, False]


def test_reversals_are_kept_not_dropped():
    df = make_trips([("2026-08-01 08:00", 1, -5.0, -5.0)])
    assert len(clean_frame(df)) == 1


def test_file_that_is_too_small_is_rejected(tmp_path):
    path = tmp_path / "small.parquet"
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(path)
    with pytest.raises(ValueError, match="bytes"):
        check_parquet_file(path, min_bytes=10_000_000)


def test_file_without_parquet_markers_is_rejected(tmp_path):
    path = tmp_path / "fake.parquet"
    path.write_bytes(b"x" * 2000)
    with pytest.raises(ValueError, match="not a valid Parquet"):
        check_parquet_file(path, min_bytes=1000)


def test_valid_parquet_file_passes(tmp_path):
    path = tmp_path / "ok.parquet"
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(path)
    assert check_parquet_file(path, min_bytes=1) > 0


@pytest.mark.parametrize("code, expected", [(403, True), (404, True), (200, False), (500, False), (429, False)])
def test_not_published_status(code, expected):
    assert is_not_published_status(code) is expected
