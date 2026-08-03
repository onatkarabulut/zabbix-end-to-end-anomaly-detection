import pandas as pd

import train_test_split as tts


def _df():
    t = pd.date_range("2026-07-01 00:00", periods=100, freq="min")
    return pd.DataFrame({
        "datetime_minute": t,
        "host": ["h1"] * 100,
        "system.cpu.util_avg_5m": range(100),
        "system.cpu.util_hourly_avg": [1.0] * 100,
        "is_anomaly": [1] * 10 + [0] * 90,
    })


def _split_epoch(ts_str):
    return int(pd.Timestamp(ts_str, tz="UTC").timestamp())


def test_split_is_disjoint_and_chronological():
    df = _df()
    train, test, split = tts.split_time_based(
        df, split_ts=_split_epoch("2026-07-01 00:30"), gap_min=0
    )
    assert split == pd.Timestamp("2026-07-01 00:30")
    assert train["datetime_minute"].max() < test["datetime_minute"].min()
    assert len(train) + len(test) == len(df)


def test_gap_excludes_boundary_rows():
    df = _df()
    train, test, _ = tts.split_time_based(
        df, split_ts=_split_epoch("2026-07-01 00:30"), gap_min=10
    )
    assert train["datetime_minute"].max() == pd.Timestamp("2026-07-01 00:19")
    assert test["datetime_minute"].min() == pd.Timestamp("2026-07-01 00:40")
    assert len(train) + len(test) == len(df) - 20


def test_split_ratio():
    df = _df()
    train, test, _ = tts.split_time_based(df, ratio=0.7, gap_min=0)
    assert 0 <= len(train) / len(df) - 0.7 < 0.02
    assert len(train) + len(test) == len(df)


def test_drop_hourly():
    df = _df()
    out = tts.drop_hourly(df)
    assert not any("_hourly_" in c for c in out.columns)
    assert "system.cpu.util_avg_5m" in out.columns
