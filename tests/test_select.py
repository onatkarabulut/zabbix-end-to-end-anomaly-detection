import pandas as pd

import select_features as sf


def _df():
    return pd.DataFrame({
        "datetime_minute": pd.to_datetime("2026-07-01"),
        "host": ["h1"],
        "is_anomaly": [0],
        "time_since_last_alarm": [0],
        "a": [1.0], "b": [2.0], "c": [3.0],
        "const": [5.0] * 1,
        "x_hourly_avg": [1.0],
    })


def test_candidate_columns_excludes_meta_and_hourly():
    cols = sf.candidate_columns(_df(), keep_hourly=False)
    assert set(cols) == {"a", "b", "c", "const"}


def test_candidate_columns_keeps_hourly_if_requested():
    cols = sf.candidate_columns(_df(), keep_hourly=True)
    assert "x_hourly_avg" in cols


def test_filter_variance_drops_constants():
    df = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [5.0, 5.0, 5.0]})
    kept, dropped = sf.filter_variance(df, ["a", "b"], threshold=0.0)
    assert kept == ["a"]
    assert dropped == 1


def test_dedup_correlation_removes_highly_correlated():
    df = pd.DataFrame({
        "x": [1.0, 2.0, 3.0, 4.0],
        "y": [2.0, 4.0, 6.0, 8.0],
        "z": [1.0, 5.0, 2.0, 9.0],
    })
    kept = sf.dedup_correlation(df, ["x", "y", "z"], threshold=0.95)
    assert len(kept) == 2
    assert not ({"x", "y"} <= set(kept))
