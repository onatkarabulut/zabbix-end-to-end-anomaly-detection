import os
import sqlite3
import tempfile

import pandas as pd
import pytest

from ETL.feature_engineering import (
    _is_metric,
    ALARM_LABEL_WINDOW_MIN,
    compute_enriched_features,
)


def _build_mini_db(path):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE hosts (hostid INTEGER PRIMARY KEY, host TEXT, name TEXT, status INTEGER)")
    conn.execute("CREATE TABLE items (itemid INTEGER PRIMARY KEY, hostid INTEGER, name TEXT, key_ TEXT, value_type INTEGER, status INTEGER)")
    conn.execute("CREATE TABLE triggers (triggerid INTEGER PRIMARY KEY, expression TEXT, description TEXT, priority INTEGER, status INTEGER)")
    conn.execute("CREATE TABLE functions (functionid INTEGER PRIMARY KEY, itemid INTEGER, triggerid INTEGER, name TEXT, parameter TEXT)")
    conn.execute("CREATE TABLE events (eventid INTEGER PRIMARY KEY, source INTEGER, object INTEGER, objectid INTEGER, clock REAL, ns INTEGER, value INTEGER, name TEXT, severity INTEGER)")
    conn.execute("INSERT INTO hosts VALUES (1, 'h1', 'Host 1', 0)")
    conn.execute("INSERT INTO items VALUES (1, 1, 'CPU util', 'system.cpu.util', 0, 0)")
    conn.execute("INSERT INTO triggers VALUES (1, 'last(/h1/system.cpu.util)>80', 'High CPU', 2, 0)")
    conn.execute("INSERT INTO functions VALUES (1, 1, 1, 'last', '0')")
    alarm_clock = int(pd.Timestamp("2026-07-23 12:00:00").timestamp())
    conn.execute(
        "INSERT INTO events VALUES (1, 0, 0, 1, ?, 0, 1, 'High CPU', 2)",
        (alarm_clock,),
    )
    conn.commit()
    conn.close()


def test_is_metric_excludes_hourly_and_meta():
    assert _is_metric("system.cpu.util") is True
    assert _is_metric("vm.memory.size[available]") is True
    assert _is_metric("system.cpu.util_hourly_avg") is False
    assert _is_metric("datetime_minute") is False
    assert _is_metric("host") is False


def test_is_anomaly_and_time_since_last_alarm_from_events():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db = tmp.name
    try:
        _build_mini_db(db)
        minutes = pd.date_range("2026-07-23 11:55", periods=20, freq="min")
        df = pd.DataFrame({
            "datetime_minute": minutes,
            "host": "h1",
            "system.cpu.util": [1.0] * 20,
        })
        out = compute_enriched_features(df, db_path=db)
        out = out.set_index("datetime_minute")

        at_alarm = out.loc["2026-07-23 12:00:00"]
        assert at_alarm["time_since_last_alarm"] == 0.0
        assert out.loc["2026-07-23 12:05:00"]["time_since_last_alarm"] == 5.0
        assert out.loc["2026-07-23 11:55:00"]["time_since_last_alarm"] == 999999

        window = out.loc["2026-07-23 12:00:00":"2026-07-23 12:09:00"]
        assert (window["is_anomaly"] == 1.0).all()
        assert out.loc["2026-07-23 12:10:00"]["is_anomaly"] == 0.0
        assert out.loc["2026-07-23 11:55:00"]["is_anomaly"] == 0.0
    finally:
        os.unlink(db)


def test_no_db_path_means_no_labels():
    minutes = pd.date_range("2026-07-23 11:55", periods=5, freq="min")
    df = pd.DataFrame({
        "datetime_minute": minutes,
        "host": "h1",
        "system.cpu.util": [1.0] * 5,
    })
    out = compute_enriched_features(df, db_path=None)
    assert (out["is_anomaly"] == 0.0).all()
    assert "time_since_last_alarm" not in out.columns
    assert ALARM_LABEL_WINDOW_MIN > 0
