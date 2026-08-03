#!/usr/bin/env python3
"""
Feature selection: 813 ozellik vs ~5000 ornek problemi icin boyut kucultme.

1. Sabite yakin sutunlari ele (varyans esigi)
2. |korelasyon| > esik olan ciftlerden birini ele (greedy)
3. Istege bagli: en yuksek varyansli top-k tut

Hedef/meta sutunlari (datetime_minute, host, is_anomaly, time_since_last_alarm)
ve _hourly_* (leakage) adaylardan cikarilir.

Kullanim:
  python ml/select_features.py --db data/zabbix_ml.db --out data/selected_features.csv
  python ml/select_features.py --input data/train.csv --top-k 200
"""

import argparse
import os
import sqlite3

import numpy as np
import pandas as pd

EXCLUDED = {
    "datetime_minute", "host", "is_anomaly", "time_since_last_alarm",
    "anomaly", "label",
}


def load_data(db=None, table=None, input_csv=None):
    if input_csv:
        df = pd.read_csv(input_csv, parse_dates=["datetime_minute"])
        return df
    conn = sqlite3.connect(db)
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    df["datetime_minute"] = pd.to_datetime(df["datetime_minute"])
    return df


def candidate_columns(df, keep_hourly=False):
    cols = []
    for c in df.columns:
        if c in EXCLUDED:
            continue
        if "_hourly_" in c and not keep_hourly:
            continue
        if not pd.api.types.is_numeric_dtype(df[c]):
            continue
        cols.append(c)
    return cols


def filter_variance(df, cols, threshold):
    variances = df[cols].var()
    kept = variances[variances > threshold].index.tolist()
    dropped = len(cols) - len(kept)
    return kept, dropped


def dedup_correlation(df, cols, threshold):
    if len(cols) <= 1:
        return cols
    corr = df[cols].corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape, dtype=bool), k=1))
    to_drop = set()
    for col in cols:
        if col in to_drop:
            continue
        for other in cols:
            if other in to_drop or other == col:
                continue
            if upper.loc[col, other] > threshold:
                to_drop.add(other)
    return [c for c in cols if c not in to_drop]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/zabbix_ml.db")
    parser.add_argument("--table", default="ml_features_enriched")
    parser.add_argument("--input", default=None, help="CSV kullanilacaksa")
    parser.add_argument("--variance-threshold", type=float, default=0.0,
                        help="altinda varyans olan sutunlar elenir")
    parser.add_argument("--corr-threshold", type=float, default=0.95)
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--keep-hourly", action="store_true",
                        help="_hourly_* sutunlarini da aday yap")
    parser.add_argument("--out", default="data/selected_features.csv")
    args = parser.parse_args()

    df = load_data(db=args.db, table=args.table, input_csv=args.input)
    cols = candidate_columns(df, keep_hourly=args.keep_hourly)
    print(f"[select] aday sutun: {len(cols)} (toplam {len(df.columns)})")

    kept, dropped = filter_variance(df, cols, args.variance_threshold)
    if dropped:
        print(f"[select] varyans esigi sonrasi: {dropped} elendi")

    kept = dedup_correlation(df, kept, args.corr_threshold)
    print(f"[select] korelasyon esigi ({args.corr_threshold}) sonrasi: {len(kept)}")

    if args.top_k and len(kept) > args.top_k:
        variances = df[kept].var().sort_values(ascending=False)
        kept = variances.head(args.top_k).index.tolist()
        print(f"[select] top-{args.top_k} varyans: {len(kept)}")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    pd.Series(kept, name="feature").to_csv(args.out, index=False)
    print(f"[select] secilen liste yazildi: {args.out} ({len(kept)} sutun)")


if __name__ == "__main__":
    main()
