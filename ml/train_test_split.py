#!/usr/bin/env python3
"""
Zaman bazli (chronological) train/test split.

Karistirma YOK: gecmis -> train, gelecek -> test. Egitim sirasinda gelecek
bilgisinin modelin gordugu veriye girmemesi icin split etrafinda istege bagli
bir bosluk (--gap-min) birakilabilir.

Kullanim:
  python ml/train_test_split.py --db data/zabbix_ml.db --table ml_features_enriched \
      --split-ts 1785600000 --gap-min 60 --out data/
  python ml/train_test_split.py --db data/zabbix_ml.db --split-ratio 0.8
"""

import argparse
import os
import sqlite3

import pandas as pd

TARGET_COLUMNS = ("is_anomaly", "time_since_last_alarm")


def load_table(db, table):
    conn = sqlite3.connect(db)
    df = pd.read_sql(f"SELECT * FROM {table} ORDER BY host, datetime_minute", conn)
    conn.close()
    df["datetime_minute"] = pd.to_datetime(df["datetime_minute"])
    return df


def drop_hourly(df):
    cols = [c for c in df.columns if "_hourly_" not in c]
    dropped = len(df.columns) - len(cols)
    if dropped:
        print(f"[split] {dropped} _hourly_* sutunu atlandi (leakage korumasi)")
    return df[cols]


def split_time_based(df, split_ts=None, ratio=None, gap_min=0):
    tmin = df["datetime_minute"].min()
    tmax = df["datetime_minute"].max()
    if split_ts is not None:
        split = pd.Timestamp(split_ts, unit="s")
    elif ratio is not None:
        split = tmin + (tmax - tmin) * ratio
    else:
        raise ValueError("--split-ts veya --split-ratio gereklidir")

    gap = pd.Timedelta(minutes=gap_min)
    train = df[df["datetime_minute"] < split - gap].copy()
    test = df[df["datetime_minute"] >= split + gap].copy()
    return train, test, split


def class_balance(df, label="is_anomaly"):
    if label not in df.columns:
        return "(-)"
    return f"({int(df[label].sum())}/{len(df)})"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/zabbix_ml.db")
    parser.add_argument("--table", default="ml_features_enriched")
    parser.add_argument("--split-ts", type=int, default=None,
                        help="UTC epoch; bu andan once train, sonra test")
    parser.add_argument("--split-ratio", type=float, default=None,
                        help="0-1; zaman araligi uzunluguna gore")
    parser.add_argument("--gap-min", type=int, default=0,
                        help="split etrafinda atilan bosluk (dakika)")
    parser.add_argument("--drop-hourly", action="store_true", default=True,
                        help="_hourly_* sutunlarini dus (varsayilan acik)")
    parser.add_argument("--no-drop-hourly", action="store_true", help="_hourly_* kalsin")
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    if args.no_drop_hourly:
        args.drop_hourly = False

    df = load_table(args.db, args.table)
    if args.drop_hourly:
        df = drop_hourly(df)

    train, test, split = split_time_based(
        df, split_ts=args.split_ts, ratio=args.split_ratio, gap_min=args.gap_min
    )

    os.makedirs(args.out, exist_ok=True)
    train_path = os.path.join(args.out, "train.csv")
    test_path = os.path.join(args.out, "test.csv")
    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)

    print(f"[split] split noktasi: {split}")
    print(f"[split] train: {len(train)} satir  {class_balance(train)}")
    print(f"[split] test : {len(test)} satir  {class_balance(test)}")
    print(f"[split] yazildi: {train_path}, {test_path}")


if __name__ == "__main__":
    main()
