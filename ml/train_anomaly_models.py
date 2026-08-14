#!/usr/bin/env python3
"""
Faz 3 - Uc modelin karsilastirmali anomali tespit egitimi.

Data leakage korumasi (PDF Faz 2/3 kosulu):

1. Ham `ml_feature_matrix`'ten yuklenir (rolling ozellik icermez, saf).
2. Zaman bazli sabit split: gecmis -> train, gelecek -> test (--split-ts).
3. Kayan pencere (_avg/_std/_trend) ve scaler train partition'inda FIT edilir,
   test partition'ina ayri ayri UYGULANIR. Test bilgisi asla train'e sicmaz.
4. Ozellik secimi (varyans + korelasyon + top-k) sadece train'de yapilir.
5. y_true `data/ground_truth.jsonl` pencerelerinden uretilir ve asla ozellik
   olarak modele verilmez.

Modeller:
  - IsolationForest (sklearn)
  - OneClassSVM (sklearn, RBF)
  - LSTM-Autoencoder (torch) - torch yoksa atlanir

Ciktilari:
  - data/models/<model>.joblib veya .pt
  - data/eval_results.csv      : precision/recall/F1 (y_true = sentetik testler)
  - data/predictions.csv       : dakika bazli skor + tahmin + gercek etiket
  - data/comp_vs_zabbix.csv    : ML vs Zabbix alarm karsilastirmasi (gecikme + FP)

Kullanim:
  python ml/train_anomaly_models.py --db data/zabbix_ml.db --split-ts 1785801600
  python ml/train_anomaly_models.py --db data/zabbix_ml.db --split-ts 1785801600 \
      --thr-quantile 0.99 --top-k 300 --seq-len 60 --epochs 10 --skip-lstm
"""

import argparse
import json
import os
import sqlite3
import sys

import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import precision_score, recall_score, f1_score
    SKLEARN_OK = True
except Exception as e:
    print(f"[train] sklearn yuklenemedi: {e}")
    SKLEARN_OK = False

try:
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_OK = True
except Exception as e:
    print(f"[train] torch yuklenemedi (LSTM atlanacak): {e}")
    TORCH_OK = False

ROLLING_WINDOWS = (5, 30, 60)

GROUND_TRUTH_FILE = os.path.join(
    os.path.dirname(__file__), "..", "data", "ground_truth.jsonl"
)

METRIC_PREFIXES = (
    "system.cpu", "vm.memory", "vfs.dev", "vfs.fs", "system.swap",
    "zabbix[process", "zabbix[preprocessing", "zabbix[vcache", "zabbix[rcache",
    "zabbix[tcache", "zabbix[wcache", "zabbix[vps",
)


def _is_metric(col):
    if col in ("datetime_minute", "host"):
        return False
    if "_hourly_" in col:
        return False
    return any(col.startswith(p) for p in METRIC_PREFIXES)


def _rolling_trend(series, window):
    def _slope(y):
        if len(y) < 2:
            return 0.0
        return float((y.iloc[-1] - y.iloc[0]) / max(len(y), 1))
    return series.rolling(window, min_periods=2).apply(_slope, raw=False)


def load_raw_matrix(db, table="ml_feature_matrix"):
    conn = sqlite3.connect(db)
    df = pd.read_sql(f"SELECT * FROM {table} ORDER BY host, datetime_minute", conn)
    conn.close()
    df["datetime_minute"] = pd.to_datetime(df["datetime_minute"])
    return df


def add_rolling_features(df):
    metric_cols = [c for c in df.columns if _is_metric(c)]
    parts = [df[["datetime_minute", "host"]].copy()]
    for col in metric_cols:
        for w in ROLLING_WINDOWS:
            parts.append(df.groupby("host")[col].transform(
                lambda x: x.rolling(w, min_periods=1).mean()
            ).rename(f"{col}_avg_{w}m"))
            parts.append(df.groupby("host")[col].transform(
                lambda x: x.rolling(w, min_periods=1).std().fillna(0)
            ).rename(f"{col}_std_{w}m"))
            parts.append(df.groupby("host")[col].transform(
                lambda x: _rolling_trend(x, w)
            ).rename(f"{col}_trend_{w}m"))
    return pd.concat(parts, axis=1)


def select_features_on_train(train, top_k=300):
    excluded = {"datetime_minute", "host", "is_anomaly", "time_since_last_alarm"}
    cols = [
        c for c in train.columns
        if c not in excluded and pd.api.types.is_numeric_dtype(train[c])
    ]
    variances = train[cols].var()
    cols = variances[variances > 1e-6].index.tolist()
    if len(cols) <= 1:
        return cols
    corr = train[cols].corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape, dtype=bool), k=1))
    to_drop = set()
    for col in cols:
        if col in to_drop:
            continue
        for other in cols:
            if other in to_drop or other == col:
                continue
            if upper.loc[col, other] > 0.95:
                to_drop.add(other)
    kept = [c for c in cols if c not in to_drop]
    if top_k and len(kept) > top_k:
        variances = train[kept].var().sort_values(ascending=False)
        kept = variances.head(top_k).index.tolist()
    return kept


def read_gt_windows():
    windows = {}
    if not os.path.exists(GROUND_TRUTH_FILE):
        return windows
    with open(GROUND_TRUTH_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            host = rec.get("host") or "Zabbix server"
            windows.setdefault(host, []).append(
                (int(rec["start_ts"]), int(rec["end_ts"]), rec.get("label", rec["type"]))
            )
    return windows


def build_y_true(df, windows):
    y_true = np.zeros(len(df), dtype=int)
    anomaly = [""] * len(df)
    for i, row in df.iterrows():
        ts = int(row["datetime_minute"].timestamp())
        for (start, end, lbl) in windows.get(row["host"], []):
            if start <= ts < end:
                y_true[i] = 1
                anomaly[i] = lbl
                break
    return y_true, anomaly


class LSTMAutoencoder(nn.Module):
    def __init__(self, n_feat, hidden=32, latent=8):
        super().__init__()
        self.encoder = nn.LSTM(n_feat, hidden, batch_first=True)
        self.latent = nn.Linear(hidden, latent)
        self.decode = nn.Linear(latent, hidden)
        self.decoder = nn.LSTM(hidden, n_feat, batch_first=True)

    def forward(self, x):
        out, _ = self.encoder(x)
        code = self.latent(out[:, -1, :]).unsqueeze(1).repeat(1, x.shape[1], 1)
        out = self.decode(code)
        rec, _ = self.decoder(out)
        return rec


def make_sequences(X, seq_len):
    n, nf = X.shape
    n_seq = n - seq_len + 1
    seqs = np.zeros((n_seq, seq_len, nf))
    for i in range(n_seq):
        seqs[i] = X[i:i + seq_len]
    return seqs


def train_lstm(X, seq_len, epochs, device):
    if not TORCH_OK:
        return None, None, None
    torch.manual_seed(42)
    seqs = make_sequences(X, seq_len)
    dataset = TensorDataset(torch.from_numpy(seqs).float())
    loader = DataLoader(dataset, batch_size=64, shuffle=True)
    model = LSTMAutoencoder(X.shape[1]).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3)
    criterion = nn.MSELoss()
    model.train()
    for ep in range(epochs):
        total = 0.0
        for (xb,) in loader:
            xb = xb.to(device)
            opt.zero_grad()
            rec = model(xb)
            loss = criterion(rec, xb)
            loss.backward()
            opt.step()
            total += loss.item() * len(xb)
        print(f"[train] LSTM epoch {ep + 1}/{epochs} loss={total / len(dataset):.6f}")
    model.eval()
    with torch.no_grad():
        rec = model(torch.from_numpy(seqs).float().to(device)).cpu().numpy()
    score = np.mean((seqs - rec) ** 2, axis=(1, 2))
    return model, score, seqs


def score_lstm(model, X, seq_len, device):
    seqs = make_sequences(X, seq_len)
    model.eval()
    with torch.no_grad():
        rec = model(torch.from_numpy(seqs).float().to(device)).cpu().numpy()
    return np.mean((seqs - rec) ** 2, axis=(1, 2))


def fillna_partition(df, cols):
    df = df.copy()
    df[cols] = df[cols].fillna(0.0)
    return df

def main():
    global args
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/zabbix_ml.db")
    parser.add_argument("--table", default="ml_feature_matrix")
    parser.add_argument("--split-ts", type=int, default=1785801600,
                        help="UTC epoch; bu andan once train, sonra test (8/4 00:00 = 1785801600)")
    parser.add_argument("--thr-quantile", type=float, default=0.99,
                        help="train skorunun bu quantile ustu anomali sayilir")
    parser.add_argument("--top-k", type=int, default=300)
    parser.add_argument("--seq-len", type=int, default=60)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--skip-lstm", action="store_true")
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    if not SKLEARN_OK:
        print("[train] sklearn yok, cikiliyor. Kurulum: pip install pandas numpy scikit-learn")
        sys.exit(1)

    device = "cuda" if (TORCH_OK and torch.cuda.is_available()) else "cpu"
    print(f"[train] device={device}")
    os.makedirs(os.path.join(args.out, "models"), exist_ok=True)

    raw = load_raw_matrix(args.db, args.table)
    print(f"[train] ham matris: {len(raw)} satir, {len(raw.columns)} kolon")
    split = pd.Timestamp(args.split_ts, unit="s")
    train_raw = raw[raw["datetime_minute"] < split].copy()
    test_raw = raw[raw["datetime_minute"] >= split].copy()
    print(f"[train] split={split} -> train {len(train_raw)}, test {len(test_raw)}")
    if train_raw.empty or test_raw.empty:
        print("[train] split taraflarindan biri bos. --split-ts kontrol edin.")
        sys.exit(1)

    train = add_rolling_features(train_raw)
    test = add_rolling_features(test_raw)
    print(f"[train] rolling ozellikler: train {train.shape}, test {test.shape}")

    y_true, anomaly = build_y_true(
        pd.concat([train, test]), read_gt_windows()
    )
    y_true_train = y_true[: len(train)]
    y_true_test = y_true[len(train):]
    print(f"[train] y_true train: {y_true_train.sum()} anomali, test: {y_true_test.sum()} anomali")

    cols = select_features_on_train(train)
    print(f"[train] secilen ozellik: {len(cols)}")

    X_train = fillna_partition(train, cols)[cols].to_numpy(dtype=float)
    X_test = fillna_partition(test, cols)[cols].to_numpy(dtype=float)
    scaler = StandardScaler().fit(X_train)
    X_train_s = scaler.transform(X_train)
    X_test_s = scaler.transform(X_test)
    print(f"[train] X_train={X_train_s.shape} X_test={X_test_s.shape}")

    results = []

    if not args.skip_lstm and TORCH_OK:
        if len(test) < args.seq_len:
            print(f"[train] test {len(test)} satir < seq_len {args.seq_len}, LSTM atlandi")
        else:
            model_lstm, lstm_train_score, _ = train_lstm(
                X_train_s, args.seq_len, args.epochs, device
            )
            thr_lstm = np.quantile(lstm_train_score, args.thr_quantile)
            lstm_test_score = score_lstm(model_lstm, X_test_s, args.seq_len, device)
            lstm_pred = np.zeros(len(test), dtype=int)
            lstm_pred[args.seq_len - 1:] = (lstm_test_score > thr_lstm).astype(int)
            lstm_score = np.full(len(test), np.nan)
            lstm_score[args.seq_len - 1:] = lstm_test_score
            results.append({
                "model": "LSTM-Autoencoder", "thr": thr_lstm,
                "score": lstm_score, "pred": lstm_pred, "model_obj": model_lstm,
            })
            print(f"[train] LSTM: thr={thr_lstm:.6f}, test'te {lstm_pred.sum()} anomali")
    elif args.skip_lstm:
        print("[train] LSTM atlandi (--skip-lstm)")
    else:
        print("[train] torch yok, LSTM atlaniyor")

    if SKLEARN_OK:
        models = {
            "IsolationForest": IsolationForest(
                contamination="auto", random_state=42, n_jobs=-1
            ),
            "OneClassSVM": OneClassSVM(kernel="rbf", nu=1.0 - args.thr_quantile, gamma="scale"),
        }
        for name, model in models.items():
            print(f"[train] {name} egitiliyor...")
            model.fit(X_train_s)
            if name == "IsolationForest":
                train_score = -model.score_samples(X_train_s)
                test_score = -model.score_samples(X_test_s)
            else:
                train_score = -model.decision_function(X_train_s)
                test_score = -model.decision_function(X_test_s)
            thr = np.quantile(train_score, args.thr_quantile)
            pred = (test_score > thr).astype(int)
            results.append({
                "model": name, "thr": thr, "score": test_score, "pred": pred,
                "model_obj": model,
            })
            print(f"[train] {name}: thr={thr:.6f}, test'te {pred.sum()} anomali")

    eval_rows = []
    pred_rows = []
    for res in results:
        pred = res["pred"]
        score = res["score"]
        scored_mask = ~np.isnan(score)
        p, r, f = precision_score(y_true_test[scored_mask], pred[scored_mask].astype(int), zero_division=0), \
            recall_score(y_true_test[scored_mask], pred[scored_mask].astype(int), zero_division=0), \
            f1_score(y_true_test[scored_mask], pred[scored_mask].astype(int), zero_division=0)
        eval_rows.append({
            "model": res["model"], "thr_quantile": args.thr_quantile,
            "threshold": round(res["thr"], 6), "precision": round(p, 4),
            "recall": round(r, 4), "f1": round(f, 4),
            "n_pred_pos": int(pred[scored_mask].sum()),
        })
        rows = test.copy()
        rows["model"] = res["model"]
        rows["score"] = score
        rows["y_pred"] = pred.astype(int)
        rows["y_true"] = y_true_test
        rows["anomaly"] = anomaly[len(train): len(train) + len(rows)]
        pred_rows.append(rows.reset_index(drop=True))

    eval_df = pd.DataFrame(eval_rows)
    eval_path = os.path.join(args.out, "eval_results.csv")
    eval_df.to_csv(eval_path, index=False)
    print(f"[train] degerlendirme yazildi: {eval_path}")
    print(eval_df.to_string(index=False))

    pred_df = pd.concat(pred_rows, ignore_index=True)
    pred_path = os.path.join(args.out, "predictions.csv")
    pred_df.to_csv(pred_path, index=False)
    print(f"[train] tahminler yazildi: {pred_path}")

    comp = build_zabbix_comp(pred_df, args.db)
    comp_path = os.path.join(args.out, "comp_vs_zabbix.csv")
    comp.to_csv(comp_path, index=False)
    print(f"[train] Zabbix karsilastirma yazildi: {comp_path}")
    print(comp.to_string(index=False))

    save_models(scaler, results)


def build_zabbix_comp(pred_df, db):
    zabbix_windows = _read_zabbix_alarm_windows(db)
    rows = []
    for model_name, grp in pred_df.groupby("model"):
        for (start, end, lbl) in _all_gt_windows():
            wins = grp[(grp["datetime_minute"] >= pd.Timestamp(start, unit="s")) &
                       (grp["datetime_minute"] < pd.Timestamp(end, unit="s"))]
            if wins.empty:
                continue
            detected = wins[wins["y_pred"] == 1]
            if detected.empty:
                lat = None
            else:
                first = detected["datetime_minute"].min().timestamp()
                lat = int((first - start) / 60)
            zabbix_detected = any(s <= start < e or s <= end <= e for (s, e) in zabbix_windows)
            rows.append({
                "model": model_name, "window_start": start, "window_end": end,
                "label": lbl, "latency_min": lat,
                "caught": detected.shape[0] > 0,
                "detected_minutes": detected.shape[0],
                "zabbix_alarm_overlap": zabbix_detected,
            })
    return pd.DataFrame(rows)


def _read_zabbix_alarm_windows(db):
    try:
        with sqlite3.connect(db) as conn:
            alarms = pd.read_sql(
                "SELECT e.clock "
                "FROM events e "
                "JOIN triggers t ON e.objectid = t.triggerid "
                "JOIN functions f ON t.triggerid = f.triggerid "
                "JOIN items i ON f.itemid = i.itemid "
                "JOIN hosts h ON i.hostid = h.hostid "
                "WHERE e.source = 0 AND e.value = 1 "
                "AND h.host = 'Zabbix server'",
                conn,
            )
    except Exception:
        return []
    if alarms.empty:
        return []
    clock = alarms["clock"]
    if not pd.api.types.is_numeric_dtype(clock.dtype):
        clock = pd.to_numeric(clock, errors="coerce")
    clocks = clock.dropna().astype(int).tolist()
    return [(c, c + 10 * 60) for c in clocks]


def _all_gt_windows():
    wins = []
    for host_wins in read_gt_windows().values():
        wins.extend(host_wins)
    return wins


def save_models(scaler, results):
    import joblib
    out = os.path.join(args.out, "models")
    os.makedirs(out, exist_ok=True)
    joblib.dump(scaler, os.path.join(out, "scaler.joblib"))
    for res in results:
        name = res["model"].lower().replace(" ", "_")
        with open(os.path.join(out, f"{name}_thr.txt"), "w") as f:
            f.write(str(res["thr"]))
        obj = res.get("model_obj")
        if obj is not None:
            joblib.dump(obj, os.path.join(out, f"{name}.joblib"))
    print(f"[train] modeller kaydedildi: {out}/")


if __name__ == "__main__":
    main()
