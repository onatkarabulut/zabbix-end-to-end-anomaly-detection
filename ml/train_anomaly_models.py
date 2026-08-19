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
  - data/models/runs/<run_id>/<model>.joblib veya .pt   (versiyonlu)
  - data/eval_results.csv      : precision/recall/F1 (y_true = sentetik testler)
  - data/predictions.csv       : dakika bazli skor + tahmin + gercek etiket
  - data/comp_vs_zabbix.csv    : ML vs Zabbix alarm karsilastirmasi (gecikme + FP)

Versiyonlama:
  Her egitim runs/<run_id>/ altina kaydedilir; eski modeller silinmez.
  --promote ile bu run data/models/current.json icinde 'aktif' isaretlenir
  (consumer ve audit aktif run'i kullanir). Aktif edilmeden once eski model
  gecerli kalir -> begenilmeyen retrain kolayca geri alinabilir.

Kullanim:
  python ml/train_anomaly_models.py --db data/zabbix_ml.db --split-ts 1785801600
  python ml/train_anomaly_models.py --db data/zabbix_ml.db --split-ts 1785801600 \
      --thr-quantile 0.99 --top-k 300 --seq-len 60 --epochs 10 --skip-lstm --promote
"""

import argparse
import calendar
import json
import os
import sqlite3
import sys
import time

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


def load_raw_matrix(db, table="ml_feature_matrix"):
    conn = sqlite3.connect(db)
    df = pd.read_sql(f"SELECT * FROM {table} ORDER BY host, datetime_minute", conn)
    conn.close()
    df["datetime_minute"] = pd.to_datetime(df["datetime_minute"])
    return df


def _host_minute_grid(df, host):
    """Bir host'un satirlarini kesintisiz 1-dk izgaraya reindex'ler.

    Rolling ozellikler ZAMAN-bazli olmalidir (realtime scorer ile birebir ayni
    kural: '5m ort' = son 5 GERCEK dakika). Satir-bazli pandas rolling, veri
    bosluklarinda gercek zaman penceresinden sapar ve egitim/canli skor
    uyumsuzluguna yol acar. Bozuk dakikalar NaN'dir.
    """
    g = df[df["host"] == host].sort_values("datetime_minute")
    if g.empty or g["datetime_minute"].isna().all():
        return pd.DataFrame(index=pd.DatetimeIndex([]))
    idx = pd.date_range(g["datetime_minute"].min(), g["datetime_minute"].max(), freq="min")
    return g.set_index("datetime_minute").reindex(idx)


def _time_based_rolling_stats(s, w):
    """Kesintisiz dakika izgarasi uzerinde realtime scorer (_feature_vector)
    ile birebir ayni ZAMAN-bazli kayan pencere istatistikleri:

      avg  : son w dk'daki mevcut degerlerin ortalamasi (NaN yoksayilir)
      std  : populasyon std (ddof=0, np.std gibi); <2 deger varsa 0
      trend: (son mevcut - ilk mevcut) / mevcut deger sayisi; <2 ise 0

    Pandas rolling varsayilan ddof=1 (orneklem std) kullanir; realtime
    np.std(ddof=0) kullandigindan ddof=0 verilir. Satir-bazli degil pencere
    zaman-bazlidir (boslukta NaN sayilir) -> train/inference uyumlu.
    """
    present = s.notna()
    cnt = present.rolling(w, min_periods=1).sum()
    avg = s.rolling(w, min_periods=1).mean()
    std = s.rolling(w, min_periods=1).std(ddof=0).fillna(0)
    # trend: ilk mevcut = window basindaki ilk deger (bfill), son = ffill
    first = s.bfill().shift(w - 1, fill_value=s.bfill().iloc[0] if len(s) else 0.0)
    last = s.ffill()
    trend = ((last - first) / cnt).where(cnt >= 2, 0.0)
    return avg, std, trend


def add_rolling_features(df, history=None):
    """df satirlari icin rolling ozellikler. `history` verilirse (train),
    pencereler bu gecmis uzerinden de bakar (realtime scorer'in hist'i gibi)
    -> test basindaki pencereler bos/NaN kalmaz, eval realtime ile tutarli."""
    metric_cols = [c for c in df.columns if _is_metric(c)]
    base = df[["datetime_minute", "host"]].copy()
    # Zaman ozellikleri: gunluk/haftalik periyodik desenler (drift'i azaltir)
    dt = df["datetime_minute"]
    base["hour_of_day"] = dt.dt.hour.astype(float)
    base["day_of_week"] = dt.dt.dayofweek.astype(float)
    base["is_weekend"] = (dt.dt.dayofweek >= 5).astype(float)

    # Realtime scorer (_feature_vector) ile ayni ZAMAN-bazli kural.
    # Izgara history+df uzerine kurulur; cikti yine sadece df satirlarina.
    combined = df if history is None else pd.concat([history, df], ignore_index=True)
    parts = [base]
    for host in df["host"].unique():
        grid = _host_minute_grid(combined, host)
        mask = (df["host"] == host).to_numpy()
        ts_of_rows = df.loc[mask, "datetime_minute"]
        for col in metric_cols:
            s = grid[col].astype(float)
            for w in ROLLING_WINDOWS:
                avg, std, trend = _time_based_rolling_stats(s, w)
                for stat_name, r in (("avg", avg), ("std", std), ("trend", trend)):
                    out = np.full(len(df), np.nan)
                    out[mask] = r.reindex(ts_of_rows).to_numpy()
                    parts.append(pd.Series(out, index=df.index, name=f"{col}_{stat_name}_{w}m"))
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


def _utc_epoch(dt):
    # datetime_minute SQLite'ta UTC wall-clock olarak saklanir (naive).
    # Makine saat diliminden bagimsiz, deterministik UTC epoch uret (K2).
    return int(calendar.timegm(dt.timetuple()))


def build_y_true(df, windows):
    y_true = np.zeros(len(df), dtype=int)
    anomaly = [""] * len(df)
    for i, row in df.iterrows():
        ts = _utc_epoch(row["datetime_minute"])
        for (start, end, lbl) in windows.get(row["host"], []):
            if start <= ts < end:
                y_true[i] = 1
                anomaly[i] = lbl
                break
    return y_true, anomaly


if TORCH_OK:
    class LSTMAutoencoder(nn.Module):
        def __init__(self, n_feat, hidden=64, latent=16, dropout=0.2):
            super().__init__()
            self.encoder = nn.LSTM(n_feat, hidden, batch_first=True)
            self.drop = nn.Dropout(dropout)
            self.latent = nn.Linear(hidden, latent)
            self.decode = nn.Linear(latent, hidden)
            self.decoder = nn.LSTM(hidden, n_feat, batch_first=True)

        def forward(self, x):
            out, _ = self.encoder(x)
            out = self.drop(out)
            code = self.latent(out[:, -1, :]).unsqueeze(1).repeat(1, x.shape[1], 1)
            out = self.decode(code)
            rec, _ = self.decoder(out)
            return rec
else:
    class LSTMAutoencoder:  # noqa: E301
        def __init__(self, *args, **kwargs):
            raise RuntimeError("torch kurulu degil, LSTM kullanilamaz")


def make_sequences(X, seq_len, timestamps=None, max_gap_min=5):
    """Zaman-sirali sekanslar uretir. Her baslangic indeksi icin seq_len'lik
    kayan pencere donulur.
    NOT: Eskiden kopuklugu gecen sekanslar atilirdi - bu, kopukluktan hemen
    sonraki anomali dakikalarini (F1=0'in nedeni) hic skorlanamaz yapiyordu.
    Artik TUM dakikalar skorlanir; eksik/0 degerler realtime'daki
    _feature_matrix gibi 0 vektor ile temsil edilir (tutarlilik)."""
    n, nf = X.shape
    starts = np.arange(0, n - seq_len + 1)
    n_seq = len(starts)
    seqs = np.zeros((n_seq, seq_len, nf))
    for i, s in enumerate(starts):
        seqs[i] = X[s:s + seq_len]
    return seqs, starts


def train_lstm(X, seq_len, epochs, device, timestamps=None, exclude=None):
    """LSTM-Autoencoder egitimi. Erken durdurma icin son %15'i validasyon
    olarak zaman-sirali ayirir; overfit'i dropout + erken durdurma ile onler.
    exclude (bool array, len=X): True satirlar anomali etiketli olup egitimden
    ve esik hesabindan CIKARILIR - autoencoder yalnizca normal davranisi
    ogrenir (anomali paternini ezberleyip duyarsizlasmaz)."""
    if not TORCH_OK:
        return None, None, None
    torch.manual_seed(42)
    seqs, starts = make_sequences(X, seq_len, timestamps)
    if exclude is not None:
        exclude = np.asarray(exclude, dtype=bool)
        last_rows = starts + seq_len - 1
        keep = ~exclude[last_rows]
        seqs, starts = seqs[keep], starts[keep]
    n_val = max(int(len(seqs) * 0.15), 1)
    train_seqs = torch.from_numpy(seqs[:-n_val]).float()
    val_seqs = torch.from_numpy(seqs[-n_val:]).float()
    loader = DataLoader(TensorDataset(train_seqs), batch_size=64, shuffle=True)
    model = LSTMAutoencoder(X.shape[1]).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-4)
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, patience=3, factor=0.5)
    criterion = nn.MSELoss()
    best_val, patience, best_state = float("inf"), 0, None
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
        model.eval()
        with torch.no_grad():
            vrec = model(val_seqs.to(device)).cpu().numpy()
            val_loss = float(np.mean((val_seqs.numpy() - vrec) ** 2))
        model.train()
        sched.step(val_loss)
        print(f"[train] LSTM epoch {ep + 1}/{epochs} train={total / len(train_seqs):.6f} val={val_loss:.6f}")
        if val_loss < best_val - 1e-4:
            best_val, patience = val_loss, 0
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            patience += 1
            if patience >= 5:
                print(f"[train] LSTM erken durdurma (epoch {ep + 1})")
                break
    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        rec = model(torch.from_numpy(seqs).float().to(device)).cpu().numpy()
    # Egitim skoru da noktasal: threshold, testteki score_lstm ile ayni kuraldan
    # uretilir (pencere ortalamasi esigi siskinlestirip anomaliyi bastirirdi).
    score = np.mean((seqs[:, -1, :] - rec[:, -1, :]) ** 2, axis=1)
    return model, score, seqs


def score_lstm(model, X, seq_len, device, timestamps=None):
    """Noktasal LSTM skoru: sekansin SADECE son zaman adiminin (puanlanan
    dakikanin) rekonstruksiyon hatasi. Pencere ortalamasi, kisa anomali
    rampalarini ezip F1=0'a yol aciyordu; son adim odagi bu kaybi onler."""
    seqs, starts = make_sequences(X, seq_len, timestamps)
    model.eval()
    with torch.no_grad():
        rec = model(torch.from_numpy(seqs).float().to(device)).cpu().numpy()
    score = np.mean((seqs[:, -1, :] - rec[:, -1, :]) ** 2, axis=1)
    return score, starts


def fillna_partition(df, cols):
    df = df.copy()
    df[cols] = df[cols].fillna(0.0)
    return df

def main():
    global args
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/zabbix_ml.db")
    parser.add_argument("--table", default="ml_feature_matrix")
    parser.add_argument("--split-ts", type=int,
                        default=int(time.time()) - 14 * 86400,
                        help="UTC epoch; bu andan once train, sonra test (varsayilan: now - 14 gun)")
    parser.add_argument("--thr-quantile", type=float, default=0.99,
                        help="train skorunun bu quantile ustu anomali sayilir")
    parser.add_argument("--lstm-thr-quantile", type=float, default=0.95,
                        help="LSTM icin ayri esik quantile'i. Noktasal skorlarin "
                             "dagilimi cok carpiktir; 0.9995 gibi ucta bir quantile "
                             "LSTM'i asla anomali yakalayamaz yapar (F1=0). "
                             "0.95 daha denge kuran bir varsayilandir.")
    parser.add_argument("--top-k", type=int, default=300)
    parser.add_argument("--seq-len", type=int, default=60)
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--skip-lstm", action="store_true")
    parser.add_argument("--out", default="data")
    parser.add_argument("--run-id", default=None,
                        help="run_<epoch> olusturulur; verilirse sabit (ornek: run_20260818_2114)")
    parser.add_argument("--promote", action="store_true",
                        help="egitim sonrasi bu run'i aktif model yap (current.json)")
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
    test = add_rolling_features(test_raw, history=train_raw)
    print(f"[train] rolling ozellikler: train {train.shape}, test {test.shape}")

    y_true, anomaly = build_y_true(
        pd.concat([train, test]), read_gt_windows()
    )
    y_true_train = y_true[: len(train)]
    y_true_test = y_true[len(train):]
    print(f"[train] y_true train: {y_true_train.sum()} anomali, test: {y_true_test.sum()} anomali")

    cols = select_features_on_train(train, top_k=args.top_k)
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
                X_train_s, args.seq_len, args.epochs, device,
                timestamps=train["datetime_minute"].to_numpy(),
                exclude=y_true_train,
            )
            thr_lstm = np.quantile(lstm_train_score, args.lstm_thr_quantile)
            # LSTM skoru realtime gibi TUM gecmis (train+test) uzerinden kayan
            # pencere ile hesaplanir; cikti sadece test satirlarina yazilir.
            X_combined_s = np.vstack([X_train_s, X_test_s])
            combined_ts = np.concatenate([
                train["datetime_minute"].to_numpy(),
                test["datetime_minute"].to_numpy(),
            ])
            lstm_test_score, lstm_starts = score_lstm(
                model_lstm, X_combined_s, args.seq_len, device,
                timestamps=combined_ts,
            )
            lstm_pred = np.zeros(len(test), dtype=int)
            lstm_score = np.full(len(test), np.nan)
            for i, s in enumerate(lstm_starts):
                t = s + args.seq_len - 1
                if t < len(train):
                    continue
                j = t - len(train)
                lstm_pred[j] = (lstm_test_score[i] > thr_lstm).astype(int)
                lstm_score[j] = lstm_test_score[i]
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
        contamination = 1.0 - args.thr_quantile
        models = {
            "IsolationForest": IsolationForest(
                contamination=contamination, random_state=42, n_jobs=-1,
                n_estimators=300, max_samples="auto", max_features=1.0,
            ),
            "OneClassSVM": OneClassSVM(kernel="rbf", nu=contamination, gamma="scale"),
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
            "n_test_rows": len(test),
            "test_min_ts": int(test["datetime_minute"].min().timestamp()),
            "test_max_ts": int(test["datetime_minute"].max().timestamp()),
            "n_train_rows": len(train),
            "train_min_ts": int(train["datetime_minute"].min().timestamp()),
            "train_max_ts": int(train["datetime_minute"].max().timestamp()),
        })
        rows = pd.DataFrame({
            "datetime_minute": test["datetime_minute"].reset_index(drop=True),
            "host": test["host"].reset_index(drop=True),
            "model": res["model"],
            "score": score,
            "y_pred": pred.astype(int),
            "y_true": y_true_test,
            "anomaly": anomaly[len(train): len(train) + len(test)],
        })
        pred_rows.append(rows)

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

    save_models(scaler, results, cols, args.seq_len)


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
                first = _utc_epoch(detected["datetime_minute"].min())
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


def save_models(scaler, results, feature_cols, seq_len):
    import joblib
    import torch

    # Versiyonlu dizin: data/models/runs/<run_id>/
    runs_root = os.path.join(args.out, "models", "runs")
    if getattr(args, "run_id", None):
        run_id = args.run_id
    else:
        run_id = "run_" + time.strftime("%Y%m%d_%H%M%S")
    out = os.path.join(runs_root, run_id)
    os.makedirs(out, exist_ok=True)
    joblib.dump(scaler, os.path.join(out, "scaler.joblib"))
    for res in results:
        name = res["model"].lower().replace(" ", "_")
        with open(os.path.join(out, f"{name}_thr.txt"), "w") as f:
            f.write(str(res["thr"]))
        obj = res.get("model_obj")
        if obj is None:
            continue
        if name == "lstm-autoencoder":
            torch.save(obj.state_dict(), os.path.join(out, f"{name}.pt"))
        else:
            joblib.dump(obj, os.path.join(out, f"{name}.joblib"))

    spec = {
        "feature_cols": list(feature_cols),
        "seq_len": int(seq_len),
        "rolling_windows": list(ROLLING_WINDOWS),
        "metric_prefixes": list(METRIC_PREFIXES),
        "thresholds": {
            res["model"].lower().replace(" ", "_"): round(float(res["thr"]), 8)
            for res in results
        },
    }
    spec_path = os.path.join(out, "feature_spec.json")
    with open(spec_path, "w", encoding="utf-8") as f:
        json.dump(spec, f, indent=2)
    print(f"[train] model spec kaydedildi: {spec_path}")
    print(f"[train] modeller kaydedildi: {out}/")

    # Denetim/versiyon icin eval ciktilarini run dizinine kopyala
    import shutil
    for fname in ("eval_results.csv", "predictions.csv", "comp_vs_zabbix.csv"):
        src = os.path.join(args.out, fname)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(out, fname))
            print(f"[train] {fname} -> {out}/")

    # --promote: bu run'i aktif model yap (current.json)
    if getattr(args, "promote", False):
        write_current(args.out, run_id)
        print(f"[train] AKTIF model: {run_id}")


def write_current(out, run_id):
    """data/models/current.json'u gunceller (aktif run'i isaretler)."""
    models_dir = os.path.join(out, "models")
    os.makedirs(models_dir, exist_ok=True)
    current = {
        "run_id": run_id,
        "active_at": pd.Timestamp.utcnow().isoformat(),
    }
    with open(os.path.join(models_dir, "current.json"), "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2)
    return run_id


if __name__ == "__main__":
    main()
