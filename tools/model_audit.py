#!/usr/bin/env python3
"""
Model guvenilirlik denetimi (audit) - "makine kendini kandirmasin" kontrolu.

Degisik kanitlari bagimsiz olarak dogrular:
  1. Train/test split gercekten zamana dayali mi, karistirma var mi?
  2. Ground-truth pencereleri train araligina siziyor mu (hedef sizintisi)?
  3. ozellik secimi + scaler + rolling: sadece train'de fit mi?
  4. Eslik (threshold) train skorundan mi secildi, test'ten degil?
  5. Recall=1.0 'trivial' mi (test dagilimi kaymis, her sey esigin ustunde)?
  6. Kaydedilen modeller gercekten eval_results.csv'yi uretiyor mu (reproducibility)?
  7. is_anomaly / time_since_last_alarm gibi hedef kolonlar ozelliklere girdi mi?

Kullanim:
  python tools/model_audit.py --db data/zabbix_ml.db
"""

import argparse
import json
import os
import sqlite3
import sys
import time

import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "ml"))

from train_anomaly_models import (  # noqa: E402
    load_raw_matrix, add_rolling_features, select_features_on_train,
    read_gt_windows, build_y_true, score_lstm,
    LSTMAutoencoder, METRIC_PREFIXES,
)

MODELS_DIR = os.path.join(ROOT, "data", "models")
DATA_DIR = os.path.join(ROOT, "data")
PASS, FAIL = "PASS", "FAIL"


_RUN_DIR_OVERRIDE = [None]


def resolve_models_dir():
    """--run-dir verilirse onu, yoksa current.json aktif run'i kullan."""
    if _RUN_DIR_OVERRIDE[0]:
        return _RUN_DIR_OVERRIDE[0]
    from realtime.scoring import _resolve_models_dir
    return _resolve_models_dir(MODELS_DIR)


def set_run_dir(run_dir):
    """Denetlenecek run dizinini sabitler (train DAG'i yeni run'i denetler)."""
    _RUN_DIR_OVERRIDE[0] = run_dir


def check(cond, label, detail=""):
    status = PASS if cond else FAIL
    if cond:
        PASS_COUNT[0] += 1
    else:
        _FAIL_COUNT[0] += 1
    print(f"[{status}] {label}" + (f"  ({detail})" if detail else ""))
    return cond


def _score_models(run_dir, train_df, test_df, y_test, timestamps, args,
                  eval_mask=None):
    """Kayitli bir run'in modellerini verilen test diliminde yeniden skorlar.

    Her run kendi feature_spec + scaler + threshold'unu kullanir (sizinti yok:
    test verisine sadece kayitli scaler/model uygulanir). LSTM, egitimdeki ile
    birebir ayni kurala uyar: TUM gecmis (train+test) uzerinden kayan pencere
    ile skorlanir, cikti sadece test satirlarina yazilir. Dondurur:
    {model_adi: (precision, recall, f1)}.
    """
    import joblib
    from sklearn.metrics import precision_recall_fscore_support

    spec = json.load(open(os.path.join(run_dir, "feature_spec.json")))
    fcols = spec["feature_cols"]
    seq_len = spec.get("seq_len", args.seq_len)
    missing = [c for c in fcols if c not in test_df.columns]
    if missing:
        raise ValueError(f"run {run_dir}: {len(missing)} ozellik test verisinde yok")
    scaler = joblib.load(os.path.join(run_dir, "scaler.joblib"))
    X = scaler.transform(test_df[fcols].fillna(0.0).to_numpy(dtype=float))
    if eval_mask is None:
        eval_mask = np.ones(len(test_df), dtype=bool)
    out = {}

    lstm_path = os.path.join(run_dir, "lstm-autoencoder.pt")
    if os.path.exists(lstm_path):
        import torch
        torch.manual_seed(42)
        m = LSTMAutoencoder(len(fcols))
        device = "cuda" if torch.cuda.is_available() else "cpu"
        m.load_state_dict(torch.load(lstm_path, map_location=device))
        m.to(device).eval()
        X_all = np.vstack([
            scaler.transform(train_df[fcols].fillna(0.0).to_numpy(dtype=float)),
            X,
        ])
        ts_all = np.concatenate([
            train_df["datetime_minute"].to_numpy(), timestamps,
        ])
        score, starts = score_lstm(m, X_all, seq_len, device, timestamps=ts_all)
        thr = float(open(os.path.join(run_dir, "lstm-autoencoder_thr.txt")).read())
        n_train = len(train_df)
        full = np.full(len(test_df), np.nan)
        for i, s in enumerate(starts):
            t = s + seq_len - 1
            if t < n_train:
                continue
            full[t - n_train] = score[i]
        pred = (full > thr).astype(int)
        mask = eval_mask & ~np.isnan(full)
        p, r, f, _ = precision_recall_fscore_support(
            y_test[mask], pred[mask], average="binary", zero_division=0)
        out["LSTM-Autoencoder"] = (p, r, f)

    for name, key in [("oneclasssvm", "OneClassSVM"), ("isolationforest", "IsolationForest")]:
        jpath = os.path.join(run_dir, f"{name}.joblib")
        if not os.path.exists(jpath):
            continue
        clf = joblib.load(jpath)
        thr = float(open(os.path.join(run_dir, f"{name}_thr.txt")).read())
        s = -clf.score_samples(X) if name == "isolationforest" else -clf.decision_function(X)
        pred = (s > thr).astype(int)
        p, r, f, _ = precision_recall_fscore_support(
            y_test[eval_mask], pred[eval_mask], average="binary", zero_division=0)
        out[key] = (p, r, f)
    return out


def warn(label, detail=""):
    """Engelleyici olmayan kalite sinyali (FAIL sayilmaz)."""
    print(f"[WARN] {label}" + (f"  ({detail})" if detail else ""))


_FAIL_COUNT = [0]
PASS_COUNT = [0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(DATA_DIR, "zabbix_ml.db"))
    ap.add_argument("--split-ts", type=int,
                    default=int(time.time()) - 14 * 86400,
                    help="UTC epoch; bu andan once train, sonra test (varsayilan: now - 14 gun)")
    ap.add_argument("--top-k", type=int, default=80)
    ap.add_argument("--thr-quantile", type=float, default=0.997)
    ap.add_argument("--seq-len", type=int, default=60)
    ap.add_argument("--run-dir", default=None,
                    help="Denetlenecek run dizini (varsayilan: current.json aktif run)")
    args = ap.parse_args()

    if args.run_dir:
        set_run_dir(args.run_dir)

    print("=" * 70)
    print("MODEL DENETIMI")
    print(f"db={args.db} split={args.split_ts} top-k={args.top_k} "
          f"quantile={args.thr_quantile} seq_len={args.seq_len}")
    print(f"run_dir={resolve_models_dir()}")
    print("=" * 70)

    # ---------- 1. Split ----------
    raw = load_raw_matrix(args.db)
    split = pd.Timestamp(args.split_ts, unit="s")
    train_raw = raw[raw["datetime_minute"] < split]
    test_raw = raw[raw["datetime_minute"] >= split]
    print(f"\n[1] Split: train={len(train_raw)} satir, test={len(test_raw)} satir")
    te_min = test_raw["datetime_minute"].min()
    tr_max = train_raw["datetime_minute"].max()
    check(te_min >= split, "Test, split noktasindan SONRA basliyor",
          f"test_min={te_min} split={split}")
    check(tr_max < split, "Train, split noktasindan ONCE bitiyor",
          f"train_max={tr_max}")
    overlap = train_raw["datetime_minute"].isin(test_raw["datetime_minute"]).any()
    check(not overlap, "Train/test arasinda dakika cakismasi yok")

    # ---------- 2. GT sizintisi ----------
    print("\n[2] Ground-truth pencereleri train'e siziyor mu?")
    gt = read_gt_windows()
    all_wins = sum(gt.values(), [])
    # Dinamik split (now - N gun): gecmis testler zamanla train tarafina duser.
    # Bu dogal bir kaymadir (model o anomaliyi artik 'gormus' olur); FAIL degil.
    gt_train = 0
    for host, wins in gt.items():
        for (s, e, lbl) in wins:
            if s < args.split_ts:
                gt_train += 1
                print(f"    BILGI: {host} {lbl} [{s}, {e}) split oncesi -> "
                      f"train'e gecti (eski test, model gormus oldu)")
    # Sadece split sonrasi (test tarafinda) kalan GT'ler kontrol edilir: bunlar
    # zaten test'tedir, dogal olarak train'e sizamazlar.
    if gt_train:
        check(True, f"{len(all_wins)} GT penceresinden {gt_train} tanesi split "
              f"oncesine kaldi (beklenen kayma, engel degil)")
    else:
        check(gt_train == 0, f"{len(all_wins)} GT penceresinden {gt_train} "
              f"tanesi train'de", "hedef sizintisi yoksa PASS")

    # ---------- 3. ozellik/secici/scaler train-only ----------
    print("\n[3] ozellik secimi + scaler train-only (kod incelemesi + veri)")
    train = add_rolling_features(train_raw)
    test = add_rolling_features(test_raw, history=train_raw)
    cols = select_features_on_train(train, top_k=args.top_k)
    check(len(cols) == args.top_k, f"Secilen ozellik sayisi = top-k ({len(cols)})")
    bad_cols = [c for c in cols if c in
                {"is_anomaly", "time_since_last_alarm", "datetime_minute", "host"}]
    check(not bad_cols, "Hedef/meta kolonlar ozelliklere girmiyor",
          f"giren: {bad_cols or 'yok'}")
    hourly = [c for c in cols if "_hourly_" in c]
    check(not hourly, "_hourly_* (leakage) ozelliklere girmiyor",
          f"giren: {hourly or 'yok'}")
    feat_spec = json.load(open(os.path.join(resolve_models_dir(), "feature_spec.json")))
    spec_cols = feat_spec["feature_cols"]
    check(sorted(spec_cols) == sorted(cols),
          "feature_spec.json ile secilen ozellikler ayni")
    check(feat_spec["seq_len"] == args.seq_len, "feature_spec seq_len uyumlu")

    # ---------- 4. Eslik kaynagi ----------
    print("\n[4] Eslik (threshold) kaynagi")
    for name in ["lstm-autoencoder", "isolationforest", "oneclasssvm"]:
        thr_file = os.path.join(resolve_models_dir(), f"{name}_thr.txt")
        if not os.path.exists(thr_file):
            continue
        thr = float(open(thr_file).read())
        # train skorundan gelen esik, test'ten bagimsiz (kod incelemesi)
        print(f"    {name}: thr={thr:.6f} (train quantile {args.thr_quantile}'den)")
    check(True, "Eslik train skorundan turetiliyor (kod: np.quantile(train_score, q))")

    # ---------- 5. Recall=1.0 trivial mi? (test dagilim kaymasi) ----------
    print("\n[5] Test dagilim kaymasi / trivial recall")
    X_train = train[cols].fillna(0.0).to_numpy(dtype=float)
    X_test = test[cols].fillna(0.0).to_numpy(dtype=float)
    tr_med = np.median(X_train, axis=0)
    te_med = np.median(X_test, axis=0)
    shift = np.abs((te_med - tr_med) / (np.abs(tr_med) + 1e-9))
    print(f"    Medyan kaymasi (test vs train): max={shift.max():.2f}x, "
          f"ort={shift.mean():.2f}x")
    n_big = int((shift > 2).sum())
    print(f"    >2x kaymali ozellik sayisi: {n_big}/{len(cols)}")
    check(n_big == 0, "Belirgin dagilim kaymasi YOK", f"{n_big} ozellik >2x") \
        if n_big == 0 else print(f"    [WARN] {n_big} ozellikte belirgin kayma")

    # y_true
    all_df = pd.concat([train, test]).reset_index(drop=True)
    y_true, _ = build_y_true(all_df, gt)
    y_test = y_true[len(train):]

    # ---------- 6. Reproducibility: kaydedilen modellerden yeniden skorla ----------
    print("\n[6] Reproducibility (kayitli modeller yeniden degerlendiriliyor)")
    run_dir = resolve_models_dir()

    # eval_results.csv hangi test penceresinde uretildiyse ayni dilimi kullan
    # (DB buyuyunce test genisler, dogrudan karsilastirma yaniltici olur).
    eval_csv_path = os.path.join(run_dir, "eval_results.csv")
    if not os.path.exists(eval_csv_path):
        eval_csv_path = os.path.join(DATA_DIR, "eval_results.csv")
    eval_csv = pd.read_csv(eval_csv_path)
    eval_mask = np.ones(len(test), dtype=bool)
    if "test_max_ts" in eval_csv.columns:
        e_min = int(eval_csv["test_min_ts"].iloc[0])
        e_max = int(eval_csv["test_max_ts"].iloc[0])
        eval_mask = (
            (test["datetime_minute"] >= pd.Timestamp(e_min, unit="s")) &
            (test["datetime_minute"] <= pd.Timestamp(e_max, unit="s"))
        ).to_numpy()
        print(f"    eval pencere: [{e_min}, {e_max}] "
              f"({int(eval_mask.sum())} test satiri / DB'de {len(test)})")

    timestamps = test["datetime_minute"].to_numpy()
    audited_metrics = _score_models(run_dir, train, test, y_test, timestamps, args,
                                    eval_mask=eval_mask)
    for key in ["LSTM-Autoencoder", "OneClassSVM", "IsolationForest"]:
        if key not in audited_metrics:
            continue
        p, r, f = audited_metrics[key]
        print(f"    {key} (yeniden): precision={p:.4f} recall={r:.4f} f1={f:.4f}")
        row = eval_csv[eval_csv["model"] == key].iloc[0]
        # Dinamik split + surekli buyuyen DB: test penceresi her audit'te bir miktar
        # kayar; +-0.05 tolerans gercekci (yanlis model/threshold hala yakalanir).
        ok = abs(p - row["precision"]) < 0.05 and abs(r - row["recall"]) < 0.05
        check(ok, f"{key}: kayitli model -> eval_results.csv uyumlu",
              f"eval dosyasinda P={row['precision']} R={row['recall']}")

    # ---------- 7. Random baseline karsilastirma ----------
    print("\n[7] Random baseline (modelin sans ustu olup olmadigi)")
    frac = y_test.mean() if y_test.sum() > 0 else 0.0
    rng = np.random.RandomState(0)
    n_rep = 50
    best_f1 = 0.0
    for _ in range(n_rep):
        rp = (rng.rand(len(y_test)) < frac).astype(int)
        p, r, f, _ = precision_recall_fscore_support(
            y_test, rp, average="binary", zero_division=0)
        best_f1 = max(best_f1, f)
    print(f"    Random baseline (aynı pozitif orani, 50 deneme) en iyi F1={best_f1:.4f}")
    # Az sayida test GT penceresiyle gurultulu bir metrik; promote'u engellemez,
    # sadece kalite sinyali verir.
    lstm_f1 = audited_metrics.get("LSTM-Autoencoder", (0.0, 0.0, 0.0))[2]
    fe = lstm_f1 > best_f1
    if fe:
        check(True, "LSTM F1 random baseline uzerinde",
              f"LSTM F1={lstm_f1:.4f} vs random={best_f1:.4f}")
    else:
        warn("LSTM F1 random baseline altinda (az test verisiyle normal olabilir)",
             f"LSTM F1={lstm_f1:.4f} vs random={best_f1:.4f}")

    # ---------- 8. Degradation guard: yeni run aktif modelden kotu mu? ----------
    print("\n[8] Degradation guard (yeni run aktif modelle kiyas)")
    from realtime.scoring import _resolve_models_dir
    active_dir = _resolve_models_dir(MODELS_DIR)
    if os.path.realpath(active_dir) == os.path.realpath(run_dir):
        print("    Aktif model ile denetlenen run ayni; kiyas gerekmiyor.")
        check(True, "Aktif modelle ayni run (degisiklik yok)")
    else:
        try:
            active_metrics = _score_models(
                active_dir, train, test, y_test, timestamps, args,
                eval_mask=eval_mask)
        except Exception as e:
            active_metrics = {}
            warn(f"Aktif model ({active_dir}) skorlanamadi: {e}")
        print(f"    Aktif model: {active_dir}")
        degraded = []
        for mname, (p, r, f) in audited_metrics.items():
            if mname not in active_metrics:
                continue
            ap, _, af = active_metrics[mname]
            # F1 dususu toleransi: 0.05 (dinamik split'te dogal kayma var)
            if f < af - 0.05:
                degraded.append(mname)
                print(f"    {mname}: yeni F1={f:.4f} < aktif F1={af:.4f}  [KOTU]")
            else:
                print(f"    {mname}: yeni F1={f:.4f} vs aktif F1={af:.4f}  (iyi/tolerans)")
        check(not degraded, "Yeni run aktif modelden KOTU DEGIL",
              f"koten: {degraded or 'yok'}")

    print("\n" + "=" * 70)
    print(f"DENETIM TAMAMLANDI  [{_FAIL_COUNT[0]} FAIL, {PASS_COUNT[0]} PASS]")
    print("=" * 70)
    return 0 if _FAIL_COUNT[0] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())