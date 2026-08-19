"""Skorlama - kayitli model ile dakika bazli anomali skoru.

Kaynak: Faz 3'te egitilen modeller (data/models/):
  - feature_spec.json   : secilen ozellikler + esikler + seq_len
  - scaler.joblib       : train ortalamasi/standart sapmasi
  - isolationforest.joblib / oneclasssvm.joblib / lstm-autoencoder.pt

Consumer her host icin son dakikalarin ham (key -> deger) serisini tutar.
Bu modul, seriden ETL/feature_engineering ile AYNI sekilde rolling
avg/std/trend ozellik vektorlerini uretir ve skorlar:
  - avg  : window dakikalik ortalama (min_periods=1)
  - std  : window dakikalik std (min_periods=2, yoksa 0)
  - trend: (son - ilk) / adet (min_periods=2)
"""

import json
import os
import re

import numpy as np
import pandas as pd

MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "models")


def _resolve_models_dir(models_dir: str) -> str:
    """current.json varsa aktif run dizinini, yoksa verilen dizini doner.

    Versiyonlama: data/models/runs/<run_id>/ altinda tutulur; current.json
    hangi run'un aktif oldugunu isaretler. Eski kurulumlar (dosyalar dogrudan
    data/models/ icinde) icin geriye uyumlu: current.json yoksa ayni dizin.
    """
    current_file = os.path.join(models_dir, "current.json")
    if os.path.exists(current_file):
        try:
            with open(current_file, encoding="utf-8") as f:
                import json as _json
                run_id = _json.load(f).get("run_id")
            run_dir = os.path.join(models_dir, "runs", run_id)
            if os.path.isdir(run_dir):
                return run_dir
        except Exception:
            pass
    return models_dir

# feature_spec kolon adindan (key, window, istatistik) cozumleme:
#   system.cpu.util_avg_5m   -> key='system.cpu.util', window=5, stat='avg'
#   hour_of_day/day_of_week/is_weekend -> ozel zaman ozellikleri (realtime
#   epoch'tan turetir, egitimle birebir ayni: yoksa 0 kaliyordu -> tutarsizlik)
_FEAT_RE = re.compile(r"^(.*)_(avg|std|trend)_(\d+)m$")

_TIME_FEATURES = {
    "hour_of_day": lambda dt: float(dt.hour),
    "day_of_week": lambda dt: float(dt.dayofweek),
    "is_weekend": lambda dt: float(dt.dayofweek >= 5),
}

try:
    import torch
    TORCH_OK = True
except Exception:
    TORCH_OK = False


def _parse_feature_col(col: str):
    if col in _TIME_FEATURES:
        return {"time_feature": col}
    m = _FEAT_RE.match(col)
    if not m:
        return None
    return {"key": m.group(1), "stat": m.group(2), "window": int(m.group(3))}


def _load_lstm(models_dir: str):
    """Egitilmis LSTM-Autoencoder'i yukler. Model ml/train_anomaly_models
    icinden import edilir (torch yoksa None doner)."""
    if not TORCH_OK:
        return None, None
    import importlib
    import sys

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ml"))
    sys.path.insert(0, root)
    tmod = importlib.import_module("train_anomaly_models")
    spec_path = os.path.join(models_dir, "feature_spec.json")
    with open(spec_path, encoding="utf-8") as f:
        spec = json.load(f)
    seq_len = spec["seq_len"]
    n_feat = len(spec["feature_cols"])
    m = tmod.LSTMAutoencoder(n_feat)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    state = torch.load(os.path.join(models_dir, "lstm-autoencoder.pt"),
                       map_location=device)
    m.load_state_dict(state)
    m.to(device).eval()
    return m, device, seq_len


class RealtimeScorer:
    def __init__(self, models_dir: str = MODELS_DIR):
        self.models_dir = _resolve_models_dir(models_dir)
        self.feature_cols = []
        self._col_parsed = []
        self.thresholds = {}
        self.scaler = None
        self.models = {}
        self.lstm = None
        self.lstm_device = None
        self.seq_len = 60
        self._load()

    def _load(self):
        spec_path = os.path.join(self.models_dir, "feature_spec.json")
        with open(spec_path, encoding="utf-8") as f:
            spec = json.load(f)
        self.feature_cols = spec["feature_cols"]
        self._col_parsed = [_parse_feature_col(c) for c in self.feature_cols]
        self.thresholds = spec.get("thresholds", {})
        self.seq_len = spec.get("seq_len", 60)

        import joblib

        self.scaler = joblib.load(os.path.join(self.models_dir, "scaler.joblib"))
        # Eksik feature'lar 0 ile degil scaler ortalamasi (mu) ile doldurulur.
        # 0 degeri mu>0 olan feature'lar icin (0-mu)/sigma ile |z| onlarca
        # olan asiri negatif skorlar uretir ve LSTM rekonstruksiyon hatasi
        # patlar (ornek: vfs.fs.dependent.size z(0)=-34.8). Bir metrik kisa
        # sure eksik kaldiginda bile FP alarm olusturmak yerine nötr kalir.
        self._fill_value = self.scaler.mean_.copy()
        for name in ("isolationforest", "oneclasssvm"):
            path = os.path.join(self.models_dir, f"{name}.joblib")
            if os.path.exists(path):
                self.models[name] = joblib.load(path)

        lstm_path = os.path.join(self.models_dir, "lstm-autoencoder.pt")
        if TORCH_OK and os.path.exists(lstm_path):
            loaded = _load_lstm(self.models_dir)
            if loaded is not None:
                self.lstm, self.lstm_device, self.seq_len = loaded

    # ------------------------------------------------------------------
    # Feature uretimi (ETL/feature_engineering ile ayni kural)
    # ------------------------------------------------------------------
    def _window_values(self, hist, key, current_epoch, window):
        """hist: [(epoch, {key: value})] artan sirali. current_epoch dahil son
        `window` dakikadaki key degerleri (Egitim'deki rolling(w) ile ayni:
        '5m ort' = son 5 gercek dakika, bugun dahil)."""
        lo = current_epoch - (window - 1) * 60
        vals = []
        for epoch, kv in hist:
            if epoch > current_epoch:
                break
            if epoch >= lo and key in kv and kv[key] is not None:
                vals.append(float(kv[key]))
        return vals

    def _feature_vector(self, hist, current_epoch) -> np.ndarray:
        vec = self._fill_value.copy()
        dt = pd.Timestamp(current_epoch, unit="s")
        for i, spec in enumerate(self._col_parsed):
            if spec is None:
                continue
            if "time_feature" in spec:
                vec[i] = _TIME_FEATURES[spec["time_feature"]](dt)
                continue
            vals = self._window_values(hist, spec["key"], current_epoch,
                                       spec["window"])
            if not vals:
                continue
            if spec["stat"] == "avg":
                vec[i] = float(np.mean(vals))
            elif spec["stat"] == "std":
                vec[i] = float(np.std(vals)) if len(vals) >= 2 else 0.0
            elif spec["stat"] == "trend":
                vec[i] = float((vals[-1] - vals[0]) / len(vals))
        return vec

    def _feature_matrix(self, hist) -> np.ndarray:
        """hist'ten son seq_len dakikanin ozellik matrisi (seq_len, n_feat).
        Eksik dakikalar (seride hic kayit olmayan) mu vektoru ile doldurulur
        (0 ile degil - aksi halde scaler asiri negatif z-skor uretir)."""
        n = len(self.feature_cols)
        if not hist:
            return np.tile(self._fill_value, (self.seq_len, 1))
        epochs = sorted({e for e, _ in hist})
        # en guncel seq_len dakika (artik olmayanlar onceye sayilir)
        start = epochs[-1] - (self.seq_len - 1) * 60
        grid = [start + i * 60 for i in range(self.seq_len)]
        rows = [self._feature_vector(hist, e) for e in grid]
        return np.vstack(rows)

    # ------------------------------------------------------------------
    # Skorlama
    # ------------------------------------------------------------------
    def _row_score(self, row: np.ndarray) -> dict:
        scaled = self.scaler.transform(row.reshape(1, -1))
        out = {}
        for name, model in self.models.items():
            if name == "isolationforest":
                s = float(-model.score_samples(scaled)[0])
            else:
                s = float(-model.decision_function(scaled)[0])
            thr = self.thresholds.get(name, np.inf)
            out[name] = {"score": s, "threshold": thr, "anomaly": int(s > thr)}
        return out

    def score_lstm(self, hist) -> dict:
        """Noktasal LSTM skoru: son seq_len dakikanin matrisinin yalnizca son
        zaman adimindaki (puanlanan dakikadaki) rekonstruksiyon hatasi.
        Egitimdeki score_lstm ile ayni kural - pencere ortalamasi kisa
        anomalileri ezer, son adim odagi bunu onler."""
        if self.lstm is None:
            return {"score": float("nan"), "threshold": self.thresholds.get(
                "lstm-autoencoder", float("nan")), "anomaly": 0}
        mat = self._feature_matrix(hist)
        scaled = self.scaler.transform(mat)
        seq = torch.from_numpy(scaled).float().unsqueeze(0).to(self.lstm_device)
        with torch.no_grad():
            rec = self.lstm(seq)
        score = float(np.mean((seq.cpu().numpy()[:, -1, :]
                               - rec.cpu().numpy()[:, -1, :]) ** 2))
        thr = self.thresholds.get("lstm-autoencoder", float("nan"))
        return {"score": score, "threshold": thr, "anomaly": int(score > thr)}

    def score(self, hist) -> dict:
        """hist ([(epoch, {key: value})]) icin durumsuz modellerin skoru."""
        if not hist:
            row = np.zeros(len(self.feature_cols))
        else:
            row = self._feature_vector(hist, hist[-1][0])
        return self._row_score(row)

    def vote(self, hist) -> dict:
        """Kayan pencere + durumsuz modellerin ortak karari.

        - LSTM: son seq_len dakikalik pencere rekonstruksiyon hatasi (ana model)
        - IF/OCSVM: en guncel dakika vektoru (yardimci)
        Alert: herhangi bir model esigi asarsa uretilir.
        """
        results = self.score(hist)
        lstm_res = self.score_lstm(hist)
        if not np.isnan(lstm_res["score"]):
            results["lstm-autoencoder"] = lstm_res
        anomalies = [n for n, r in results.items() if r["anomaly"]]
        return {
            "results": results,
            "n_models": len(results),
            "n_anomaly_models": len(anomalies),
            "alert": len(anomalies) > 0,
            "max_score": max((r["score"] for r in results.values()), default=0.0),
        }