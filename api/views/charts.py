"""Rapor grafikleri - matplotlib ile PNG uretimi.

Sunucu tarafinda render edilir; endpoint'ler image/png dondurur. Boylece
rapor icin gereken grafikler tek URL ile alinabilir (tarayici/markdown).
"""

import io
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # basliksiz ortam (sunucu) icin
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

_COLORS = {"LSTM-Autoencoder": "#d62728", "IsolationForest": "#1f77b4",
           "OneClassSVM": "#2ca02c", "lstm-autoencoder": "#d62728",
           "isolationforest": "#1f77b4", "oneclasssvm": "#2ca02c"}


def _fig_to_png(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def metrics_comparison(eval_rows: list) -> bytes:
    """3 model icin precision/recall/F1 bar grafigi (eval_results.csv)."""
    models = [r["model"] for r in eval_rows]
    metrics = ["precision", "recall", "f1"]
    fig, ax = plt.subplots(figsize=(9, 5))
    width = 0.25
    xs = range(len(models))
    for i, m in enumerate(metrics):
        vals = [float(r.get(m) or 0) for r in eval_rows]
        bars = ax.bar([x + i * width for x in xs], vals, width, label=m)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + 0.01, f"{v:.3f}",
                    ha="center", fontsize=8)
    ax.set_xticks([x + width for x in xs])
    ax.set_xticklabels(models)
    ax.set_ylim(0, 1.1)
    ax.set_title("Model Karsilastirmasi - Precision / Recall / F1")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    return _fig_to_png(fig)


def latency_chart(comp_rows: list) -> bytes:
    """Ground truth pencerelerinde yakalama + gecikme (comp_vs_zabbix.csv)."""
    # model -> [(pencere_etiketi, latency ya da None)]
    by_model = {}
    for r in comp_rows:
        label = f"{r.get('label','?')}\n{r.get('window_start','')}"
        lat = r.get("latency_min")
        lat = float(lat) if lat not in (None, "", "nan") else None
        caught = str(r.get("caught")).lower() == "true"
        by_model.setdefault(r["model"], []).append((label, lat, caught))

    fig, ax = plt.subplots(figsize=(10, 5))
    n_models = len(by_model)
    for i, (model, rows) in enumerate(sorted(by_model.items())):
        xs, ys = [], []
        for j, (label, lat, caught) in enumerate(rows):
            if caught and lat is not None:
                xs.append(j + i * 0.2 - 0.2)
                ys.append(lat)
        ax.scatter(xs, ys, s=80, label=model,
                   color=_COLORS.get(model), zorder=3)
    labels = [r[0] for r in next(iter(by_model.values()), [])]
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=7)
    ax.set_ylabel("Tespit gecikmesi (dk)")
    ax.set_title("Ground Truth Pencereleri - Tespit Gecikmesi "
                 "(isaretsiz = yakalanamadi)")
    ax.legend()
    ax.grid(alpha=0.3)
    return _fig_to_png(fig)


def score_timeseries(series: list, model: str) -> bytes:
    """Realtime alert skor zaman serisi + esik cizgisi."""
    fig, ax = plt.subplots(figsize=(11, 4.5))
    if series:
        ts = [datetime.fromisoformat(str(r["datetime_minute"]))
              for r in series]
        scores = [float(r["score"]) for r in series]
        thr = [float(r["threshold"]) for r in series]
        ax.plot(ts, scores, lw=1.2, label="skor",
                color=_COLORS.get(model, "#333"))
        ax.plot(ts, thr, lw=1, ls="--", color="gray", label="esik")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        fig.autofmt_xdate()
    else:
        ax.text(0.5, 0.5, "veri yok", ha="center", va="center",
                transform=ax.transAxes)
    ax.set_title(f"Realtime Anomali Skoru - {model}")
    ax.legend()
    ax.grid(alpha=0.3)
    return _fig_to_png(fig)


def predictions_timeline(pred_rows: list, gt_windows: list,
                         model: str) -> bytes:
    """Egitim degerlendirme skorlari + ground truth pencere overlay'i.

    pred_rows: predictions.csv satirlari (tek model filtreli)
    gt_windows: [(start_ts, end_ts, label)] epoch saniye
    """
    fig, ax = plt.subplots(figsize=(12, 4.5))
    if pred_rows:
        ts = [datetime.fromisoformat(str(r["datetime_minute"]))
              for r in pred_rows]
        scores = [float(r["score"]) for r in pred_rows]
        ax.plot(ts, scores, lw=0.8, color=_COLORS.get(model, "#333"),
                label="skor")
        # anomali tahminleri
        anom = [(t, s) for t, s, r in zip(ts, scores, pred_rows)
                if str(r.get("y_pred")) == "1"]
        if anom:
            ax.scatter([a[0] for a in anom], [a[1] for a in anom],
                       s=12, color="red", label="tahmin: anomali", zorder=3)
        # ground truth pencereleri
        for start, end, label in gt_windows:
            ax.axvspan(datetime.fromtimestamp(start),
                       datetime.fromtimestamp(end),
                       alpha=0.2, color="orange")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        fig.autofmt_xdate()
    else:
        ax.text(0.5, 0.5, "veri yok", ha="center", va="center",
                transform=ax.transAxes)
    ax.set_title(f"Test Donemi Skorlari + Ground Truth Pencereleri - {model}"
                 " (turuncu = gercek anomali)")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    return _fig_to_png(fig)


def alerts_per_hour(rows: list) -> bytes:
    """Saatlik alert yogunlugu (model bazli cizgi)."""
    from collections import defaultdict
    by_model = defaultdict(lambda: defaultdict(int))
    for r in rows:
        hour = str(r["datetime_minute"])[:13]  # YYYY-MM-DD HH
        by_model[r["model"]][hour] += 1
    fig, ax = plt.subplots(figsize=(11, 4.5))
    for model, counts in sorted(by_model.items()):
        hours = sorted(counts)
        ts = [datetime.fromisoformat(h + ":00:00") for h in hours]
        ax.plot(ts, [counts[h] for h in hours], marker="o", ms=3,
                lw=1, label=model, color=_COLORS.get(model))
    if by_model:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H"))
        fig.autofmt_xdate()
    else:
        ax.text(0.5, 0.5, "veri yok", ha="center", va="center",
                transform=ax.transAxes)
    ax.set_ylabel("alert / saat")
    ax.set_title("Saatlik Alert Yogunlugu")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    return _fig_to_png(fig)
