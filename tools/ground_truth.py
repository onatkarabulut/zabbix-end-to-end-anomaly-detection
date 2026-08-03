#!/usr/bin/env python3
"""
Sentetik anomali uret + ground-truth etiketleri logla.

Fikir: ML modelini dogrulamak icin "gercek" anomali anlarini bilmemiz gerekir.
Bu script, izlenen host uzerinde yapay yuk/ariza uretir (CPU burn, bellek,
disk doldurma, ag trafigi) ve calistigi zaman araligini data/ground_truth.jsonl
dosyasina yazar. Boylece "labels" komutu ile her dakika icin 0/1 hedef etiket
uretebiliriz.

Tum zamanlar UTC epoch (saniye) olarak tutulur. ml_feature_matrix'deki
datetime_minute de UTC oldugu icin uyumludur.

Kullanim ornekleri:
  python tools/ground_truth.py cpu    --duration 120 --label cpu_burn --cores 2
  python tools/ground_truth.py memory --duration 120 --size 2G
  python tools/ground_truth.py disk   --duration 120 --size 1G --path /tmp/zabbix_gt
  python tools/ground_truth.py net    --duration 60  --target 10.0.0.5 --rate 100M
  python tools/ground_truth.py manual --start 1785708000 --end 1785708300 --label restore_restart
  python tools/ground_truth.py list
  python tools/ground_truth.py labels --db data/zabbix_ml.db --out data/labels.csv
"""

import argparse
import calendar
import csv
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid

GT_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "ground_truth.jsonl")
DEFAULT_HOST = "Zabbix server"


def _now():
    return int(time.time())


def _log(record):
    os.makedirs(os.path.dirname(os.path.abspath(GT_FILE)), exist_ok=True)
    with open(GT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"[ground-truth] loglandi: {record['anomaly_id']} "
          f"{record['type']} {record['start_ts']} -> {record['end_ts']} ({record['label']})")


def _run(cmd, duration, record, sudo):
    print(f"[ground-truth] calistiriliyor: {' '.join(cmd)} ({duration}s)")
    start_ts = _now()
    proc = subprocess.Popen(cmd)
    try:
        proc.wait(timeout=duration + 10)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    record["start_ts"] = start_ts
    record["end_ts"] = _now()
    _log(record)


def _which(name, hint):
    path = shutil.which(name)
    if path is None:
        sys.exit(f"[ground-truth] '{name}' bulunamadi. {hint}")
    return path


def cmd_cpu(args):
    stress = _which("stress-ng", "apt-get install stress-ng (veya container icinde)")
    cores = args.cores or os.cpu_count() or 1
    cmd = [stress, "--cpu", str(cores), "--timeout", f"{args.duration}s"]
    if args.sudo:
        cmd = ["sudo", "-n"] + cmd
    _run(cmd, args.duration, {
        "anomaly_id": f"cpu-{uuid.uuid4().hex[:8]}",
        "type": "cpu", "host": args.host, "label": args.label,
        "metadata": {"cores": cores},
    }, args.sudo)


def cmd_memory(args):
    stress = _which("stress-ng", "apt-get install stress-ng")
    cmd = [stress, "--vm", "1", "--vm-bytes", args.size, "--vm-hang", "0",
           "--timeout", f"{args.duration}s"]
    if args.sudo:
        cmd = ["sudo", "-n"] + cmd
    _run(cmd, args.duration, {
        "anomaly_id": f"mem-{uuid.uuid4().hex[:8]}",
        "type": "memory", "host": args.host, "label": args.label,
        "metadata": {"size": args.size},
    }, args.sudo)


def cmd_disk(args):
    dd = _which("dd", "coreutils eksik")
    os.makedirs(args.path, exist_ok=True)
    target = os.path.join(args.path, f"fill_{uuid.uuid4().hex[:6]}.bin")
    cmd = [dd, "if=/dev/zero", f"of={target}", "bs=1M", f"count={args.size}",
           "oflag=direct"]
    if args.sudo:
        cmd = ["sudo", "-n"] + cmd
    record = {
        "anomaly_id": f"disk-{uuid.uuid4().hex[:8]}",
        "type": "disk", "host": args.host, "label": args.label,
        "metadata": {"file": target, "size_gb": args.size},
    }
    _run(cmd, args.duration, record, args.sudo)
    if os.path.exists(target):
        try:
            os.remove(target)
        except OSError as e:
            print(f"[ground-truth] temizlenemedi {target}: {e}")


def cmd_net(args):
    iperf = _which("iperf3", "apt-get install iperf3; karsidan 'iperf3 -s' calistirin")
    cmd = [iperf, "-c", args.target, "-t", str(args.duration)]
    if args.rate:
        cmd += ["-b", args.rate]
    if args.sudo:
        cmd = ["sudo", "-n"] + cmd
    _run(cmd, args.duration, {
        "anomaly_id": f"net-{uuid.uuid4().hex[:8]}",
        "type": "net", "host": args.host, "label": args.label,
        "metadata": {"target": args.target, "rate": args.rate},
    }, args.sudo)


def cmd_manual(args):
    _log({
        "anomaly_id": f"manual-{uuid.uuid4().hex[:8]}",
        "type": "manual", "host": args.host, "label": args.label,
        "start_ts": args.start, "end_ts": args.end,
        "metadata": {"note": args.note},
    })


def cmd_list(_):
    if not os.path.exists(GT_FILE):
        print("[ground-truth] kayit yok.")
        return
    with open(GT_FILE, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            print(f"  {rec['anomaly_id']}  {rec['type']:<7} {rec['label']:<24} "
                  f"{rec['start_ts']} -> {rec['end_ts']}  host={rec['host']}")


def cmd_labels(args):
    if not os.path.exists(GT_FILE):
        sys.exit("[ground-truth] ground_truth.jsonl yok. Once anomali uretin.")

    records = [json.loads(l) for l in open(GT_FILE, encoding="utf-8")]
    windows_by_host = {}
    for rec in records:
        windows_by_host.setdefault(rec["host"], []).append(
            (rec["start_ts"], rec["end_ts"], rec.get("label", rec["type"]))
        )

    conn = sqlite3.connect(args.db)
    rows = conn.execute(
        "SELECT datetime_minute, host FROM ml_features_enriched "
        "ORDER BY host, datetime_minute"
    ).fetchall()
    conn.close()

    out_rows = []
    for dt_str, host in rows:
        minute_ts = calendar.timegm(
            time.strptime(dt_str.split(".")[0], "%Y-%m-%d %H:%M:%S")
        )
        label = 0
        anomaly = ""
        for (start, end, lbl) in windows_by_host.get(host, []):
            if start <= minute_ts < end:
                label = 1
                anomaly = lbl
                break
        out_rows.append((dt_str, host, label, anomaly))

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["datetime_minute", "host", "label", "anomaly"])
        writer.writerows(out_rows)
    n_pos = sum(1 for r in out_rows if r[2] == 1)
    print(f"[ground-truth] {args.out} yazildi: {len(out_rows)} satir, "
          f"{n_pos} anomali etiketli ({len(records)} pencere).")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help=f"izlenen host (default: {DEFAULT_HOST})")
    parser.add_argument("--sudo", action="store_true", help="sudo -n ile calistir")
    sub = parser.add_subparsers(dest="action", required=True)

    p = sub.add_parser("cpu", help="CPU burn (stress-ng)")
    p.add_argument("--duration", type=int, default=120)
    p.add_argument("--cores", type=int, default=None)
    p.add_argument("--label", default="cpu_burn")
    p.set_defaults(func=cmd_cpu)

    p = sub.add_parser("memory", help="Bellek doldur (stress-ng)")
    p.add_argument("--duration", type=int, default=120)
    p.add_argument("--size", default="2G")
    p.add_argument("--label", default="memory_pressure")
    p.set_defaults(func=cmd_memory)

    p = sub.add_parser("disk", help="Disk doldur (dd)")
    p.add_argument("--duration", type=int, default=120)
    p.add_argument("--size", type=int, default=1, help="GB")
    p.add_argument("--path", default="/tmp/zabbix_gt")
    p.add_argument("--label", default="disk_fill")
    p.set_defaults(func=cmd_disk)

    p = sub.add_parser("net", help="Ag trafigi (iperf3)")
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--target", required=True)
    p.add_argument("--rate", default=None, help="orn. 100M")
    p.add_argument("--label", default="network_burst")
    p.set_defaults(func=cmd_net)

    p = sub.add_parser("manual", help="Elle pencere ekle")
    p.add_argument("--start", type=int, required=True, help="UTC epoch")
    p.add_argument("--end", type=int, required=True, help="UTC epoch")
    p.add_argument("--label", default="manual_anomaly")
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_manual)

    sub.add_parser("list", help="Logdaki kayitlari listele").set_defaults(func=cmd_list)

    p = sub.add_parser("labels", help="ML icin 0/1 etiket CSV uret")
    p.add_argument("--db", default="data/zabbix_ml.db")
    p.add_argument("--out", default="data/labels.csv")
    p.set_defaults(func=cmd_labels)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
