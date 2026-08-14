#!/usr/bin/env python3
"""
GUVENLI bellek (RAM) testi - otomatik RAM algilama + yuzdelik hedef.

Host RAM'ini /proc/meminfo'dan okur, RAM_PERCENT kadarini kademeli olarak
isgal eder, tepe noktada bekler ve serbest birakir. OOM riskine karsi
guvenlik kapagi: kullanilabilir bellegin %90'ini asla gecmez.

Kullanim:
  python3 memory_leak_test.py                      # RAM_PERCENT=25 varsayilan
  RAM_PERCENT=25 python3 memory_leak_test.py       # hedef: toplam RAM'in %25'i
  RAM_HOLD_MIN=5 python3 memory_leak_test.py       # tepe noktada bekleme (dk)

Ornek (46GiB host, RAM_PERCENT=25):
  -> hedef ~11.5 GiB, ~120 adim x ~96MB, ~10dk dolu + 5dk tepe = ~15dk pencere.
"""

import json
import os
import re
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GT_FILE = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "data", "ground_truth.jsonl"))
GT_HOST = os.getenv("GT_HOST", "Zabbix server")

RAM_PERCENT = float(os.getenv("RAM_PERCENT", "25"))
HOLD_MIN = int(os.getenv("RAM_HOLD_MIN", "5"))
STEP_INTERVAL_S = int(os.getenv("RAM_STEP_INTERVAL_S", "5"))
TARGET_STEPS = int(os.getenv("RAM_TARGET_STEPS", "120"))
MAX_STEP_BYTES = int(os.getenv("RAM_MAX_STEP_MB", "200")) * 1024 * 1024


def read_meminfo():
    info = {}
    with open("/proc/meminfo", encoding="utf-8") as f:
        for line in f:
            key, rest = line.split(":", 1)
            try:
                kb = int(rest.split()[0])
            except (ValueError, IndexError):
                continue
            info[key] = kb * 1024
    return info


def compute_target():
    mem = read_meminfo()
    total = mem.get("MemTotal", 0)
    available = mem.get("MemAvailable", total)
    if total <= 0:
        print("[*] /proc/meminfo okunamadi, hedef 2GiB'ye dusuldu.")
        return int(2 * 1024**3), int(2 * 1024**3)
    target = int(total * RAM_PERCENT / 100.0)
    safety_cap = int(available * 0.9)
    if target > safety_cap:
        print(f"[*] Hedef {target / 1e9:.1f}GiB guvenlik kapagini ({safety_cap / 1e9:.1f}GiB) asti, sinirlaniyor.")
        target = safety_cap
    return target, total


def run_pressure(target):
    step = max(int(target // TARGET_STEPS), 1)
    step = min(step, MAX_STEP_BYTES)
    steps = max(target // step, 1)
    allocated = 0
    buf = []
    print(f"[*] Hedef: ~{target / 1e9:.1f}GiB ({RAM_PERCENT:g}%), "
          f"{steps} adim x ~{step / 1e6:.0f}MB, {STEP_INTERVAL_S}s arayla.")
    for i in range(steps):
        buf.append(bytearray(step))
        allocated += step
        if i % 10 == 0 or i == steps - 1:
            print(f"    -> Adim {i + 1}/{steps}: ~{allocated / 1e9:.1f}GiB isgal")
        time.sleep(STEP_INTERVAL_S)
    print(f"[*] Tepe noktasi (~{allocated / 1e9:.1f}GiB). Zabbix'in trendi yakalamasi icin {HOLD_MIN} dk bekleniyor...")
    time.sleep(HOLD_MIN * 60)
    return allocated


def log_ground_truth(start_ts, end_ts, allocated):
    record = {
        "anomaly_id": f"ram-{start_ts}",
        "type": "ram",
        "host": GT_HOST,
        "label": "memory_pressure",
        "start_ts": start_ts,
        "end_ts": end_ts,
        "metadata": {
            "percent": RAM_PERCENT,
            "target_gb": round(allocated / 1e9, 2),
        },
    }
    os.makedirs(os.path.dirname(GT_FILE), exist_ok=True)
    dup = re.compile(
        rf'"type": "ram".*"start_ts": {start_ts}.*"end_ts": {end_ts}'
    )
    if os.path.exists(GT_FILE):
        with open(GT_FILE, encoding="utf-8") as f:
            if dup.search(f.read()):
                print("[ground-truth] atlandi (tekrar): ram pencere zaten var")
                return
    with open(GT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"[ground-truth] loglandi: {record['anomaly_id']} ram memory_pressure {start_ts} -> {end_ts}")


def main():
    print("[*] GUVENLI Bellek (RAM) testi basliyor...")
    target, _ = compute_target()
    start_ts = int(time.time())
    allocated = 0
    try:
        allocated = run_pressure(target)
    except KeyboardInterrupt:
        print("\n[-] Test manuel durduruldu. Ground-truth'a yazilmiyor (iptal edildi).")
        sys.exit(0)
    finally:
        end_ts = int(time.time())
        print("[*] RAM serbest birakildi, sistem normale dondu.")
    log_ground_truth(start_ts, end_ts, allocated)


if __name__ == "__main__":
    main()
