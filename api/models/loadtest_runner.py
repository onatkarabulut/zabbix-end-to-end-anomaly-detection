"""Load test calistirici - test scriptlerini subprocess ile yonetir.

NOT: Testler HOST kaynaklarini yormalidir (Zabbix host metriklerini
etkilemek icin). API bir container icinde calisiyorsa uretilen yuk
container ile sinirli kalir; gercek host testi icin API'yi host'ta
calistirin veya container'i uygun kaynak limitleriyle baslatin.
"""

import json
import os
import signal
import subprocess
import sys
import time

from .. import config

# Desteklenen test turleri -> (komut fabrikasi, aciklama)
_STATE_FILE = os.path.join(os.path.dirname(config.GROUND_TRUTH_FILE),
                           "api_loadtest_state.json")


def available_tests() -> list:
    return [
        {"type": "cpu", "script": "cpu_spike_test.sh",
         "params": {"CPU_WORKERS": "yuklenecek cekirdek (vars: tum)",
                    "CPU_DURATION_S": "sure sn (vars: 300)"},
         "description": "stress-ng ile CPU yuku"},
        {"type": "ram", "script": "memory_leak_test.py",
         "params": {"RAM_PERCENT": "toplam RAM yuzdesi (vars: 25)",
                    "RAM_HOLD_MIN": "tepe bekleme dk (vars: 5)",
                    "RAM_STEP_INTERVAL_S": "adim araligi sn (vars: 5)"},
         "description": "kademeli bellek isgali (memory pressure)"},
        {"type": "disk", "script": "disk_fill_test.sh",
         "params": {"DISK_FILL_GB": "yazilacak GB (vars: 5)"},
         "description": "disk doldurma"},
        {"type": "net", "script": "network_flap.sh",
         "params": {"NET_DURATION_S": "sure sn"},
         "description": "ag trafigi simulasyonu (iperf3)"},
    ]


def _script_cmd(test_type: str) -> list:
    mapping = {t["type"]: t["script"] for t in available_tests()}
    if test_type not in mapping:
        raise ValueError(f"bilinmeyen test turu: {test_type}")
    script = os.path.join(config.LOAD_TESTS_DIR, mapping[test_type])
    if not os.path.exists(script):
        raise FileNotFoundError(f"test scripti yok: {script}")
    if script.endswith(".py"):
        return [sys.executable, script]
    return ["bash", script]


def _read_state() -> dict:
    try:
        with open(_STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _write_state(state: dict):
    with open(_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def start(test_type: str, params: dict | None = None) -> dict:
    state = _read_state()
    if state.get("pid") and _pid_alive(state["pid"]):
        raise RuntimeError(
            f"zaten calisan test var: {state.get('type')} (pid {state['pid']})")

    env = dict(os.environ)
    for k, v in (params or {}).items():
        env[str(k)] = str(v)

    cmd = _script_cmd(test_type)
    log_path = os.path.join(os.path.dirname(_STATE_FILE),
                            f"api_loadtest_{test_type}.log")
    logf = open(log_path, "ab")
    proc = subprocess.Popen(
        cmd, env=env, stdout=logf, stderr=subprocess.STDOUT,
        start_new_session=True,  # API restart'inda test olmesin
    )
    state = {"pid": proc.pid, "type": test_type, "params": params or {},
             "started_at": int(time.time()), "log": log_path}
    _write_state(state)
    return state


def status() -> dict:
    state = _read_state()
    if not state:
        return {"running": False}
    alive = state.get("pid") and _pid_alive(state["pid"])
    out = dict(state, running=bool(alive))
    if state.get("started_at"):
        out["elapsed_s"] = int(time.time()) - state["started_at"]
    # log kuyrugu (son 2KB)
    try:
        with open(state.get("log", ""), "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 2048))
            out["log_tail"] = f.read().decode(errors="replace")
    except OSError:
        out["log_tail"] = ""
    return out


def stop() -> dict:
    state = _read_state()
    pid = state.get("pid")
    if not pid or not _pid_alive(pid):
        return {"stopped": False, "reason": "calisan test yok"}
    # start_new_session=True ile acildi -> tum grup sonlandirilir
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except (OSError, ProcessLookupError):
        os.kill(pid, signal.SIGTERM)
    _write_state({})
    return {"stopped": True, "pid": pid, "type": state.get("type")}
