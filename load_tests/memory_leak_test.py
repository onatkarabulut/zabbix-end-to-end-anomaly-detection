import json
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GT_FILE = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "data", "ground_truth.jsonl"))
GT_HOST = os.getenv("GT_HOST", "Zabbix server")

print("[*] GÜVENLİ Bellek Sızıntısı testi başlıyor...")

# Güvenlik Limiti: 40 adım * 50MB = Maksimum 2 GB RAM işgal edilecek.
MAX_STEPS = 40
start_ts = int(time.time())
leak_list = []
step = 0

try:
    while step < MAX_STEPS:
        leak_list.append(' ' * 50 * 1024 * 1024)
        step += 1
        print(f"    -> Adım {step}/{MAX_STEPS}: Toplam işgal: ~{step * 50} MB")
        time.sleep(10)

    print("\n[*] Güvenli limite ulaşıldı. Zabbix'in trendi yakalaması için 2 dakika bekleniyor...")
    time.sleep(120)

except KeyboardInterrupt:
    print("\n[-] Test manuel durduruldu. Ground-truth'a yazılmıyor (iptal edildi).")
    del leak_list
    sys.exit(0)

finally:
    del leak_list
    print("[*] RAM serbest bırakıldı, sistem normale döndü.")

end_ts = int(time.time())
record = {
    "anomaly_id": f"ram-{start_ts}",
    "type": "ram",
    "host": GT_HOST,
    "label": "memory_pressure",
    "start_ts": start_ts,
    "end_ts": end_ts,
    "metadata": {"size": "2G"},
}

os.makedirs(os.path.dirname(GT_FILE), exist_ok=True)
with open(GT_FILE, "a", encoding="utf-8") as f:
    f.write(json.dumps(record, ensure_ascii=False) + "\n")
print(f"[ground-truth] loglandi: {record['anomaly_id']} ram memory_pressure {start_ts} -> {end_ts}")
