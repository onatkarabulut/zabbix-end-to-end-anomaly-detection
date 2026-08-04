import time
import sys
import datetime

LOG_FILE = "load_data/anomaly_ground_truth.log"

def log_event(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp} - {message}\n")

log_event("[ANOMALİ BAŞLANGICI] Kademeli Bellek Sızıntısı (Maks 2GB) başladı.")
print("[*] GÜVENLİ Bellek Sızıntısı testi başlıyor...")

# Güvenlik Limiti: 40 adım * 50MB = Maksimum 2 GB RAM işgal edilecek.
MAX_STEPS = 40 
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
    print("\n[-] Test manuel durduruldu.")
finally:
    # Güvenlik: RAM kesin olarak serbest bırakılır.
    del leak_list
    log_event("[ANOMALİ BİTİŞİ] Bellek Sızıntısı sona erdi. RAM temizlendi.")
    print("[*] RAM serbest bırakıldı, sistem normale döndü.")
    sys.exit(0)