#!/bin/bash
# Bulunulan dizinde yavaş yavaş 5GB'lık bir dosya oluşturur ve işi bitince siler.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/ground_truth_lib.sh"

DUMP_FILE="zabbix_anomaly_test.img"

START_TS=$(date +%s)
echo "[*] GÜVENLİ Disk Testi çalışıyor..."

# Güvenlik: Her saniye 50MB yazarak diski boğmadan 100 adımda durur.
dd if=/dev/zero of=$DUMP_FILE bs=50M count=100 status=progress

echo "[*] Maksimum boyuta ulaşıldı, Zabbix'in yakalaması için 5 dakika bekleniyor..."
sleep 300

# Güvenlik: Dosya sistemde çöp olarak kalmasın diye mutlaka silinir.
rm -f $DUMP_FILE

END_TS=$(date +%s)
log_ground_truth disk disk_fill "$START_TS" "$END_TS" '{"file": "zabbix_anomaly_test.img", "size_gb": 5}'
echo "[*] Test tamamlandı, oluşturulan test dosyası silindi."
