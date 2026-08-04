#!/bin/bash
# Bulunulan dizinde yavaş yavaş 5GB'lık bir dosya oluşturur ve işi bitince siler.

LOG_FILE="load_data/anomaly_ground_truth.log"
DUMP_FILE="zabbix_anomaly_test.img"

echo "$(date '+%Y-%m-%d %H:%M:%S') - [ANOMALİ BAŞLANGICI] Kademeli disk doldurma başladı (Hedef: 5GB)." >> $LOG_FILE

# Güvenlik: Her saniye 50MB yazarak diski boğmadan 100 adımda durur.
dd if=/dev/zero of=$DUMP_FILE bs=50M count=100 status=progress

echo "[*] Maksimum boyuta ulaşıldı, Zabbix'in yakalaması için 5 dakika bekleniyor..."
sleep 300

# Güvenlik: Dosya sistemde çöp olarak kalmasın diye mutlaka silinir.
rm -f $DUMP_FILE

echo "$(date '+%Y-%m-%d %H:%M:%S') - [ANOMALİ BİTİŞİ] Disk temizlendi." >> $LOG_FILE
echo "[*] Test tamamlandı, oluşturulan test dosyası silindi."