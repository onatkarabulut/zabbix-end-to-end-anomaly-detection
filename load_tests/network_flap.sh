#!/bin/bash
# iperf3 ile localhost üzerinde aşırı ağ trafiği yaratır.
# Çalıştırmadan önce iperf3 paketi kurulu olmalıdır (sudo pacman -S iperf3)

LOG_FILE="load_data/anomaly_ground_truth.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') - [ANOMALİ BAŞLANGICI] Ağ trafiği darboğazı başladı." >> $LOG_FILE
echo "[*] GÜVENLİ Ağ Testi çalışıyor (Sadece Localhost)..."

# Arka planda iperf3 sunucusunu başlatır (-D daemon mod)
iperf3 -s -D

# Güvenlik: Süre 300 saniye (5 dakika) ile sınırlıdır.
iperf3 -c 127.0.0.1 -t 300

# Arka plandaki sunucuyu güvenle kapatır.
pkill iperf3

echo "$(date '+%Y-%m-%d %H:%M:%S') - [ANOMALİ BİTİŞİ] Ağ trafiği normale döndü." >> $LOG_FILE
echo "[*] Test tamamlandı."
