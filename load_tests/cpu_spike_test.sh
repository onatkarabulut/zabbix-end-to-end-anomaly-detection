#!/bin/bash
# 600 saniye (10 dakika) boyunca CPU çekirdeklerini %85 yüke sabitler.
# Güvenlik: timeout komutu ile 610. saniyede zorla kapatılır (SIGKILL).

LOG_FILE="load_data/anomaly_ground_truth.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') - [ANOMALİ BAŞLANGICI] CPU Spike (%85) tetiklendi." >> $LOG_FILE
echo "[*] GÜVENLİ CPU Testi çalışıyor..."

timeout -k 610s 600s stress-ng --cpu 0 --cpu-load 85 --timeout 600s --metrics-brief

echo "$(date '+%Y-%m-%d %H:%M:%S') - [ANOMALİ BİTİŞİ] CPU Spike sona erdi." >> $LOG_FILE
echo "[*] Test tamamlandı, sistem normale döndü."