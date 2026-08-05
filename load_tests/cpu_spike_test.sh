#!/bin/bash
# 600 saniye (10 dakika) boyunca CPU çekirdeklerini %85 yüke sabitler.
# Güvenlik: timeout komutu ile 610. saniyede zorla kapatılır (SIGKILL).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/ground_truth_lib.sh"

START_TS=$(date +%s)
echo "[*] GÜVENLİ CPU Testi çalışıyor..."

timeout -k 610s 600s stress-ng --cpu 0 --cpu-load 85 --timeout 600s --metrics-brief

END_TS=$(date +%s)
log_ground_truth cpu cpu_burn "$START_TS" "$END_TS" "{\"cores\": $(nproc)}"
echo "[*] Test tamamlandı, sistem normale döndü."
