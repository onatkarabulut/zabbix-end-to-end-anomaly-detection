#!/bin/bash
# iperf3 ile localhost üzerinde aşırı ağ trafiği yaratır.
# Çalıştırmadan önce iperf3 paketi kurulu olmalıdır (sudo pacman -S iperf3)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/ground_truth_lib.sh"

START_TS=$(date +%s)
echo "[*] GÜVENLİ Ağ Testi çalışıyor (Sadece Localhost)..."

# Arka planda iperf3 sunucusunu başlatır (-D daemon mod)
iperf3 -s -D

# Güvenlik: Süre 300 saniye (5 dakika) ile sınırlıdır.
iperf3 -c 127.0.0.1 -t 300

# Arka plandaki sunucuyu güvenle kapatır.
pkill iperf3

END_TS=$(date +%s)
log_ground_truth net network_burst "$START_TS" "$END_TS" '{"target": "127.0.0.1"}'
echo "[*] Test tamamlandı."
