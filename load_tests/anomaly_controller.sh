#!/bin/bash

# Zabbix Anomali Testi Ana Tetikleyici Scripti
# Kullanim: ./anomaly_controller.sh <cpu|ram|disk|net> <start|stop>
#
# Basarili tamamlanan testler data/ground_truth.jsonl dosyasina JSONL satir
# olarak yazilir (tools/ground_truth.py ile ayni format). Iptal edilen testler
# anomali olarak loglanmaz.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/ground_truth_lib.sh"

LABEL_CPU=cpu_burn
LABEL_RAM=memory_pressure
LABEL_DISK=disk_fill
LABEL_NET=network_burst

TEST_TYPE=$1
ACTION=$2

# Parametreler eksikse uyar
if [ -z "$TEST_TYPE" ] || [ -z "$ACTION" ]; then
    echo "Kullanım Hatası: Test tipi ve eylem belirtmelisiniz!"
    echo "Örnek Başlatma : ./anomaly_controller.sh cpu start"
    echo "Örnek Durdurma : ./anomaly_controller.sh cpu stop"
    echo "Seçenekler     : cpu, ram, disk, net"
    exit 1
fi

# ==========================================
# GÜVENLİK VE TEMİZLİK (CLEANUP) FONKSİYONU
# ==========================================
cleanup() {
    echo ""
    echo "[!] Ctrl+C algılandı! İşlem anında durduruluyor..."

    # Tüm test araçlarını acımasızca (SIGKILL) kapat
    pkill -9 stress-ng 2>/dev/null
    pkill -9 iperf3 2>/dev/null
    pkill -9 -f "dd if=/dev/zero" 2>/dev/null
    pkill -9 -f "python3 -c" 2>/dev/null

    # Disk testi yarım kaldıysa oluşturulan çöp dosyayı sil
    rm -f zabbix_test_data.img 2>/dev/null

    echo "[*] Sistem temizlendi ve normale döndü."
    exit 1
}

# Eğer eylem 'stop' ise sadece temizlik yap ve çık
if [ "$ACTION" == "stop" ]; then
    ACTION="stop_only"
    cleanup
fi

# Eğer eylem 'start' değilse uyar
if [ "$ACTION" != "start" ]; then
    echo "[!] Hatalı eylem! Sadece 'start' veya 'stop' kullanabilirsiniz."
    exit 1
fi

# Ctrl+C (SIGINT) veya SIGTERM sinyallerini yakala ve anında cleanup fonksiyonuna git
trap 'cleanup' SIGINT SIGTERM

# ==========================================
# TEST BAŞLANGICI
# ==========================================

# Başlangıç zamanlarını yakala (UTC epoch - ground_truth ile uyumlu)
START_TS=$(date +%s)
START_DATE=$(date '+%Y-%m-%d')
START_TIME=$(date '+%H:%M:%S')

echo "=================================================="
echo "[*] $START_DATE $START_TIME - '$TEST_TYPE' anomali testi başlatılıyor..."
echo "=================================================="

METADATA="{}"

# CASE mantığı ile ilgili testi çalıştır
case $TEST_TYPE in
    cpu)
        echo "[*] GÜVENLİ CPU Testi (Spike): 10 dakika boyunca %85 yük uygulanıyor..."
        METADATA="{\"cores\": $(nproc)}"
        # Arka planda çalıştırıp wait ile bekliyoruz ki Ctrl+C anında devralabilsin
        timeout -k 615s 600s stress-ng --cpu 0 --cpu-load 85 --timeout 600s --metrics-brief &
        wait $!
        ;;

    ram)
        echo "[*] GÜVENLİ Bellek Testi: RAM_PERCENT=${RAM_PERCENT:-25}% hedef (otomatik algılanan RAM üzerinden)..."
        RAM_PERCENT="${RAM_PERCENT:-25}" RAM_HOLD_MIN="${RAM_HOLD_MIN:-5}" \
            python3 "$SCRIPT_DIR/memory_leak_test.py" &
        wait $!
        LOGGED_BY_SCRIPT=1
        ;;

    disk)
        echo "[*] GÜVENLİ Disk Doldurma: Bulunulan dizinde 5GB dosya oluşturuluyor..."
        METADATA='{"file": "zabbix_test_data.img", "size_gb": 5}'
        dd if=/dev/zero of=zabbix_test_data.img bs=50M count=100 status=progress &
        wait $!
        echo "[*] Zabbix'in disk düşüşünü yakalaması için 5 dakika bekleniyor..."
        sleep 300 &
        wait $!
        rm -f zabbix_test_data.img
        echo "[*] Test dosyası silindi."
        ;;

    net)
        echo "[*] GÜVENLİ Ağ Darboğazı: Localhost üzerinde iperf3 ile 5 dakika yük..."
        METADATA='{"target": "127.0.0.1"}'
        iperf3 -s -D
        iperf3 -c 127.0.0.1 -t 300 &
        wait $!
        pkill iperf3
        ;;

    *)
        echo "[!] HATA: Geçersiz test tipi ('$TEST_TYPE')."
        echo "[!] Seçenekler: cpu, ram, disk, net"
        exit 1
        ;;
esac

# Bitiş zamanlarını yakala ve süreyi hesapla
END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))

# Trap bağlantısını kaldır (Temiz kapanış)
trap - SIGINT SIGTERM

# Ground truth'e yaz (basarili testler icin)
if [ -z "${LOGGED_BY_SCRIPT:-}" ]; then
    LABEL_VAR="LABEL_$(echo "$TEST_TYPE" | tr '[:lower:]' '[:upper:]')"
    LABEL="${!LABEL_VAR}"
    log_ground_truth "$TEST_TYPE" "$LABEL" "$START_TS" "$END_TS" "$METADATA"
fi

echo "=================================================="
echo "[*] Test başarıyla tamamlandı!"
echo "[*] Toplam Süre: $DURATION saniye"
echo "[*] Sonuçlar '$GT_FILE' dosyasına kaydedildi."
echo "=================================================="
