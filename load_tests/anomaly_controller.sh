#!/bin/bash

# Zabbix Anomali Testi Ana Tetikleyici Scripti
# Kullanım: ./anomaly_controller.sh <cpu|ram|disk|net> <start|stop>

# Dizin yoksa oluştur
mkdir -p load_data
CSV_FILE="load_data/anomali_ground_truth.csv"

# Eğer CSV dosyası henüz yoksa, ilk satır olarak başlıkları ekle
if [ ! -f "$CSV_FILE" ]; then
    echo "Test_Tipi,Tarih,Baslangic_Saati,Bitis_Saati,Sure_Saniye,Durum" > "$CSV_FILE"
fi

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
    
    # Eğer start komutuyla çalışırken Ctrl+C yapıldıysa CSV'ye kaydet
    if [ "$ACTION" == "start" ]; then
        END_TS=$(date +%s)
        END_TIME=$(date '+%H:%M:%S')
        DURATION=$((END_TS - START_TS))
        
        echo "$TEST_TYPE,$START_DATE,$START_TIME,$END_TIME,$DURATION,Iptal_Edildi" >> "$CSV_FILE"
        echo "[*] Yarım kalan test CSV'ye 'Iptal_Edildi' olarak kaydedildi. (Süre: $DURATION sn)"
    fi
    
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

# Başlangıç zamanlarını yakala
START_TS=$(date +%s)
START_DATE=$(date '+%Y-%m-%d')
START_TIME=$(date '+%H:%M:%S')

echo "=================================================="
echo "[*] $START_DATE $START_TIME - '$TEST_TYPE' anomali testi başlatılıyor..."
echo "=================================================="

# CASE mantığı ile ilgili testi çalıştır
case $TEST_TYPE in
    cpu)
        echo "[*] GÜVENLİ CPU Testi (Spike): 10 dakika boyunca %85 yük uygulanıyor..."
        # Arka planda çalıştırıp wait ile bekliyoruz ki Ctrl+C anında devralabilsin
        timeout -k 615s 600s stress-ng --cpu 0 --cpu-load 85 --timeout 600s --metrics-brief &
        wait $!
        ;;
        
    ram)
        echo "[*] GÜVENLİ Bellek Sızıntısı (Memory Leak): Maksimum 2GB RAM işgal edilecek..."
        python3 -c "
import time
MAX_STEPS = 40
leak_list = []
print('    -> RAM yavaş yavaş dolduruluyor...')
for step in range(MAX_STEPS):
    leak_list.append(' ' * 50 * 1024 * 1024)
    time.sleep(10)
print('    -> 2GB sınıra ulaşıldı. Zabbix tespiti için 2 dakika bekleniyor...')
time.sleep(120)
del leak_list
print('    -> RAM serbest bırakıldı.')
" &
        wait $!
        ;;
        
    disk)
        echo "[*] GÜVENLİ Disk Doldurma: Bulunulan dizinde 5GB dosya oluşturuluyor..."
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
END_TIME=$(date '+%H:%M:%S')
DURATION=$((END_TS - START_TS))

# Trap bağlantısını kaldır (Temiz kapanış)
trap - SIGINT SIGTERM

# Veriyi CSV'ye kaydet (Başarıyla bitenler)
echo "$TEST_TYPE,$START_DATE,$START_TIME,$END_TIME,$DURATION,Tamamlandi" >> "$CSV_FILE"

echo "=================================================="
echo "[*] Test başarıyla tamamlandı!"
echo "[*] Toplam Süre: $DURATION saniye"
echo "[*] Sonuçlar '$CSV_FILE' dosyasına kaydedildi."
echo "=================================================="