# Araştırma Görevi - Teslim Tarihi : 14.08.2026

### Zabbix Tabanlı Uçtan Uca Anomali Tespit ve Erken Uyarı Sistemi

## Senaryo

Operasyon ekibi, mevcut Zabbix trigger'larının çok fazla yanlı**ş** alarm (false positive) üretti**ğ**inden vekritik olayları geç yakaladı**ğ**ından **ş**ikayetçi. Bu görevde senden, geçmi**ş** verilerle e**ğ**itilen bir makine

ö**ğ**renmesi hattını (pipeline) canlı veri üzerinde çalı**ş**ır hale getirmen bekleniyor.

## Kapsam

Görev dört fazdan olu**ş**uyor ve her faz bir öncekinin çıktısı üzerine in**ş**a ediliyor. Amaç yalnızca kodçalı**ş**tırmak de**ğ**il, uçtan uca çalı**ş**an bir sistem kurmak. Her fazı bitirmeden bir sonrakine geçme — sırayla

ilerle.

## Veri Kaynağı

Bu görevde sana hazır bir Zabbix veritabanı veya veri seti verilmeyecek — kendi Zabbix ortamını kurup kendi verini üreteceksin. Bu, görevin ayrılmaz bir parçası: gerçek bir sistemde de veri toplama altyapısını

sıfırdan kurman gerekecek.

- Docker ile ücretsiz bir Zabbix sunucusu + agent kur (resmi Zabbix Docker imajlarını kullanabilirsin: zabbix/zabbix-server-pgsql, zabbix/zabbix-web-nginx-pgsql, zabbix/zabbix-agent).
- • Kendi bilgisayarını, bir sanal makineyi veya birkaç konteyneri izlemeye ba**ş**la — CPU, bellek, disk, a**ğ**metrikleri böylece organik ve gerçek olarak birikmeye ba**ş**lar.
- • En az 1-2 haftalık sürekli veri toplanmasını bekle; modelin ö**ğ**renebilece**ğ**i kadar 'normal davranı**ş**'verisi birikmeden anomali tespiti anlamsız kalır.
- • Anomali/alarm üretmek için kendi scriptlerinle yapay yük bindir: stress-ng ile CPU/bellek yükü, birdisk doldurma scripti, a**ğ** trafi**ğ**i simülasyonu (iperf3 vb.) gibi araçlar kullanabilirsin.
- • Üretti**ğ**in anormal olayları not al (hangi gün/saatte ne yaptı**ğ**ını logla) — bu senin ground truth'unolacak, gerçek problems tablosunun yerini tutacak.
- • Belirli senaryolar tasarlaman i**ş**ini kolayla**ş**tırır: örn. "3. gün disk doldurma", "5. gün CPU spike'ı",
- "7. gün a**ğ** kesintisi simülasyonu" gibi planlı olaylar olu**ş**tur ve raporunda bunları belgeler.
_Not:_ Bu adım Faz 1'den önce, hazırlık a**ş**aması olarak dü**ş**ünülmeli. Ortamı kurup veri birikmeyeba**ş**ladıktan sonra Faz 1'e geçebilirsin.

### Faz 1 — Veri Altyapısı

- • history, trends, events, problems, items, hosts tablolarından üretim veritabanına yükbindirmeyecek **ş**ekilde bir veri çekme katmanı yaz (batch + pagination, retry mantı**ğ**ı, zaman
aralı**ğ**ına göre parçalama).

- • Ham veriyi ayrı bir analiz **ş**emasına (kendi Postgres/SQLite'ına) aktaran bir ETL scripti kur. Scriptidempotent olmalı — aynı script iki kez çalı**ş**tırılınca veri tekrarlanmamalı.
- • Zorla**ş**tırıcı kısıt: Script'in production veritabanına tek seferde en fazla X saniyelik sorgu süresiyleyük bindirmesine izin verilecek **ş**ekilde tasarla (query timeout + chunking zorunlu).
### Faz 2 — Özellik Mühendisliği

- • Her host için kayan pencere (rolling window) istatistikleri hesapla: 5 dk / 30 dk / 1 saatlik ortalama,standart sapma, trend e**ğ**imi, son N alarmdan bu yana geçen süre.
- • Alarmları etiket olarak kullanacaksan, veri sızıntısı (data leakage) olmadan zaman bazlı train/testayrımı yap — en sık dü**ş**ülen tuzak budur.
### Faz 3 — Üç Modelin Karşılaştırmalı Eğitimi

- • Aynı veri seti üzerinde üç yöntemi e**ğ**it: Isolation Forest, One-Class SVM, basit bir LSTMAutoencoder.
- • Ground truth olarak problems tablosunu kullanarak precision/recall/F1 ve gecikme metri**ğ**ini(anomali gerçek alarmdan kaç dakika önce/sonra yakalandı) hesapla.
- • Rapor: hangi model hangi metrik tipinde (CPU spike vs. kademeli disk dolumu vs. network flap)daha ba**ş**arılı, neden.
### Faz 4 — Gerçek Zamanlı Entegrasyon

- • Zabbix Connector veya real-time export ile gelen akı**ş**ı tüket, e**ğ**itti**ğ**in modeli online skorlamadakullan.
- • Skor bir e**ş**i**ğ**i geçince kendi API endpoint'ine (basit bir Django/DRF servisi) POST at, oradanZabbix'e external check veya webhook ile geri bildirim döngüsü kur.
- • Zorla**ş**tırıcı kısıt: Sistem, Zabbix sunucusu yeniden ba**ş**lasa veya export dosyası rotate olsa bile verikaybetmemeli — checkpoint/offset mantı**ğ**ı kur.
## Teslimatlar (Nihai)

_1._ Çalı**ş**an kod (repo + README: nasıl kurulur, nasıl çalı**ş**tırılır)
_2._ Model kar**ş**ıla**ş**tırma raporu (grafiklerle)
_3._ Kısa bir mimari diyagramı: Zabbix **→** ETL **→** model **→** geri bildirim
## Değerlendirme Kriterleri

- Kod kalitesi
- • Veri sızıntısı olmadan do**ğ**ru de**ğ**erlendirme yapılmı**ş** mı
- Production veritabanına yük bindirmeme konusunda gerçekçi önlemler var mı
- • Modelin ba**ş**arısız oldu**ğ**u durumlar dürüstçe raporlanmı**ş** mı
