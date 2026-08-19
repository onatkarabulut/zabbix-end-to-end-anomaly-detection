# Zabbix ML Pipeline

Zabbix PostgreSQL verisini saatlik chunk'lar halinde MinIO'ya cekip, SQLite
veri ambarina yukleyen ve ML icin feature engineering yapan Apache Airflow
boru hatti.

## Mimari

```
Zabbix PostgreSQL ──> extract.py ──> MinIO (zabbix-raw-data, parquet)
                                          │
                                          ├──> validate.py (parquet dogrulama)
                                          ├──> transform.py (feature matrix)
                                          │        └──> MinIO (zabbix-processed-data)
                                          └──> load.py (SQLite ambar + ham tablolar)
                                                    │
                                                    └──> feature_engineering.py
                                                         └──> ml_features_enriched
```

Airflow DAG (`dags/zabbix_ml_etl.py`) her saat kosar:

1. `extract_zabbix_data` — history/trends/events/problem chunk'larini + hosts/items/
   triggers/functions snapshot'larini PostgreSQL'den MinIO'ya yazar (retry'li).
2. `validate_raw_parquet` — chunk icin zorunlu parquet dosyalarinin var oldugunu
   ve bos olmadigini dogrular.
3. `transform_to_feature_matrix` — history + items + hosts'tan dakika bazli pivot
   feature matrix uretir; trends'ten `_hourly_*` ozellikleri ekler.
4. `load_to_sqlite_warehouse` — feature matrix'i `ml_feature_matrix` tablosuna yazar.
5. `load_raw_tables` — ham `events`, `problem`, `triggers`, `functions`, `items`,
   `hosts` tablolarini MinIO'dan SQLite'a yeniden yukler (idempotent, PK index'li).
6. `feature_engineering` — kayan pencere ozellikleri (`_avg/_std/_trend_{5,30,60}m`)
   ve `time_since_last_alarm` hesaplar -> `ml_features_enriched`.

## SQLite tablolari

| Tablo                | Icerik                                       | PK / Unique        |
|----------------------|----------------------------------------------|--------------------|
| `ml_feature_matrix`  | Ham pivot feature matrix (dakika bazli)      | (host, datetime_minute) |
| `ml_features_enriched` | Kayan pencere + alarm ozellikleri          | (host, datetime_minute) |
| `events`             | Alarm/olay gecmisi (source=0, value=1 alarm) | eventid            |
| `problem`            | Problem kayitlari                            | eventid            |
| `triggers`/`functions` | Trigger ve fonksiyon referansi             | triggerid / functionid |
| `items`/`hosts`      | Item ve host envanteri                       | itemid / hostid    |

`time_since_last_alarm`: `events` (source=0, value=1) tablosu triggers/functions/
items/hosts ile join'lenerek alarm zamanlari cikarilir, `merge_asof` (backward,
point-in-time) ile her dakikaya en son alarmdan gecen sure (dakika) yazilir. Alarm
yoksa 999999. `events` bos ise `problem` tablosuna dusulur.

## Hedef kolon: `is_anomaly`

`ml_features_enriched` icindeki `is_anomaly` (0/1) hedef/etiket kolonudur,
**asla ozellik olarak kullanilmamalidir**. Su pencereler `1` sayilir:

- Zabbix alarm olaylari (`events` source=0, value=1): `[alarm_dk, alarm_dk + ALARM_LABEL_WINDOW_MIN)` — varsayilan pencere 10 dk (`ALARM_LABEL_WINDOW_MIN` env).
- `data/ground_truth.jsonl` pencereleri (`[start, end)`) — dosya varsa `is_anomaly=1`.

Ground truth dosyasi yoksa etiketler sadece alarmlardan uretilir.

## Yardimci araclar (tools/)

```bash
# Warehouse'u MinIO'dan sifirdan yeniden insa (bozuk DB durumunda)
python tools/rebuild_warehouse.py --db data/zabbix_ml.db

# Model versiyon yonetimi (list / promote / rollback / current / remove)
python tools/model_cli.py list
python tools/model_cli.py promote run_verify_tb2

# Model denetimi (sizinti, reprodüksiyon, degradation guard)
python tools/model_audit.py --db data/zabbix_ml.db --run-dir data/models/runs/<run_id>
```

Split ve feature selection, egitim hatti `ml/train_anomaly_models.py`
icerisindedir (zaman-bazli `now - 14 gun` split + `select_features_on_train`).
`datetime_minute`, `host`, `is_anomaly`, `time_since_last_alarm` ve
`_hourly_*` kolonlari ozellik adaylarindan haric tutulur (leakage + hedef
kolon korumasi).

## Kurulum & calistirma

Ortam degiskenleri (Airflow container'inda):

- `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` — Zabbix PostgreSQL baglantisi
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` — MinIO (default minioadmin/minioadmin123)
- `SQLITE_DB_PATH` — ambar yolu (default `/opt/airflow/data/zabbix_ml.db`)
- `MAX_BACKFILL_HOURS` — geriye donuk tarama limiti (default 720)
- `EXTRACT_TIMEOUT_MS`, `EXTRACT_PAGE_SIZE` — extract ayarlari

Kod parcalarini bagimsiz test etmek icin:

```bash
export SQLITE_DB_PATH=/opt/airflow/data/zabbix_ml.db
python -m ETL.transform <chunk_id>          # ornek: chunk_1785708000_1785711600
python -m ETL.load <chunk_id>
python -m ETL.validate <chunk_id>
python -m ETL.feature_engineering            # full backfill
```

DAG'i Airflow'tan tetiklemek: `zabbix_ml_pipeline` dag'ini acip "Trigger DAG".

## Ground truth (sentetik anomali + etiket)

Ground truth tek kaynaktan gelir: `data/ground_truth.jsonl` (JSONL). Anomali
testleri (`load_tests/`) basarili tamamlaninca pencerelerini dogrudan buraya
yazar (UTC epoch, `tools/ground_truth.py` ile ayni format). Iptal edilen
testler loglanmaz; ayni pencere tekrar yazilmaz (idempotent).

Iki giris yolu vardir:

1. **Controller (tek giris noktasi)** — `load_tests/anomaly_controller.sh`:
   ```bash
   cd load_tests
   ./anomaly_controller.sh cpu start     # cpu/ram/disk/net
   ./anomaly_controller.sh cpu stop      # iptal/temizlik
   ```
2. **Standalone scriptler** — `load_tests/cpu_spike_test.sh`,
   `disk_fill_test.sh`, `network_flap.sh`, `memory_leak_test.py`. Hepsi
   `load_tests/ground_truth_lib.sh`'i source edip `log_ground_truth` ile
   `data/ground_truth.jsonl`'e yazar.

RAM testi host RAM'ini otomatik algilar ve `RAM_PERCENT` kadarini isgal eder
(OOM guvenlik kapagi: kullanilabilir bellegin %90'i). Ornekler:

```bash
./anomaly_controller.sh ram start                  # varsayilan: toplam RAM'in %25'i
RAM_PERCENT=40 ./anomaly_controller.sh ram start   # hedefi buyut
RAM_HOLD_MIN=10 ./anomaly_controller.sh ram start  # tepe noktada bekleme (dk)
```

46GiB host'ta `RAM_PERCENT=25` -> ~12GiB yuk, `vm.memory.util`'de belirgin bir
spike olusturur; 8GiB host'ta ayni script ~2GiB yapar (orantisal, tasinabilir).

Ayrica `tools/ground_truth.py` elle pencere loglar ve etiket uretir:

```bash
python tools/ground_truth.py manual --start <utc_epoch> --end <utc_epoch> --label note
python tools/ground_truth.py list
# Istege bagli: dakika bazli 0/1 etiket CSV'si (gorsel dogrulama / analiz icin)
python tools/ground_truth.py labels --db data/zabbix_ml.db --out data/labels.csv
```

Notlar:
- Zamanlar UTC epoch'tur; `ml_feature_matrix` de UTC oldugundan uyumludur.
- `cpu/memory` icin `stress-ng`, `net` icin `iperf3`, `disk` icin `dd`
  gereklidir.
- **Tek gercek etiket kaynagi `ground_truth.jsonl`'dir.** `labels.csv`
  (`ground_truth.py labels`) opsiyonel, turetilmis bir dosyadir — anomali
  penceresi [start, end) araligindaki her dakikaya 1 isaretler; model egitimi
  bunu kullanmaz, `y_true`'yu dogrudan jsonl'den uretir.

## Faz 3 - Model karsilastirmasi

`ml/train_anomaly_models.py` ham `ml_feature_matrix`'ten okur, data leakage
olmadan (PDF Faz 2/3 kosulu) split + feature engineering + 3 model calistirir:

```bash
# Gerekli kutuphaneler (host'ta yoksa once kur):
python3 -m pip install -r requirements-ml.txt

# Egitim (varsayilan split: now - 14 gun, surekli kayar)
python ml/train_anomaly_models.py --db data/zabbix_ml.db
# Opsiyonlar:
#   --split-ts <epoch>    split noktasi (oncesi train, sonrasi test)
#   --thr-quantile 0.99   train skoru quantile'i (ustu anomali)
#   --top-k 300           secilen ozellik sayisi
#   --seq-len 60          LSTM pencere uzunlugu
#   --epochs 10           LSTM epoch
#   --skip-lstm           LSTM'i atla (torch yoksa / hizli kosum icin)
```

Leakage korumasi:

1. Split **ham** `ml_feature_matrix` uzerinde yapilir (rolling ozellik icermez).
2. Kayan pencere (`_avg/_std/_trend`) + scaler **train'de fit edilir**, test'e
   ayri uygulanir.
3. Ozellik secimi (varyans + korelasyon + top-k) sadece train'de.
4. `y_true` `ground_truth.jsonl`'den uretilir, asla model ozelligi olmaz.

Ciktilar (`data/`):

| Dosya | Icerik |
|-------|--------|
| `eval_results.csv` | Model basina precision/recall/F1 (y_true = sentetik testler) |
| `predictions.csv` | Dakika bazli skor + tahmin + gercek etiket |
| `comp_vs_zabbix.csv` | ML vs Zabbix alarm karsilastirmasi (gecikme dakika + yakalanma) |
| `models/` | `scaler.joblib`, `<model>.joblib`, `<model>_thr.txt` |

Zabbix alarmlari `y_true`'ya karistirilmaz; `comp_vs_zabbix.csv`'de ayri bir
baseline olarak raporlanir (model ne kadar erken / kac yanlis alarm).

## Faz 3 - Sonuclar

Sentetik testlerle (3 CPU burn, 2 memory pressure, 1 disk fill) egitilen
modellerin, dinamik split `now - 14 gun` (ornek retrain: 2026-08-05) sonrasi
test seti sonuclari. Hiperparametreler: `top-k=150`, `thr-quantile=0.9995`,
`seq-len=60`:

| Model | Precision | Recall | F1 |
|-------|-----------|--------|-----|
| LSTM-Autoencoder | 0.00 | 0.00 | 0.00 |
| OneClassSVM | 0.073 | 1.00 | 0.137 |
| IsolationForest | 0.00 | 0.00 | 0.00 |

- OneClassSVM her iki memory penceresini de tam yakaladi (15/15 + 15/15,
  latency 0 dk); testteki tek skorlanabilir anomali turu memory oldu.
- **LSTM 0**: test anomali pencereleri (13:00-13:44) verideki 20 dk'lik
  kopuklugun (12:02-12:22) hemen sonrasinda; LSTM skoru sekansin sonuna
  atandigindan pencereler yapısal olarak skorlanamadi. Realtime ile ayni
  kurala (tum gecmis uzerinden kayan pencere) getirildi, ancak bu veri
  kisitini asamaz.
- **IsolationForest 0**: testte hic pozitif uretmedi (esik cok sik).
- Not: `disk_fill` penceresinde (14.08) feature matrix'te hic satir yok
  (ETL o gun calismadi) — model o anomaliyi goremez. Test yalnizca 30 anomali
  dakikasi icerdigi icin F1 metrikleri kucuk orneklemden etkilenir; recall 1.0
  dogru kullanicinin istedigi tespit davranisini yansitir.
- `comp_vs_zabbix.csv`'de tum pencerelerde `zabbix_alarm_overlap=False`:
  sentetik testler Zabbix trigger esiklerini asmadigindan Zabbix hicbir anomaliyi
  alarmlamadi — ML'in erken yakalama potansiyeli PDF tezini destekliyor.

### Network flap (ağ) metrik tipi neden dahil edilemedi

Faz 3 raporu CPU spike / kademeli disk dolumu / **network flap** ayrimini
istiyor, ancak ag metrikleri pipeline'a hic girmiyor:

- Zabbix agent'ta `lo` (loopback) arayuzu icin item yok; yalnizca
  `net.if.in/out["eth0"]` item'lari mevcut.
- ETL (`transform.py`) yalnizca Zabbix'te var olan item'lari pivotlar; ag
  verisi olmadigindan `ml_feature_matrix`'te hicbir `net.*` kolonu olusmuyor.
- `load_tests/anomaly_controller.sh net start` ile iperf3 loopback testi
  yapildi (14.08 18:19, 300s, ~86 Gbit/s), ancak `lo` izlenmediginden
  Zabbix'e/history'ye yansimadi ve ground truth'ten cikarildi.

Bu nedenle "network flap" metrik tipi model karsilastirmasina dahil
edilemedi; CPU/RAM/disk sonuclari bu durumdan etkilenmez (egitim zaten ag
kolonu icermiyordu). Gercek ag izlemesi icin agent'ta `lo` veya fiziksel
arayuz item'i tanimlanip ETL'in yeniden kusturulmesi gerekir.
