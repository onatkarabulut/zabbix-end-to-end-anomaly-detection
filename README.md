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

## Yardimci araclar (ml/ + tools/)

```bash
# Warehouse'u MinIO'dan sifirdan yeniden insa (bozuk DB durumunda)
python tools/rebuild_warehouse.py --db data/zabbix_ml.db

# Zaman bazli train/test split (karistirma yok, gecmis->train gelecek->test)
python ml/train_test_split.py --db data/zabbix_ml.db --split-ts <epoch> --gap-min 60 --out data/
python ml/train_test_split.py --db data/zabbix_ml.db --split-ratio 0.8
#   --drop-hourly (varsayilan acik): _hourly_* sutunlarini duser (leakage korumasi)

# Feature selection (813 -> ~100-400; varyans + korelasyon dedup + top-k)
python ml/select_features.py --db data/zabbix_ml.db --variance-threshold 1e-6 --corr-threshold 0.95 --out data/selected_features.csv
python ml/select_features.py --input data/train.csv --top-k 200
```

Split ve select araclari `datetime_minute`, `host`, `is_anomaly`,
`time_since_last_alarm` ve `_hourly_*` sutunlarini ozellik adaylarindan haric
tutar (leakage + hedef kolon korumasi).

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

`tools/ground_truth.py` izlenen host uzerinde yapay yuk uretir ve zaman
pencerelerini `data/ground_truth.jsonl`'a loglar; `labels` komutu ML icin
dakika bazli 0/1 hedef etiket CSV'si uretir.

```bash
python tools/ground_truth.py cpu    --duration 120 --label cpu_burn --cores 2
python tools/ground_truth.py memory --duration 120 --size 2G
python tools/ground_truth.py disk   --duration 120 --size 1G --path /tmp/zabbix_gt
python tools/ground_truth.py net    --duration 60 --target 10.0.0.5 --rate 100M
python tools/ground_truth.py manual --start <utc_epoch> --end <utc_epoch> --label note
python tools/ground_truth.py list
python tools/ground_truth.py labels --db data/zabbix_ml.db --out data/labels.csv
```

Notlar:
- Zamanlar UTC epoch'tur; `ml_feature_matrix` de UTC oldugundan uyumludur.
- `cpu/memory` icin `stress-ng`, `net` icin `iperf3` (karsi taraf `iperf3 -s`),
  `disk` icin `dd` gereklidir. Container icinde root gerektiren komutlar icin
  `--sudo` bayragini kullanin.
- `labels` ciktisi: `datetime_minute, host, label(0/1), anomaly` — anomali
  penceresi [start, end) araligindaki her dakikaya 1 isaretler.
