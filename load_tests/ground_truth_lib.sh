#!/bin/bash
# Ortak ground-truth loglama yardimcisi.
# Tum anomali testleri (controller + standalone) bu dosyayi source eder ve
# log_ground_truth fonksiyonu ile data/ground_truth.jsonl'e (JSONL) yazar.
#
# Kullanim (test scriptinin basinda):
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/ground_truth_lib.sh"
#   log_ground_truth cpu cpu_burn "$START_TS" "$END_TS" '{"cores": 4}'

# data/ground_truth.jsonl yolu: load_tests/../data/ground_truth.jsonl
_LIB_SRC="${BASH_SOURCE[0]:-$0}"
GT_FILE="$(cd "$(dirname "$_LIB_SRC")/.." && pwd)/data/ground_truth.jsonl"
GT_HOST="${GT_HOST:-Zabbix server}"

log_ground_truth() {
    local type="$1"
    local label="$2"
    local start_ts="$3"
    local end_ts="$4"
    local metadata="$5"
    [[ -z "$metadata" ]] && metadata="{}"

    mkdir -p "$(dirname "$GT_FILE")"

    # Idempotent: ayni (type, start, end) penceresi tekrar yazilmaz.
    if [[ -f "$GT_FILE" ]] && grep -qE "\"type\": \"$type\".*\"start_ts\": $start_ts.*\"end_ts\": $end_ts" "$GT_FILE"; then
        echo "[ground-truth] atlandi (tekrar): $type $start_ts -> $end_ts"
        return 0
    fi

    local id
    id="$type-$(uuidgen 2>/dev/null | cut -c1-8 || echo "$start_ts")"

    printf '{"anomaly_id": "%s", "type": "%s", "host": "%s", "label": "%s", "start_ts": %s, "end_ts": %s, "metadata": %s}\n' \
        "$id" "$type" "$GT_HOST" "$label" "$start_ts" "$end_ts" "$metadata" >> "$GT_FILE"
    echo "[ground-truth] loglandi: $id $type $label $start_ts -> $end_ts"
}
