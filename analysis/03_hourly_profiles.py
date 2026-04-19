"""
Задача: построить почасовой профиль входов пассажиров по станциям метро.

Фильтры:
- TRANSPORT_TYPE_ID = 1 (только метро)
- VALIDATION_MODE = 1 (только входы, не выходы)
- IS_TEST = 0 (через JOIN к REF_PSG_PLACES)
- Исключить аннулирования: TRN_TYPE_CATEGORY = 19
- Исключить выходные транзакции: TRN_TYPE_ID IN (70, 71)
- Время из INP_DATE

Выходной файл: analysis/output/station_hourly.csv
Поля: date, hour, weekday, is_weekend, ST_CODE, ST_NAME, LN_NAME, entries

Запуск: python3 analysis/03_hourly_profiles.py
Ожидаемое время: 15–30 минут на 89M строк (зависит от диска/CPU)
"""

import csv
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
PASS_FILE = ROOT / "data/pass_10-160324/PASS_ALL_202503242210.csv"
PLACES_FILE = ROOT / "data/pass_10-160324/REF_PSG_PLACES_202503251822.csv"
TRN_TYPE_FILE = ROOT / "data/pass_10-160324/TRN_TYPE_202503251753.csv"
OUT_DIR = ROOT / "analysis/output"
OUT_FILE = OUT_DIR / "station_hourly.csv"

# Transaction IDs for exit transactions (exclude from entry count)
EXIT_TRN_TYPE_IDS = {70, 71}  # Транзакция выхода БСК Classic/Ultralight

# TRN_TYPE_CATEGORY=19 are cancellations
CANCEL_CATEGORY = 19

WEEKDAYS = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']


def load_places_lookup():
    """Load REF_PSG_PLACES into memory: PLACE_ID -> {ST_CODE, ST_NAME, LN_NAME, IS_TEST}"""
    lookup = {}
    with open(PLACES_FILE, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            place_id = row.get('PLACE_ID', '').strip()
            if place_id:
                lookup[place_id] = {
                    'ST_CODE': row.get('ST_CODE', '').strip(),
                    'ST_NAME': row.get('ST_NAME', '').strip(),
                    'LN_NAME': row.get('LN_NAME', '').strip(),
                    'IS_TEST': row.get('IS_TEST', '0').strip(),
                }
    print(f"Loaded {len(lookup)} place records")
    return lookup


def load_trn_type_lookup():
    """Load TRN_TYPE into memory: ID -> TRN_TYPE_CATEGORY"""
    lookup = {}
    with open(TRN_TYPE_FILE, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            tid = row.get('ID', '').strip()
            cat = row.get('TRN_TYPE_CATEGORY', '').strip()
            if tid:
                try:
                    lookup[int(tid)] = int(cat) if cat else 0
                except ValueError:
                    pass
    print(f"Loaded {len(lookup)} transaction type records")
    return lookup


def parse_inp_date(inp_date_str: str):
    """Parse INP_DATE and return (date_str, hour). Returns None on failure."""
    s = inp_date_str.strip()
    if not s:
        return None
    # Strip milliseconds: "2025-03-10 08:23:45.000" → "2025-03-10 08:23:45"
    if '.' in s:
        s = s[:s.rindex('.')]
    for fmt in ('%Y-%m-%d %H:%M:%S', '%d.%m.%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M', '%d.%m.%Y %H:%M'):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime('%Y-%m-%d'), dt.hour
        except ValueError:
            continue
    return None


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=== Step 1: Load lookup tables ===")
    places = load_places_lookup()
    trn_types = load_trn_type_lookup()

    # Precompute set of cancellation IDs from TRN_TYPE
    cancel_ids = {tid for tid, cat in trn_types.items() if cat == CANCEL_CATEGORY}
    print(f"Cancellation TRN_TYPE_IDs: {sorted(cancel_ids)}")

    print(f"\n=== Step 2: Stream through PASS_ALL (~89M rows) ===")
    print(f"File: {PASS_FILE}")

    # Key: (date, hour, ST_CODE) -> entry_count
    counts = defaultdict(int)
    # Track station metadata: ST_CODE -> (ST_NAME, LN_NAME)
    station_meta = {}

    total_rows = 0
    skipped_transport = 0
    skipped_not_entry = 0
    skipped_test = 0
    skipped_cancel = 0
    skipped_exit_trn = 0
    skipped_no_place = 0
    skipped_parse = 0
    counted = 0

    file_size = os.path.getsize(PASS_FILE)
    print(f"File size: {file_size / 1e9:.1f} GB")

    with open(PASS_FILE, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter=';')

        for row in reader:
            total_rows += 1

            if total_rows % 5_000_000 == 0:
                print(f"  {total_rows:,} rows processed, counted: {counted:,}")
                sys.stdout.flush()

            # Filter 1: Only metro
            transport_type = row.get('TRANSPORT_TYPE_ID', '').strip()
            if transport_type != '1':
                skipped_transport += 1
                continue

            # Filter 2: Only entries (VALIDATION_MODE=1)
            val_mode = row.get('VALIDATION_MODE', '').strip()
            if val_mode != '1':
                skipped_not_entry += 1
                continue

            # Filter 3: Exclude cancellations by TRN_TYPE_ID
            tran_type_str = row.get('TRAN_TYPE_ID', '').strip()
            try:
                tran_type_id = int(tran_type_str)
            except ValueError:
                tran_type_id = 0

            if tran_type_id in cancel_ids:
                skipped_cancel += 1
                continue

            # Also exclude exit transaction types
            if tran_type_id in EXIT_TRN_TYPE_IDS:
                skipped_exit_trn += 1
                continue

            # Filter 4: Get place info (IS_TEST, ST_CODE, etc.)
            place_id = row.get('PLACE_ID', '').strip()
            if not place_id or place_id not in places:
                skipped_no_place += 1
                continue

            place_info = places[place_id]
            if place_info['IS_TEST'] == '1':
                skipped_test += 1
                continue

            st_code = place_info['ST_CODE']
            st_name = place_info['ST_NAME']
            ln_name = place_info['LN_NAME']

            if not st_code:
                skipped_no_place += 1
                continue

            # Filter 5: Parse INP_DATE
            inp_date = row.get('INP_DATE', '').strip()
            parsed = parse_inp_date(inp_date)
            if parsed is None:
                skipped_parse += 1
                continue

            date_str, hour = parsed

            # Count entry
            key = (date_str, hour, st_code)
            counts[key] += 1
            counted += 1

            # Store station metadata
            if st_code not in station_meta:
                station_meta[st_code] = (st_name, ln_name)

    print(f"\nProcessing complete:")
    print(f"  Total rows:        {total_rows:,}")
    print(f"  Counted (entries): {counted:,}")
    print(f"  Skipped transport: {skipped_transport:,}")
    print(f"  Skipped not-entry: {skipped_not_entry:,}")
    print(f"  Skipped test:      {skipped_test:,}")
    print(f"  Skipped cancel:    {skipped_cancel:,}")
    print(f"  Skipped exit-trn:  {skipped_exit_trn:,}")
    print(f"  Skipped no-place:  {skipped_no_place:,}")
    print(f"  Skipped bad-date:  {skipped_parse:,}")

    print(f"\n=== Step 3: Write output ===")
    fieldnames = ['date', 'hour', 'weekday', 'is_weekend', 'ST_CODE', 'ST_NAME', 'LN_NAME', 'entries']

    rows_out = []
    for (date_str, hour, st_code), cnt in sorted(counts.items()):
        st_name, ln_name = station_meta.get(st_code, ('', ''))
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            wd = dt.weekday()
            weekday = WEEKDAYS[wd]
            is_weekend = 1 if wd >= 5 else 0
        except ValueError:
            weekday = ''
            is_weekend = ''

        rows_out.append({
            'date': date_str,
            'hour': hour,
            'weekday': weekday,
            'is_weekend': is_weekend,
            'ST_CODE': st_code,
            'ST_NAME': st_name,
            'LN_NAME': ln_name,
            'entries': cnt,
        })

    with open(OUT_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Written {len(rows_out)} rows to {OUT_FILE}")
    print(f"Unique stations: {len(station_meta)}")
    print(f"Date range: {min(k[0] for k in counts.keys())} — {max(k[0] for k in counts.keys())}")


if __name__ == '__main__':
    main()
