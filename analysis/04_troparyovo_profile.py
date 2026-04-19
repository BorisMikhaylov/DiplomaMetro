"""
Профиль пассажиропотока по станции Тропарево (ST_CODE=1694).
Только входы в метро (TRANSPORT_TYPE_ID=1, VALIDATION_MODE=1).

Выходы:
  analysis/output/troparyovo_hourly.csv  — час × день
  analysis/output/troparyovo_summary.txt — текстовая сводка

Запуск: python3 analysis/04_troparyovo_profile.py
"""

import csv
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
PASS_FILE = ROOT / "data/pass_10-160324/PASS_ALL_202503242210.csv"
TRN_TYPE_FILE = ROOT / "data/pass_10-160324/TRN_TYPE_202503251753.csv"
OUT_DIR = ROOT / "analysis/output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Тропарево: два турникета
TARGET_PLACE_IDS = {'1625', '1626'}

WEEKDAYS_RU = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']


def load_cancel_ids():
    cancel = set()
    with open(TRN_TYPE_FILE, encoding='utf-8') as f:
        for row in csv.DictReader(f, delimiter=';'):
            if row.get('TRN_TYPE_CATEGORY', '').strip() == '19':
                try:
                    cancel.add(int(row['ID']))
                except ValueError:
                    pass
    return cancel


def parse_date_hour(s):
    s = s.strip()
    # Strip milliseconds if present: "2025-03-10 00:05:51.000" → "2025-03-10 00:05:51"
    if '.' in s:
        s = s[:s.rindex('.')]
    for fmt in ('%Y-%m-%d %H:%M:%S', '%d.%m.%Y %H:%M:%S', '%Y-%m-%d %H:%M', '%d.%m.%Y %H:%M'):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime('%Y-%m-%d'), dt.weekday(), dt.hour
        except ValueError:
            pass
    return None, None, None


def main():
    cancel_ids = load_cancel_ids()
    # Exit transaction types
    exit_ids = {70, 71}

    # counts[(date, hour)] = count
    counts = defaultdict(int)
    date_weekday = {}

    total = skipped = counted = 0
    print(f"Scanning PASS_ALL for Тропарево (PLACE_ID 1625/1626)...")
    with open(PASS_FILE, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            total += 1
            if total % 5_000_000 == 0:
                print(f"  {total:,} rows scanned, entries found: {counted}", flush=True)

            # Fast pre-filter before dict lookup
            if row.get('TRANSPORT_TYPE_ID') != '1':
                continue
            if row.get('VALIDATION_MODE') != '1':
                continue
            place_id = row.get('PLACE_ID', '').strip()
            if place_id not in TARGET_PLACE_IDS:
                continue

            # Check transaction type
            try:
                tran_id = int(row.get('TRAN_TYPE_ID', '0'))
            except ValueError:
                tran_id = 0
            if tran_id in cancel_ids or tran_id in exit_ids:
                skipped += 1
                continue

            date_str, wd, hour = parse_date_hour(row.get('INP_DATE', ''))
            if date_str is None:
                skipped += 1
                continue

            counts[(date_str, hour)] += 1
            date_weekday[date_str] = wd
            counted += 1

    print(f"\nDone. Total rows scanned: {total:,}")
    print(f"Entries at Тропарево: {counted}")
    print(f"Skipped (bad/cancel): {skipped}")

    # Write CSV
    out_csv = OUT_DIR / "troparyovo_hourly.csv"
    fields = ['date', 'weekday', 'is_weekend', 'hour', 'entries']
    rows = []
    for (date_str, hour), cnt in sorted(counts.items()):
        wd = date_weekday.get(date_str, 0)
        rows.append({
            'date': date_str,
            'weekday': WEEKDAYS_RU[wd],
            'is_weekend': 1 if wd >= 5 else 0,
            'hour': hour,
            'entries': cnt,
        })
    with open(out_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nSaved → {out_csv} ({len(rows)} rows)")

    # Summary
    total_entries = sum(counts.values())
    by_day = defaultdict(int)
    for (date_str, hour), cnt in counts.items():
        by_day[date_str] += cnt
    by_hour = defaultdict(int)
    for (date_str, hour), cnt in counts.items():
        by_hour[hour] += cnt

    peak_day = max(by_day, key=by_day.get) if by_day else '-'
    peak_hour = max(by_hour, key=by_hour.get) if by_hour else '-'

    summary = [
        "=== Тропарево — сводка пассажиропотока ===",
        f"Период: {min(d for d,h in counts)} — {max(d for d,h in counts)}",
        f"Всего входов: {total_entries:,}",
        f"Среднесуточно: {total_entries // max(len(by_day),1):,}",
        "",
        "По дням:",
    ]
    for d in sorted(by_day):
        wd = WEEKDAYS_RU[date_weekday.get(d, 0)]
        summary.append(f"  {d} ({wd}): {by_day[d]:,}")

    summary += [
        "",
        "По часам (сумма за все дни):",
    ]
    for h in range(24):
        bar = '#' * (by_hour[h] // max(max(by_hour.values(), default=1) // 30, 1))
        summary.append(f"  {h:02d}:00  {by_hour[h]:6,}  {bar}")

    summary_text = '\n'.join(summary)
    print('\n' + summary_text)

    out_txt = OUT_DIR / "troparyovo_summary.txt"
    with open(out_txt, 'w', encoding='utf-8') as f:
        f.write(summary_text + '\n')
    print(f"\nSaved → {out_txt}")


if __name__ == '__main__':
    main()
