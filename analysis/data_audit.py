"""
data_audit.py — первичный аудит данных проекта диплома.
Запуск: python3 analysis/data_audit.py
Зависимости: только стандартная библиотека Python 3.
"""

import csv
import collections
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'pass_10-160324')

def analyze_pass_all(sample_rows=2_000_000):
    path = os.path.join(DATA_DIR, 'PASS_ALL_202503242210.csv')
    hours = collections.Counter()
    transport_cnt = collections.Counter()
    tran_type_cnt = collections.Counter()
    place_ids = set()
    route_codes = set()
    null_counts = collections.defaultdict(int)
    n = 0

    with open(path, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            n += 1
            if n > sample_rows:
                break
            transport_cnt[row['TRANSPORT_TYPE_ID']] += 1
            tran_type_cnt[row['TRAN_TYPE_ID']] += 1
            if row['PLACE_ID']:
                place_ids.add(row['PLACE_ID'])
            if row['ROUTE_CODE']:
                route_codes.add(row['ROUTE_CODE'])
            if row['INP_DATE']:
                try:
                    hr = int(row['INP_DATE'][11:13])
                    hours[hr] += 1
                except Exception:
                    pass
            for k, v in row.items():
                if not v or v.strip() == '':
                    null_counts[k] += 1

    print(f"\n=== PASS_ALL (sample {n} rows) ===")
    print(f"Unique PLACE_IDs: {len(place_ids)}")
    print(f"Unique ROUTE_CODEs: {len(route_codes)}")
    print(f"TRANSPORT_TYPE_ID: {dict(transport_cnt.most_common())}")
    print(f"TRAN_TYPE_ID top-5: {dict(tran_type_cnt.most_common(5))}")
    print("Null % per column (>5%):")
    for col, cnt in sorted(null_counts.items(), key=lambda x: -x[1]):
        pct = cnt / n * 100
        if pct > 5:
            print(f"  {col}: {pct:.1f}%")
    print("Hourly distribution:")
    for h in sorted(hours.keys()):
        print(f"  {h:02d}h: {hours[h]}")


def analyze_ref_places():
    path = os.path.join(DATA_DIR, 'REF_PSG_PLACES_202503251822.csv')
    with open(path, encoding='utf-8') as f:
        rows = list(csv.DictReader(f, delimiter=';'))
    print(f"\n=== REF_PSG_PLACES ({len(rows)} rows) ===")
    type_cnt = collections.Counter(r['TYPE_ID'] for r in rows)
    print(f"TYPE_ID dist: {dict(type_cnt.most_common())}")
    ln_cnt = collections.Counter(r['LN_NAME'] for r in rows if r['LN_NAME'])
    print("Top line names:")
    for ln, cnt in ln_cnt.most_common(20):
        print(f"  {cnt:4d}  {ln}")


def analyze_routes():
    path = os.path.join(DATA_DIR, 'REF_TRANSPORT_WAY_202503251803.csv')
    with open(path, encoding='utf-8') as f:
        rows = list(csv.DictReader(f, delimiter=';'))
    by_t = collections.Counter(r['TRANSPORT_ID'] for r in rows)
    print(f"\n=== REF_TRANSPORT_WAY ({len(rows)} rows) ===")
    print(f"Routes by transport type: {dict(by_t)}")


if __name__ == '__main__':
    analyze_ref_places()
    analyze_routes()
    analyze_pass_all(sample_rows=500_000)
    print("\nDone.")
