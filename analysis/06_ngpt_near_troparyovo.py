"""
Анализ маршрутов НГПТ у Тропарево.

Два направления:
  A) НГПТ → Тропарево (метро): пассажиры, которые приехали на автобусе к метро
     → это маршруты, которые нужно усилить/перенаправить при закрытии
  B) Тропарево (метро) → НГПТ: пассажиры, которые вышли из метро и сели на автобус
     → тоже нужно сохранить связность

Подход: отслеживаем карты. Два прохода по файлу:
  1. Собираем времена транзакций на Тропарево-метро по картам
  2. Для каждой НГПТ-транзакции проверяем, была ли эта карта в Тропарево ±60 мин

Выход: analysis/output/ngpt_routes_troparyovo.csv
"""

import csv
import sys
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
PASS_FILE = ROOT / "data/pass_10-160324/PASS_ALL_202503242210.csv"
WAYS_FILE = ROOT / "data/pass_10-160324/REF_TRANSPORT_WAY_202503251803.csv"
OUT_DIR = ROOT / "analysis/output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

METRO_PLACE_IDS = {'1625', '1626'}
TRANSFER_WINDOW = timedelta(minutes=60)

TRANSPORT_NAMES = {
    '1': 'Метро', '2': 'Автобус', '3': 'Трамвай',
    '4': 'Троллейбус', '5': 'Электробус', '6': 'МЦД',
}


def parse_ts(s):
    s = s.strip()
    if '.' in s:
        s = s[:s.rindex('.')]
    for fmt in ('%Y-%m-%d %H:%M:%S', '%d.%m.%Y %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def load_way_names():
    ways = {}
    with open(WAYS_FILE, encoding='utf-8') as f:
        for row in csv.DictReader(f, delimiter=';'):
            ways[row['WAY_ID']] = {
                'name': row.get('NAME', '').strip(),
                'code': row.get('CODE', '').strip(),
            }
    return ways


def main():
    print("=== Шаг 1: Собираем время транзакций на Тропарево-метро по картам ===")
    # card_key -> list of (datetime, validation_mode)
    troparyovo_times = defaultdict(list)
    total = 0

    with open(PASS_FILE, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            total += 1
            if total % 10_000_000 == 0:
                print(f"  {total:,} строк, карт у Тропарево: {len(troparyovo_times):,}", flush=True)

            if row.get('PLACE_ID', '').strip() not in METRO_PLACE_IDS:
                continue
            if row.get('TRANSPORT_TYPE_ID') != '1':
                continue

            card = row.get('CRD_SERIAL_NO', '').strip() or row.get('CRD_PAN_HASH', '').strip()
            if not card or card == "''":
                continue

            ts = parse_ts(row.get('INP_DATE', ''))
            if ts is None:
                continue

            mode = row.get('VALIDATION_MODE', '').strip()  # 1=вход, 2=выход
            troparyovo_times[card].append((ts, mode))

    print(f"Всего строк: {total:,}")
    print(f"Уникальных карт у Тропарево: {len(troparyovo_times):,}")

    print("\n=== Шаг 2: Ищем НГПТ-транзакции связанных карт ±60 мин от Тропарево ===")
    # Направление A: НГПТ до метро (автобус → Тропарево-метро)
    # Направление B: НГПТ после метро (Тропарево-метро → автобус)
    routes_a = Counter()  # до метро
    routes_b = Counter()  # после метро
    routes_a_by_hour = defaultdict(Counter)  # hour -> Counter(route)
    routes_b_by_hour = defaultdict(Counter)

    processed = 0
    with open(PASS_FILE, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            processed += 1
            if processed % 10_000_000 == 0:
                print(f"  {processed:,} строк", flush=True)

            transport = row.get('TRANSPORT_TYPE_ID', '').strip()
            if transport not in ('2', '3', '4', '5'):
                continue

            card = row.get('CRD_SERIAL_NO', '').strip() or row.get('CRD_PAN_HASH', '').strip()
            if not card or card == "''" or card not in troparyovo_times:
                continue

            bus_rt = row.get('BUS_RT_NO', '').strip()
            if not bus_rt:
                continue

            ts = parse_ts(row.get('INP_DATE', ''))
            if ts is None:
                continue

            # Проверяем, что эта НГПТ-транзакция близка по времени к Тропарево
            for metro_ts, mode in troparyovo_times[card]:
                delta = ts - metro_ts
                if timedelta(minutes=-60) <= delta <= timedelta(0):
                    # НГПТ ПЕРЕД Тропарево: автобус → метро
                    routes_a[bus_rt] += 1
                    routes_a_by_hour[metro_ts.hour][bus_rt] += 1
                    break
                elif timedelta(0) < delta <= TRANSFER_WINDOW:
                    # НГПТ ПОСЛЕ Тропарево: метро → автобус
                    routes_b[bus_rt] += 1
                    routes_b_by_hour[metro_ts.hour][bus_rt] += 1
                    break

    print(f"\nНГПТ до метро (→Тропарево):   {sum(routes_a.values()):,} транзакций")
    print(f"НГПТ после метро (Тропарево→): {sum(routes_b.values()):,} транзакций")

    print("\n=== Шаг 3: Итоги ===")
    ways = load_way_names()

    print("\n[A] Маршруты, привозящие к Тропарево (нужно усилить/перенаправить):")
    rows_out = []
    for way_id, cnt in routes_a.most_common(20):
        w = ways.get(way_id, {})
        name = w.get('name', '—')
        code = w.get('code', '—')
        print(f"  {name:30s}  WAY_ID={way_id:6s}  {cnt:4d} поездок")
        rows_out.append({'direction': 'A_to_metro', 'way_id': way_id,
                         'route_name': name, 'code': code, 'trips': cnt})

    print("\n[B] Маршруты, увозящие от Тропарево (нужно сохранить частоту):")
    for way_id, cnt in routes_b.most_common(20):
        w = ways.get(way_id, {})
        name = w.get('name', '—')
        code = w.get('code', '—')
        print(f"  {name:30s}  WAY_ID={way_id:6s}  {cnt:4d} поездок")
        rows_out.append({'direction': 'B_from_metro', 'way_id': way_id,
                         'route_name': name, 'code': code, 'trips': cnt})

    # Сохранить
    out_csv = OUT_DIR / "ngpt_routes_troparyovo.csv"
    with open(out_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['direction', 'way_id', 'route_name', 'code', 'trips'])
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"\nСохранено → {out_csv}")

    # Почасовой профиль топ-5 маршрутов «А»
    top5_a = [w for w, _ in routes_a.most_common(5)]
    print("\n[A] Почасовой профиль топ-5 маршрутов (привоз к Тропарево):")
    print(f"  {'Час':>5}", end='')
    for w in top5_a:
        print(f"  {ways.get(w,{}).get('name','?'):>12}", end='')
    print()
    for h in range(6, 24):
        print(f"  {h:02d}:00", end='')
        for w in top5_a:
            cnt = routes_a_by_hour[h].get(w, 0)
            print(f"  {cnt:12d}", end='')
        print()


if __name__ == '__main__':
    main()
