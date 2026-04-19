"""
Модель перераспределения маршрутов НГПТ при закрытии ст. Тропарево.

Алгоритм:
1. Берём почасовой профиль спроса Тропарево (из troparyovo_hourly.csv)
2. Применяем данные о реальных маршрутах у станции (из ngpt_routes_troparyovo.csv)
3. Рассчитываем:
   - сколько пассажиров нужно вывезти за каждый час
   - какие маршруты уже есть и сколько они могут взять
   - сколько не хватает → рекомендация шаттла/усиления
4. Выводим рекомендации по каждому маршруту

Выход:
  analysis/output/redistribution_plan.csv
  analysis/output/redistribution_report.md
"""

import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent.parent
HOURLY_FILE  = ROOT / "analysis/output/troparyovo_hourly.csv"
ROUTES_FILE  = ROOT / "analysis/output/ngpt_routes_troparyovo.csv"
OUT_DIR      = ROOT / "analysis/output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── Нормативы ───────────────────────────────────────────────
# Вместимость подвижного состава (чел)
CAPACITY = {
    'автобус':     85,   # ЛиАЗ-5292 / ПАЗ-320415 (средний)
    'электробус':  85,   # КАМАЗ-6282 / ЛиАЗ-6274
    'трамвай':    190,   # 71-931М «Витязь-М» (3 секции)
    'шаттл':       50,   # укороченный/микроавтобус
}

# Текущие интервалы движения по времени суток (мин)
# (приблизительные нормативы для Москвы)
INTERVAL_CURRENT = {
    'утренний_пик':   8,   # 07–10
    'дневной':       12,   # 10–17
    'вечерний_пик':   8,   # 17–21
    'ранний':        15,   # 06–07
    'поздний':       15,   # 21–24
}

# Минимальный интервал при усилении (мин)
INTERVAL_REINFORCED = {
    'утренний_пик':   4,
    'дневной':        6,
    'вечерний_пик':   4,
    'ранний':         8,
    'поздний':        8,
}

def time_period(hour):
    if hour < 6:   return None
    if hour < 7:   return 'ранний'
    if hour < 10:  return 'утренний_пик'
    if hour < 17:  return 'дневной'
    if hour < 21:  return 'вечерний_пик'
    return 'поздний'

def vehicles_per_hour(interval_min):
    if not interval_min: return 0
    return 60 // interval_min

def capacity_per_hour(vehicle_count, cap_per_vehicle):
    return vehicle_count * cap_per_vehicle

# ─── Маршруты у Тропарево (из данных + дополнение из анализа) ─
# Берём топ-маршруты из обоих направлений; тип транспорта определён
# по REF_TRANSPORT_WAY, для неизвестных WAY_ID — автобус (из PASS_ALL)
ROUTES = [
    # {route_name, way_id, veh_type, direction, weekly_trips_to, weekly_trips_from}
    {'name': '229 автобус',        'way_id': '227',  'type': 'автобус',   'to': 2489, 'from': 2302},
    {'name': '283 автобус',        'way_id': '281',  'type': 'автобус',   'to': 1210, 'from': 1820},
    {'name': 'т6 автобус',         'way_id': '1000', 'type': 'автобус',   'to': 1168, 'from':    0},
    {'name': '645 автобус',        'way_id': '642',  'type': 'автобус',   'to':  761, 'from':  453},
    {'name': '111 автобус',        'way_id': '109',  'type': 'автобус',   'to':   45, 'from':  562},
    {'name': '191 автобус',        'way_id': '189',  'type': 'автобус',   'to':   55, 'from':  532},
    {'name': '677ф автобус',       'way_id': '894',  'type': 'автобус',   'to':    0, 'from':  407},
    {'name': '171 автобус',        'way_id': '169',  'type': 'автобус',   'to':    0, 'from':  304},
    {'name': '891К автобус',       'way_id': '1266', 'type': 'автобус',   'to':    0, 'from':  313},
    {'name': '1002 пригород МО',   'way_id': '543',  'type': 'автобус',   'to':    0, 'from':  302},
    {'name': '12 трамвай',         'way_id': '439',  'type': 'трамвай',   'to':    0, 'from':  365},
    {'name': '17 трамвай',         'way_id': '35',   'type': 'трамвай',   'to':    0, 'from':  228},
    {'name': '3 трамвай',          'way_id': '426',  'type': 'трамвай',   'to':    0, 'from':  225},
    # Неизвестные маршруты (автобусы, WAY_ID вне справочника — возможно электробусы)
    {'name': 'маршрут_6553',       'way_id': '6553', 'type': 'автобус',   'to': 5594, 'from':  223},
    {'name': 'маршрут_5010',       'way_id': '5010', 'type': 'автобус',   'to':  589, 'from': 2998},
    {'name': 'маршрут_6186',       'way_id': '6186', 'type': 'автобус',   'to':  885, 'from': 1825},
    {'name': 'маршрут_6890',       'way_id': '6890', 'type': 'автобус',   'to': 1442, 'from':  746},
    {'name': 'маршрут_6343',       'way_id': '6343', 'type': 'автобус',   'to': 1609, 'from':    0},
]

# Суммарные поездки за неделю для весовых коэффициентов
total_to   = sum(r['to']   for r in ROUTES)
total_from = sum(r['from'] for r in ROUTES)


def load_hourly_avg(day_type='будний'):
    """Средний профиль по часам."""
    rows_by_hour = defaultdict(list)
    with open(HOURLY_FILE, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['date'] == '2025-03-17':  # неполный день
                continue
            if day_type == 'будний' and row['is_weekend'] == '1':
                continue
            if day_type == 'выходной' and row['is_weekend'] == '0':
                continue
            rows_by_hour[int(row['hour'])].append(int(row['entries']))
    return {h: round(sum(v)/len(v)) for h, v in rows_by_hour.items()}


def simulate(hourly_demand, day_label):
    results = []
    for hour in range(6, 24):
        demand = hourly_demand.get(hour, 0)
        period = time_period(hour)
        if period is None or demand == 0:
            continue

        cur_interval  = INTERVAL_CURRENT[period]
        rein_interval = INTERVAL_REINFORCED[period]

        # Текущая суммарная ёмкость НГПТ у Тропарево (за 1 час)
        # Взвешиваем по доле маршрута в реальном пассажиропотоке
        cur_cap_total  = 0
        rein_cap_total = 0
        route_rows = []

        for r in ROUTES:
            weight = (r['to'] + r['from']) / max(total_to + total_from, 1)
            veh_type = r['type']
            cap = CAPACITY.get(veh_type, 85)

            cur_vph  = vehicles_per_hour(cur_interval)
            rein_vph = vehicles_per_hour(rein_interval)

            cur_cap_route  = round(cur_vph  * cap * weight)
            rein_cap_route = round(rein_vph * cap * weight)

            cur_cap_total  += cur_cap_route
            rein_cap_total += rein_cap_route

            route_rows.append({
                'route':       r['name'],
                'way_id':      r['way_id'],
                'type':        veh_type,
                'weight_pct':  round(weight * 100, 1),
                'cur_cap':     cur_cap_route,
                'rein_cap':    rein_cap_route,
            })

        deficit_cur  = max(0, demand - cur_cap_total)
        deficit_rein = max(0, demand - rein_cap_total)

        # Шаттл: если дефицит остаётся после усиления
        shuttle_buses_needed = 0
        if deficit_rein > 0:
            shuttle_cap_per_bus = CAPACITY['шаттл'] * (60 // 5)  # интервал 5 мин
            shuttle_buses_needed = -(-deficit_rein // shuttle_cap_per_bus)  # ceiling

        results.append({
            'day_type':          day_label,
            'hour':              hour,
            'period':            period,
            'demand':            demand,
            'cur_cap_ngpt':      cur_cap_total,
            'rein_cap_ngpt':     rein_cap_total,
            'deficit_cur':       deficit_cur,
            'deficit_rein':      deficit_rein,
            'shuttle_needed':    int(shuttle_buses_needed),
            'load_cur_pct':      round(demand / cur_cap_total * 100) if cur_cap_total else 999,
            'load_rein_pct':     round(demand / rein_cap_total * 100) if rein_cap_total else 999,
        })
    return results


def make_report(results_weekday, results_weekend):
    lines = [
        "# Модель перераспределения маршрутов НГПТ — закрытие ст. Тропарево",
        "",
        "_Данные: НБС 10–16 марта 2025. Тропарево, Сокольническая линия._",
        "",
        "## Реальные маршруты у станции",
        "",
        "Обнаружены трекингом карт: уникальных карт за 7 дней — **61 962**.",
        "",
        "| Маршрут | Тип | Поездок к метро/нед | Поездок от метро/нед | Итого |",
        "|---------|-----|--------------------|--------------------|-------|",
    ]
    for r in sorted(ROUTES, key=lambda x: -(x['to']+x['from'])):
        total = r['to'] + r['from']
        if total < 50: continue
        lines.append(f"| {r['name']} | {r['type']} | {r['to']:,} | {r['from']:,} | {total:,} |")

    lines += [
        "",
        "## Расчёт вместимости и дефицита",
        "",
        "> **Логика:** при закрытии Тропарево все ~20 700 чел/будни (входы) нужно",
        "> вывезти наземным транспортом к Юго-западной (сев.) или Румянцево (юж.).",
        "> Текущие маршруты → усиление → шаттл покрывают остаток.",
        "",
    ]

    for label, results in [("Типичный будний день", results_weekday),
                            ("Типичный выходной день", results_weekend)]:
        lines += [
            f"### {label}",
            "",
            "| Час | Спрос | НГПТ сейчас | НГПТ усилен | Дефицит (сейчас) | Дефицит (усилен) | Шаттл (авт.) |",
            "|-----|-------|------------|------------|-----------------|-----------------|-------------|",
        ]
        for r in results:
            flag = " ⚠" if r['deficit_rein'] > 0 else ""
            lines.append(
                f"| {r['hour']:02d}:00 | {r['demand']:,} | "
                f"{r['cur_cap_ngpt']:,} ({r['load_cur_pct']}%) | "
                f"{r['rein_cap_ngpt']:,} ({r['load_rein_pct']}%) | "
                f"{r['deficit_cur']:,} | "
                f"{r['deficit_rein']:,}{flag} | "
                f"{r['shuttle_needed']} |"
            )

        peak = max(results, key=lambda x: x['demand'])
        total_demand = sum(r['demand'] for r in results)
        total_deficit_cur  = sum(r['deficit_cur']  for r in results)
        total_deficit_rein = sum(r['deficit_rein'] for r in results)
        lines += [
            "",
            f"- Суммарный спрос: **{total_demand:,} чел** (06–23 ч)",
            f"- Дефицит без усиления: **{total_deficit_cur:,} чел** ({total_deficit_cur/total_demand*100:.1f}%)",
            f"- Дефицит после усиления: **{total_deficit_rein:,} чел** ({total_deficit_rein/total_demand*100:.1f}%)",
            f"- Пик: **{peak['hour']:02d}:00**, спрос {peak['demand']:,} чел/час",
            "",
        ]

    lines += [
        "## Рекомендации по перераспределению маршрутов",
        "",
        "### 1. Маршруты для перенаправления к Юго-западной (~50% потока)",
        "Маршруты, которые сейчас заходят к Тропарево из севера/центра:",
        "- **229, 283, т6, 645, маршрут_6553, 6343, 6890** — изменить конечную/промежуточную",
        "  остановку с «Тропарево» на «Юго-западная»",
        "- Увеличить частоту до интервала **4 мин** в час пик",
        "",
        "### 2. Маршруты для перенаправления к Румянцево (~30% потока)",
        "Маршруты, идущие на юг/Новая Москва:",
        "- **1002 пригород МО, 891К, маршрут_6186, 5010** — изменить начальную остановку",
        "  на «Румянцево» (ближайшая южная открытая станция)",
        "- Интервал до **6 мин** в межпик, **4 мин** в пик",
        "",
        "### 3. Шаттл Тропарево ↔ Юго-западная",
        "При пиковом дефиците: организовать экспресс-шаттл:",
        "- Маршрут: Тропарево (вход закрытой станции) → Юго-западная",
        "- Интервал: **3–4 мин** в утренний и вечерний пик",
        "- Вместимость: микроавтобусы 50 мест или стандартный автобус",
        "- Потребность: **2–4 единицы** в час пик",
        "",
        "### 4. Информирование пассажиров",
        "- Сотрудники у входа в закрытую станцию с картой альтернатив",
        "- Объявления в поездах: «Следующая — Тропарево, станция закрыта,",
        "  выходите на Юго-западной или Румянцево»",
        "- Push-уведомления в Яндекс.Транспорт и «Метро Москвы»",
        "",
        "## Ограничения модели",
        "",
        "- Веса маршрутов рассчитаны по фактическому трафику карт (НБС), **не** по",
        "  расписанию — не все пассажиры идентифицированы через карту",
        "- Маршруты с WAY_ID > 2981 не имеют названий в нашем справочнике",
        "  (возможно электробусы или новые маршруты)",
        "- Базовая загрузка маршрутов до закрытия **не учтена** — реальный дефицит",
        "  может быть выше из-за уже существующей заполненности автобусов",
        "- Пешеходная доступность (15 мин до Юго-западной) снизит часть спроса на НГПТ",
        "",
        "_Скрипт: `analysis/07_route_redistribution.py`_",
    ]
    return '\n'.join(lines)


def main():
    print("=== Модель перераспределения маршрутов — Тропарево ===\n")

    wd_hourly = load_hourly_avg('будний')
    we_hourly = load_hourly_avg('выходной')

    results_wd = simulate(wd_hourly, 'будний')
    results_we = simulate(we_hourly, 'выходной')

    # CSV
    out_csv = OUT_DIR / "redistribution_plan.csv"
    fields = ['day_type','hour','period','demand','cur_cap_ngpt','rein_cap_ngpt',
              'deficit_cur','deficit_rein','shuttle_needed','load_cur_pct','load_rein_pct']
    with open(out_csv, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results_wd + results_we)
    print(f"CSV → {out_csv}")

    # Markdown отчёт
    report = make_report(results_wd, results_we)
    out_md = OUT_DIR / "redistribution_report.md"
    with open(out_md, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"Report → {out_md}")

    # Консольный вывод
    print("\n=== Будний день — ключевые часы ===")
    print(f"{'Час':>5}  {'Спрос':>7}  {'НГПТ(тек)':>11}  {'НГПТ(усил)':>11}  {'Дефицит':>9}  {'Шаттл':>6}")
    for r in results_wd:
        flag = ' ⚠' if r['deficit_rein'] > 0 else ''
        print(f"{r['hour']:02d}:00  {r['demand']:7,}  "
              f"{r['cur_cap_ngpt']:>11,}  {r['rein_cap_ngpt']:>11,}  "
              f"{r['deficit_rein']:>9,}{flag}  {r['shuttle_needed']:>6}")


if __name__ == '__main__':
    main()
