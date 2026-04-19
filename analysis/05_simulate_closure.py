"""
Прототип модели закрытия станции Тропарево.

Логика:
1. Загружает почасовой профиль спроса из troparyovo_hourly.csv
2. Распределяет пассажиров по альтернативам:
   - метро: соседние станции Юго-западная (сев.) и Румянцево (юж.)
   - НГПТ: автобусные маршруты в радиусе ~500м
3. Оценивает перегруз с учётом вместимости
4. Выводит рекомендации по усилению

Выход:
  analysis/output/closure_simulation.csv  — почасовой дефицит/профицит
  analysis/output/closure_report.md       — итоговый отчёт

Запуск: python3 analysis/05_simulate_closure.py
"""

import csv
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent.parent
HOURLY_FILE = ROOT / "analysis/output/troparyovo_hourly.csv"
OUT_DIR = ROOT / "analysis/output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# 1. ГРАФ МЕТРО — Сокольническая линия, южный участок
# ─────────────────────────────────────────────
# Порядок станций (юг → север): Потапово → Новомосковская → Ольховая →
# Прокшино → Филатов луг → Саларьево → Румянцево → ТРОПАРЕВО →
# Юго-западная → Проспект Вернадского → Университет → ...

METRO_NEIGHBORS = {
    'Тропарево': {
        'north': {'station': 'Юго-западная', 'travel_time_min': 2, 'direction': 'в центр'},
        'south': {'station': 'Румянцево',    'travel_time_min': 2, 'direction': 'из центра'},
    }
}

# ─────────────────────────────────────────────
# 2. ВМЕСТИМОСТЬ ПОДВИЖНОГО СОСТАВА
# ─────────────────────────────────────────────
# Поезд 81-765/766/767 «Москва» (основной состав ММ):
#   вместимость 10 вагонов: ~2000 чел (1200 сидя + 800 стоя нормативно,
#   реально в час пик до ~2500)
# Используем нормативную: 2000 чел/состав

METRO_TRAIN_CAPACITY = 2000   # чел на поезд

# Интервалы движения (в минутах) по Сокольнической линии:
#   час пик (7-9, 17-20): ~2 мин
#   межпиковое: ~3–4 мин
#   ночь/ранее утро: 5–8 мин (N/A — метро закрыто)
METRO_INTERVALS = {
    range(0, 6):   None,   # метро закрыто
    range(6, 7):   8,
    range(7, 10):  2,
    range(10, 17): 3,
    range(17, 21): 2,
    range(21, 24): 4,
}

def get_interval(hour):
    for r, interval in METRO_INTERVALS.items():
        if hour in r:
            return interval
    return None

def trains_per_hour(hour):
    """Число поездов в час на соседнюю станцию."""
    interval = get_interval(hour)
    if interval is None:
        return 0
    return 60 // interval

def metro_capacity_per_hour(hour):
    """Провозная способность через соседнюю станцию, чел/час."""
    return trains_per_hour(hour) * METRO_TRAIN_CAPACITY

# ─────────────────────────────────────────────
# 3. НГПТ В РАЙОНЕ ТРОПАРЕВО (~500м)
# ─────────────────────────────────────────────
# Источник: ручная выборка по Яндекс.Транспорту / схемам ДТиРИ
# Маршруты, имеющие остановку у ст. Тропарево или в пешей доступности:
#   - 144 (автобус, ходит к Юго-западной и далее)
#   - 205 (автобус)
#   - 830к (автобус, экспресс)
#   - н16  (ночной автобус)
# Вместимость: ЛиАЗ-5292 ~110 чел, электробус КАМАЗ-6282 ~85 чел
# Интервал в час пик: ~6–10 мин, межпиковое: ~12–15 мин

NGPT_ROUTES_NEAR_TROPARYOVO = [
    {'route': '144',  'vehicle': 'автобус', 'capacity': 110, 'interval_peak': 8,  'interval_off': 15, 'direction': 'Юго-западная / Тропарёво район'},
    {'route': '205',  'vehicle': 'автобус', 'capacity': 110, 'interval_peak': 10, 'interval_off': 15, 'direction': 'Тропарёво / ул. Обручева'},
    {'route': '830к', 'vehicle': 'автобус', 'capacity': 90,  'interval_peak': 12, 'interval_off': 20, 'direction': 'Юго-западная / Бутово'},
]

def ngpt_capacity_per_hour(hour, use_peak_interval=True):
    """
    Суммарная провозная способность НГПТ у Тропарево, чел/час.
    При закрытии станции предполагаем усиление (интервалы в пик).
    """
    total = 0
    for r in NGPT_ROUTES_NEAR_TROPARYOVO:
        interval = r['interval_peak'] if (7 <= hour <= 9 or 17 <= hour <= 20) else r['interval_off']
        vehicles_per_hour = 60 // interval
        total += vehicles_per_hour * r['capacity']
    return total

# ─────────────────────────────────────────────
# 4. МОДЕЛЬ РАСПРЕДЕЛЕНИЯ
# ─────────────────────────────────────────────
# Предположения о поведении пассажиров при закрытии:
#   - 50% идут на Юго-западную (пешком ~15 мин или автобус 144/830к)
#   - 30% идут на Румянцево (пешком ~15 мин)
#   - 20% используют НГПТ (автобус до Юго-западной, продолжают маршрут)
# Эти доли — начальные предположения, можно менять параметрически.

SHARE_YUZHNAYA  = 0.50   # доля к Юго-западной
SHARE_RUMYANTSEVO = 0.30  # доля к Румянцево
SHARE_NGPT      = 0.20   # доля на НГПТ

# Коэффициент «осведомлённости»: в первый час после закрытия лишь 40% пассажиров
# знают о закрытии и идут к альтернативам сразу; остальные сначала приходят на
# закрытую станцию и тратят дополнительно ~10–15 минут.
# Для упрощения считаем, что все пассажиры перераспределяются, но с
# повышающим коэффициентом спроса в первые 2 часа (1.15 — задержка, ожидание).
AWARENESS_PENALTY = {0: 1.15, 1: 1.10}  # первые 2 часа после закрытия

def simulate_closure(hourly_data, closed_hour_start=0, closed_hour_end=24):
    """
    Симулирует закрытие Тропарево на весь день (или диапазон часов).

    hourly_data: list of dicts {weekday, hour, entries}
    Возвращает: list of dicts с распределением по альтернативам и дефицитом.
    """
    results = []
    for row in hourly_data:
        hour = int(row['hour'])
        demand = int(row['entries'])
        weekday = row['weekday']
        is_weekend = int(row['is_weekend'])

        if not (closed_hour_start <= hour < closed_hour_end):
            results.append({**row, 'status': 'open', 'deficit': 0})
            continue

        # Штраф осведомлённости
        h_offset = hour - closed_hour_start
        penalty = AWARENESS_PENALTY.get(h_offset, 1.0)
        effective_demand = round(demand * penalty)

        # Распределение
        to_yuzhnaya   = round(effective_demand * SHARE_YUZHNAYA)
        to_rumyantsevo = round(effective_demand * SHARE_RUMYANTSEVO)
        to_ngpt        = round(effective_demand * SHARE_NGPT)

        # Вместимость альтернатив
        cap_yuzhnaya    = metro_capacity_per_hour(hour)  # провозная способность через Юго-западную
        cap_rumyantsevo = metro_capacity_per_hour(hour)  # то же для Румянцево
        cap_ngpt        = ngpt_capacity_per_hour(hour)

        # Дефицит по каждому направлению (0 если вместимость достаточна)
        def_yuzhnaya    = max(0, to_yuzhnaya   - cap_yuzhnaya)
        def_rumyantsevo = max(0, to_rumyantsevo - cap_rumyantsevo)
        def_ngpt        = max(0, to_ngpt        - cap_ngpt)
        total_deficit   = def_yuzhnaya + def_rumyantsevo + def_ngpt

        # Уровень загрузки (%)
        load_yuzhnaya    = round(to_yuzhnaya   / cap_yuzhnaya * 100)   if cap_yuzhnaya    > 0 else 999
        load_rumyantsevo = round(to_rumyantsevo / cap_rumyantsevo * 100) if cap_rumyantsevo > 0 else 999
        load_ngpt        = round(to_ngpt        / cap_ngpt * 100)        if cap_ngpt        > 0 else 999

        results.append({
            'weekday':             weekday,
            'is_weekend':          is_weekend,
            'hour':                hour,
            'demand_original':     demand,
            'demand_effective':    effective_demand,
            'to_yuzhnaya':         to_yuzhnaya,
            'to_rumyantsevo':      to_rumyantsevo,
            'to_ngpt':             to_ngpt,
            'cap_yuzhnaya':        cap_yuzhnaya,
            'cap_rumyantsevo':     cap_rumyantsevo,
            'cap_ngpt':            cap_ngpt,
            'load_yuzhnaya_pct':   load_yuzhnaya,
            'load_rumyantsevo_pct': load_rumyantsevo,
            'load_ngpt_pct':       load_ngpt,
            'deficit_yuzhnaya':    def_yuzhnaya,
            'deficit_rumyantsevo': def_rumyantsevo,
            'deficit_ngpt':        def_ngpt,
            'total_deficit':       total_deficit,
            'status':              'closed',
        })
    return results


def load_hourly_data(day_filter=None):
    """
    Загружает данные из troparyovo_hourly.csv.
    Если day_filter задан — берёт только этот тип дня ('будний'/'выходной').
    Усредняет по дням того же типа.
    """
    by_day_hour = defaultdict(list)
    rows = []
    with open(HOURLY_FILE, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            rows.append(row)

    # Сгруппировать по часу, усреднить по дням одного типа
    if day_filter == 'будний':
        rows = [r for r in rows if r['is_weekend'] == '0' and r['date'] != '2025-03-17']
        group_label = 'Типичный будний день'
    elif day_filter == 'выходной':
        rows = [r for r in rows if r['is_weekend'] == '1']
        group_label = 'Типичный выходной день'
    else:
        rows = [r for r in rows if r['date'] != '2025-03-17']
        group_label = 'Все дни'

    # Среднее по часу
    hour_sum = defaultdict(int)
    hour_count = defaultdict(set)
    hour_meta = {}
    for row in rows:
        h = int(row['hour'])
        hour_sum[h] += int(row['entries'])
        hour_count[h].add(row['date'])
        hour_meta[h] = {'weekday': row['weekday'], 'is_weekend': row['is_weekend']}

    result = []
    for h in range(24):
        n_days = len(hour_count[h]) or 1
        avg = round(hour_sum[h] / n_days)
        meta = hour_meta.get(h, {'weekday': '', 'is_weekend': '0'})
        result.append({'hour': h, 'entries': avg,
                       'weekday': group_label, 'is_weekend': meta['is_weekend']})
    return result


def print_and_save_report(results_weekday, results_weekend):
    lines = [
        "# Симуляция закрытия станции Тропарево",
        "",
        "**Станция:** Тропарево, Сокольническая линия",
        "**Сценарий:** полное закрытие на весь день",
        "**Дата отчёта:** 2026-04-18",
        "",
        "## Допущения модели",
        "",
        "| Параметр | Значение |",
        "|---------|---------|",
        f"| Вместимость поезда | {METRO_TRAIN_CAPACITY} чел |",
        "| Интервал в час пик | 2 мин → 30 поездов/час |",
        "| Интервал межпиковый | 3 мин → 20 поездов/час |",
        f"| Доля к Юго-западной | {SHARE_YUZHNAYA*100:.0f}% |",
        f"| Доля к Румянцево | {SHARE_RUMYANTSEVO*100:.0f}% |",
        f"| Доля на НГПТ | {SHARE_NGPT*100:.0f}% |",
        "| Штраф за неосведомлённость (1-й час) | +15% к спросу |",
        "",
        "## Маршруты НГПТ у Тропарево",
        "",
        "| Маршрут | Тип | Вместимость | Интервал (пик/межпик) |",
        "|---------|-----|------------|----------------------|",
    ]
    for r in NGPT_ROUTES_NEAR_TROPARYOVO:
        lines.append(f"| {r['route']} | {r['vehicle']} | {r['capacity']} | {r['interval_peak']}/{r['interval_off']} мин |")

    for label, results in [("Типичный будний день", results_weekday),
                            ("Типичный выходной день", results_weekend)]:
        lines += ["", f"## {label}", ""]
        lines += ["| Час | Спрос | →Юго-зап | →Румянц | →НГПТ | Дефицит | Загрузка Ю/Р/Н |",
                  "|-----|-------|---------|---------|-------|---------|----------------|"]
        for r in results:
            if r.get('status') == 'open':
                continue
            load_str = f"{r['load_yuzhnaya_pct']}% / {r['load_rumyantsevo_pct']}% / {r['load_ngpt_pct']}%"
            flag = " ⚠" if r['total_deficit'] > 0 else ""
            lines.append(
                f"| {r['hour']:02d}:00 | {r['demand_effective']:,} | {r['to_yuzhnaya']:,} | "
                f"{r['to_rumyantsevo']:,} | {r['to_ngpt']:,} | {r['total_deficit']:,}{flag} | {load_str} |"
            )

        total_demand = sum(r['demand_original'] for r in results if r.get('status') == 'closed')
        total_deficit = sum(r['total_deficit'] for r in results if r.get('status') == 'closed')
        peak_deficit = max((r['total_deficit'] for r in results if r.get('status') == 'closed'), default=0)
        peak_hour = next((r['hour'] for r in results if r.get('total_deficit') == peak_deficit and r.get('status') == 'closed'), '-')

        lines += [
            "",
            f"**Суммарный суточный спрос:** {total_demand:,} чел",
            f"**Суммарный дефицит:** {total_deficit:,} чел ({total_deficit/total_demand*100:.1f}%)" if total_demand else "",
            f"**Пиковый дефицит:** {peak_deficit:,} чел/час в {peak_hour}:00",
        ]

    lines += [
        "",
        "## Рекомендации",
        "",
        "### Метро",
        "- Усилить движение на участке Юго-западная–Саларьево до минимального интервала 1.5 мин",
        "  (возможно при 40 составах в парке)",
        "- Организовать объявления о закрытии на соседних станциях",
        "",
        "### НГПТ (наземный транспорт)",
        "- Выставить шаттл Тропарево ↔ Юго-западная с интервалом 3–4 мин в час пик",
        "  (нужно: ~8–10 дополнительных автобусов)",
        "- Усилить маршруты 144, 205 до интервала 5 мин в утренний пик",
        "- Организовать дополнительную посадочную площадку у входа в метро",
        "",
        "### Пассажирская навигация",
        "- Сотрудники у закрытых турникетов с картами объезда",
        "- Отправить push-уведомление в приложение «Метро Москвы» и Яндекс.Транспорт",
        "- Объявление в поездах за 2 станции до Тропарево",
        "",
        "## Ограничения модели",
        "",
        "- Доли распределения (50/30/20%) — экспертные допущения, не откалиброваны по данным",
        "- Вместимость НГПТ приблизительная (нет точного GTFS для района)",
        "- Не учтён эффект перегрузки на Юго-западной/Румянцево (нет данных о базовой нагрузке)",
        "- Не учтены пассажиры МЦД/МЦК (возможная альтернатива через Юго-западную)",
        "",
        "_Данные: НБС 10–16 марта 2025. Скрипт: `analysis/05_simulate_closure.py`_",
    ]

    report_text = '\n'.join(lines)
    out = OUT_DIR / "closure_report.md"
    with open(out, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"Report → {out}")
    return report_text


def main():
    print("=== Симуляция закрытия Тропарево ===\n")

    # Данные: средний будний и выходной день
    weekday_data = load_hourly_data('будний')
    weekend_data = load_hourly_data('выходной')

    print("Средний будний день (входы/час):")
    for r in weekday_data:
        bar = '█' * (r['entries'] // 300)
        print(f"  {r['hour']:02d}:00  {r['entries']:5,}  {bar}")

    print("\nЗапускаю симуляцию (закрытие весь день)...")
    results_weekday = simulate_closure(weekday_data)
    results_weekend = simulate_closure(weekend_data)

    # Сохранить CSV (будни)
    fields = [
        'weekday', 'is_weekend', 'hour', 'demand_original', 'demand_effective',
        'to_yuzhnaya', 'to_rumyantsevo', 'to_ngpt',
        'cap_yuzhnaya', 'cap_rumyantsevo', 'cap_ngpt',
        'load_yuzhnaya_pct', 'load_rumyantsevo_pct', 'load_ngpt_pct',
        'deficit_yuzhnaya', 'deficit_rumyantsevo', 'deficit_ngpt', 'total_deficit',
    ]
    out_csv = OUT_DIR / "closure_simulation.csv"
    with open(out_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(r for r in results_weekday + results_weekend if r.get('status') == 'closed')
    print(f"CSV → {out_csv}")

    # Отчёт
    report = print_and_save_report(results_weekday, results_weekend)

    # Вывод ключевых цифр
    print("\n=== Ключевые результаты (будний день) ===")
    total_demand  = sum(r['demand_original'] for r in results_weekday if r.get('status') == 'closed')
    total_deficit = sum(r['total_deficit'] for r in results_weekday if r.get('status') == 'closed')
    print(f"Суточный спрос:  {total_demand:,} чел")
    print(f"Суммарный дефицит: {total_deficit:,} чел ({total_deficit/total_demand*100:.1f}%)" if total_demand else "")
    print("\nЧасы с дефицитом:")
    for r in results_weekday:
        if r.get('total_deficit', 0) > 0:
            print(f"  {r['hour']:02d}:00  спрос={r['demand_effective']:,}  дефицит={r['total_deficit']:,}")


if __name__ == '__main__':
    main()
