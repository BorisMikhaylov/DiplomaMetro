"""
Задача: получить координаты станций московского метро из Overpass API (OSM).
Если Overpass недоступен — использует Nominatim (1 запрос/сек).

Выходной файл: data/external/metro_stations_coords.csv
Поля: ST_CODE, ST_NAME, LN_NAME, lat, lon, osm_name, source, match_quality

Запуск: python3 analysis/02_get_coordinates.py
"""

import csv
import json
import re
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent.parent
PLACES_FILE = ROOT / "data/pass_10-160324/REF_PSG_PLACES_202503251822.csv"
OUT_DIR = ROOT / "data/external"
OUT_FILE = OUT_DIR / "metro_stations_coords.csv"
OUT_UNMATCHED = OUT_DIR / "metro_stations_unmatched.csv"

OUT_DIR.mkdir(parents=True, exist_ok=True)

OVERPASS_QUERY_BBOX = (
    "[out:json][timeout:30];"
    "node(55.40,36.80,56.10,38.20)"
    "[\"railway\"=\"station\"][\"station\"=\"subway\"];"
    "out body;"
)

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

# Metro/rail line names to include (exclude NGPT, suburban rail, test)
METRO_LINE_KEYWORDS = [
    'линия', 'мцк', 'кольцевая', 'диаметр', 'миниметро', 'монорельс',
]
EXCLUDE_LINE_KEYWORDS = [
    'нгпт', 'наземный', 'направление', 'пассажирские агентства',
    'авиакассы', 'трамвайчики', 'пмт', 'цотт', 'стендовые',
]


def normalize_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r'\(.*?\)', '', name).strip()
    replacements = [
        ('пл.', 'площадь'), ('пр-т', 'проспект'), ('пр.', 'проспект'),
        ('ул.', 'улица'), ('б-р', 'бульвар'), ('им.', 'имени'),
        ('ш.', 'шоссе'), ('—', '-'), ('–', '-'),
    ]
    for abbr, full in replacements:
        name = name.replace(abbr, full)
    return re.sub(r'\s+', ' ', name).strip()


def is_metro_line(ln_name: str) -> bool:
    ln = ln_name.lower()
    if any(kw in ln for kw in EXCLUDE_LINE_KEYWORDS):
        return False
    if not ln:
        return False
    return True


def load_metro_stations():
    """Load unique metro stations from REF_PSG_PLACES."""
    stations = {}
    with open(PLACES_FILE, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            if row.get('IS_TEST') == '1':
                continue
            type_id = row.get('TYPE_ID', '')
            # TYPE_ID=1: metro turnstile, 8: other, 15: virtual/rail
            if type_id not in ('1', '8', '15'):
                continue
            st_code = row.get('ST_CODE', '').strip()
            st_name = row.get('ST_NAME', '').strip()
            ln_name = row.get('LN_NAME', '').strip()
            if not st_code or not st_name:
                continue
            if not is_metro_line(ln_name):
                continue
            if st_code not in stations:
                stations[st_code] = {
                    'ST_CODE': st_code,
                    'ST_NAME': st_name,
                    'LN_NAME': ln_name,
                    'LN_CODE': row.get('LN_CODE', ''),
                }
    print(f"Metro/rail stations in NBS: {len(stations)}")
    return stations


def fetch_overpass():
    """Try Overpass API endpoints with bbox query."""
    data = urllib.parse.urlencode({'data': OVERPASS_QUERY_BBOX}).encode('utf-8')
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            print(f"  Trying {endpoint}...")
            req = urllib.request.Request(
                endpoint, data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded',
                         'User-Agent': 'DiplomaResearch/1.0'},
            )
            with urllib.request.urlopen(req, timeout=45) as resp:
                result = json.loads(resp.read().decode('utf-8'))
                n = len(result.get('elements', []))
                if n > 0:
                    print(f"  Got {n} elements")
                    return result
                print(f"  Got 0 elements — skipping")
        except Exception as e:
            print(f"  Failed: {e}")
        time.sleep(2)
    return None


def geocode_nominatim(name: str, ln_name: str) -> dict:
    """Query Nominatim for a single station. Returns dict with lat/lon or None."""
    # Try with explicit metro context first
    query = f"метро {name} Москва"
    url = (
        "https://nominatim.openstreetmap.org/search?"
        + urllib.parse.urlencode({
            'q': query,
            'format': 'json',
            'limit': '10',
            'countrycodes': 'ru',
            'addressdetails': '0',
        })
    )
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'DiplomaResearch/1.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            results = json.loads(resp.read().decode('utf-8'))
        # Prefer railway/station results
        for r in results:
            cls = r.get('class', '')
            typ = r.get('type', '')
            if cls == 'railway' and typ in ('station', 'halt', 'subway_entrance'):
                return {'lat': float(r['lat']), 'lon': float(r['lon']),
                        'osm_name': r.get('display_name', '')[:80], 'source': 'nominatim_railway'}
        # Fallback: first result with "метро" in display name
        for r in results:
            dn = r.get('display_name', '').lower()
            if 'метро' in dn or 'metro' in dn:
                return {'lat': float(r['lat']), 'lon': float(r['lon']),
                        'osm_name': r.get('display_name', '')[:80], 'source': 'nominatim_fallback'}
    except Exception as e:
        print(f"    Nominatim error for '{name}': {e}")
    return {}


def build_osm_index(osm_data: dict) -> list:
    stations = []
    for el in osm_data.get('elements', []):
        if el.get('type') != 'node':
            continue
        tags = el.get('tags', {})
        name = tags.get('name:ru') or tags.get('name', '')
        if not name:
            continue
        stations.append({
            'osm_name': name,
            'osm_name_norm': normalize_name(name),
            'lat': el['lat'],
            'lon': el['lon'],
        })
    return stations


def match_stations_overpass(nbs_stations: dict, osm_stations: list):
    osm_by_name = defaultdict(list)
    for s in osm_stations:
        osm_by_name[s['osm_name_norm']].append(s)

    matched, unmatched_codes = [], []
    for st_code, info in nbs_stations.items():
        norm = normalize_name(info['ST_NAME'])
        candidates = osm_by_name.get(norm, [])
        if len(candidates) >= 1:
            osm = candidates[0]
            quality = 'exact' if len(candidates) == 1 else f'ambiguous_{len(candidates)}'
            matched.append({**info, 'lat': osm['lat'], 'lon': osm['lon'],
                             'osm_name': osm['osm_name'], 'source': 'overpass_' + quality,
                             'match_quality': quality})
        else:
            # Try partial
            partial = [s for s in osm_stations
                       if norm in s['osm_name_norm'] or s['osm_name_norm'] in norm]
            if partial:
                matched.append({**info, 'lat': partial[0]['lat'], 'lon': partial[0]['lon'],
                                 'osm_name': partial[0]['osm_name'], 'source': 'overpass_partial',
                                 'match_quality': 'partial'})
            else:
                unmatched_codes.append(st_code)
    return matched, unmatched_codes


def geocode_with_nominatim(nbs_stations: dict, unmatched_codes: list, already_matched: list):
    matched = list(already_matched)
    unmatched = []
    total = len(unmatched_codes)
    for i, st_code in enumerate(unmatched_codes):
        info = nbs_stations[st_code]
        print(f"  [{i+1}/{total}] {info['ST_NAME']} ({info['LN_NAME']})...")
        result = geocode_nominatim(info['ST_NAME'], info['LN_NAME'])
        if result:
            matched.append({**info, **result, 'match_quality': 'nominatim'})
        else:
            unmatched.append({**info, 'norm_name': normalize_name(info['ST_NAME'])})
        time.sleep(1.1)  # Nominatim rate limit: 1 req/sec
    return matched, unmatched


def save_csv(rows, path, fieldnames):
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} rows → {path}")


def main():
    print("=== Step 1: Load NBS metro stations ===")
    nbs_stations = load_metro_stations()

    # Try Overpass first
    print("\n=== Step 2: Attempt Overpass API ===")
    osm_data = fetch_overpass()

    matched = []
    unmatched_codes = list(nbs_stations.keys())

    if osm_data:
        raw_path = OUT_DIR / "osm_metro_raw.json"
        with open(raw_path, 'w', encoding='utf-8') as f:
            json.dump(osm_data, f, ensure_ascii=False, indent=2)
        print(f"Saved raw OSM → {raw_path}")

        osm_stations = build_osm_index(osm_data)
        print(f"OSM stations: {len(osm_stations)}")
        matched, unmatched_codes = match_stations_overpass(nbs_stations, osm_stations)
        print(f"Overpass matched: {len(matched)}, still need: {len(unmatched_codes)}")
    else:
        print("Overpass unavailable — will geocode ALL via Nominatim")

    # Geocode remaining via Nominatim
    if unmatched_codes:
        print(f"\n=== Step 3: Nominatim geocoding for {len(unmatched_codes)} stations ===")
        matched, unmatched = geocode_with_nominatim(nbs_stations, unmatched_codes, matched)
    else:
        unmatched = []

    print(f"\nFinal: matched={len(matched)}, unmatched={len(unmatched)}")

    if unmatched:
        print("Unmatched (need manual coordinates):")
        for s in unmatched:
            print(f"  [{s['ST_CODE']}] {s['ST_NAME']} / {s['LN_NAME']}")

    # Save
    fields = ['ST_CODE', 'ST_NAME', 'LN_NAME', 'lat', 'lon', 'osm_name', 'source', 'match_quality']
    save_csv(matched, OUT_FILE, fields)
    if unmatched:
        save_csv(unmatched, OUT_UNMATCHED,
                 ['ST_CODE', 'ST_NAME', 'LN_NAME', 'norm_name'])

    print(f"\nDone. Coordinates → {OUT_FILE}")


if __name__ == '__main__':
    main()
