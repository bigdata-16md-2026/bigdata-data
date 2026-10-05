"""Знімок закупівель Житомирщини: стрічка ProZorro, фільтр за областю, повні записи тендерів.

Результат сирий, містить ПДн і призначений лише для викладача:
  _private/<tag>_feed_zt.jsonl     елементи стрічки Житомирської області
  _private/<tag>_details.jsonl     повні записи тендерів, створених у вікні

Запуск:  python crawl_zhytomyr.py --tag a --start 2026-08-03 --end 2026-09-28
Повторний знімок (B) для ЛР 2 і частини C ЛР 5: --tag b --ids-from a (ті самі тендери в новішому стані).
"""
import argparse, json, time, pathlib, sys, zoneinfo, datetime as dt
from concurrent.futures import ThreadPoolExecutor
import requests

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "bigdata-labs"))
import bd  # noqa: E402  (bd.num для чисел у журналі)

API = "https://public-api.prozorro.gov.ua/api/2.5/tenders"
OPT = "procuringEntity,dateCreated,status,procurementMethodType"
REGION = "Житомирська область"
KYIV = zoneinfo.ZoneInfo("Europe/Kyiv")
OUT = pathlib.Path(__file__).resolve().parents[1] / "_private"
session = requests.Session()


def get_json(url, params=None, tries=6):
    for attempt in range(tries):
        try:
            response = session.get(url, params=params, timeout=120)
        except requests.RequestException:
            response = None  # мережева помилка: повторити після паузи
        if response is not None:
            if response.status_code == 200:
                return response.json()
            if response.status_code == 404:
                return None
            if response.status_code != 429 and response.status_code < 500:  # інші помилки 4xx повтор не виправить
                raise RuntimeError(f"HTTP {response.status_code}: {url} {params}")
        time.sleep(2 ** attempt)  # пауза зростає: 1, 2, 4, 8… с
    raise RuntimeError(f"не вдалося отримати {url} {params} після {tries} спроб")


def crawl_feed(start, end, path):
    offset, seen, kept, started = start.timestamp(), 0, {}, time.time()
    while True:
        page = get_json(API, {"limit": 1000, "offset": offset, "opt_fields": OPT})
        for item in page["data"]:
            seen += 1
            buyer = item.get("procuringEntity") or {}
            created = dt.datetime.fromisoformat(item["dateCreated"])
            if (buyer.get("address") or {}).get("region") == REGION and start <= created < end:
                kept[item["id"]] = item  # пізніша версія замінює ранішу
        if seen // 1000 % 50 == 0:
            print(f"стрічка, елементів: {bd.num(seen)}; Житомирщина: {bd.num(len(kept))}; {time.time() - started:.0f} с", flush=True)
        if len(page["data"]) < 1000:
            break
        offset = page["next_page"]["offset"]
    with open(path, "w", encoding="utf-8") as out_file:
        for item in kept.values():
            out_file.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"стрічку прочитано, елементів: {bd.num(seen)}; Житомирщина: {bd.num(len(kept))}", flush=True)
    return list(kept)


def fetch_details(ids, path, threads=8):
    started = time.time()
    with open(path, "w", encoding="utf-8") as out_file, ThreadPoolExecutor(threads) as pool:
        details = pool.map(lambda tender_id: get_json(f"{API}/{tender_id}"), ids)
        for i, tender in enumerate(details, 1):
            if tender:
                out_file.write(json.dumps(tender["data"], ensure_ascii=False) + "\n")
            if i % 2000 == 0:
                print(f"повні записи: {bd.num(i)} з {bd.num(len(ids))}; {time.time() - started:.0f} с", flush=True)
    print(f"повні записи готові, тендерів: {bd.num(len(ids))}; {time.time() - started:.0f} с", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--ids-from", help="тег попереднього знімка: лише перезавантажити ті самі тендери (знімок B)")
    args = parser.parse_args()
    start = dt.datetime.fromisoformat(args.start).replace(tzinfo=KYIV)
    end = dt.datetime.fromisoformat(args.end).replace(tzinfo=KYIV)
    OUT.mkdir(exist_ok=True)
    if args.ids_from:  # усі тендери вікна вже створені, тому набір id той самий, змінився тільки їхній стан
        feed = OUT / f"{args.ids_from}_feed_zt.jsonl"
        ids = [json.loads(line)["id"] for line in open(feed, encoding="utf-8")]
    else:
        ids = crawl_feed(start, end, OUT / f"{args.tag}_feed_zt.jsonl")
    fetch_details(ids, OUT / f"{args.tag}_details.jsonl")
    (OUT / f"{args.tag}_meta.json").write_text(json.dumps(
        {"tag": args.tag, "start": args.start, "end": args.end, "captured_at": dt.datetime.now(KYIV).isoformat()},
        ensure_ascii=False), encoding="utf-8")
