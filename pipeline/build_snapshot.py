"""Готує з сирих повних записів тендерів (_private/<tag>_details.jsonl, з ПДн) файли релізу без ПДн.

    zt_bronze_<tag>.jsonl.gz   Bronze: вкладені тендери після bd.minimize (без контактів, пропозицій учасників, ФОП)
    zt_silver_<tag>.parquet    Silver: одна пласка таблиця (схема в SILVER_SQL нижче, як у рішенні ЛР 2, плюс колонка sector)

Запуск: python build_snapshot.py a
"""
import gzip, json, sys, pathlib
import duckdb

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bigdata-labs"))
import bd  # noqa: E402

PRIV = ROOT / "bigdata-teacher" / "_private"
OUT = ROOT / "_data"

SILVER_SQL = """
SELECT
    id                                   AS tender_id,
    tenderID                             AS tender_number,
    title,
    status,
    procurementMethodType                AS method,
    procurementMethod                    AS method_group,
    mainProcurementCategory              AS category,
    CAST(dateCreated AS TIMESTAMPTZ)     AS created_at,
    CAST(dateModified AS TIMESTAMPTZ)    AS modified_at,
    CAST(tenderPeriod.endDate AS TIMESTAMPTZ) AS tender_end,
    items[1].classification.id           AS cpv,
    left(items[1].classification.id, 2)  AS cpv2,
    procuringEntity.identifier.id        AS buyer_edrpou,
    procuringEntity.name                 AS buyer_name,
    procuringEntity.kind                 AS buyer_kind,
    procuringEntity.address.locality     AS buyer_locality,
    CAST(value.amount AS DOUBLE)         AS expected_value,
    value.currency                       AS currency,
    coalesce(len(lots), 0)               AS n_lots,
    len(items)                           AS n_items,
    CAST(numberOfBids AS BIGINT)         AS n_bids,
    list_sum([CAST(award.value.amount AS DOUBLE) FOR award IN awards IF award.status = 'active'])          AS award_value,
    list_sum([CAST(contract.value.amount AS DOUBLE) FOR contract IN contracts IF contract.status = 'active']) AS contract_value,
    [award.suppliers[1].edrpou FOR award IN awards IF award.status = 'active'][1]                         AS winner_edrpou
FROM read_json('{src}', format='newline_delimited', union_by_name=true, sample_size=-1)
"""


def main(tag: str) -> None:
    OUT.mkdir(exist_ok=True)
    bronze = OUT / f"zt_bronze_{tag}.jsonl.gz"
    kept = 0
    with open(PRIV / f"{tag}_details.jsonl", encoding="utf-8") as src, gzip.open(bronze, "wt", encoding="utf-8") as dst:
        for line in src:
            tender = bd.minimize(json.loads(line))
            if ((tender.get("procuringEntity") or {}).get("address") or {}).get("region") != bd.REGION:
                continue
            dst.write(json.dumps(tender, ensure_ascii=False) + "\n")
            kept += 1
    print(f"Bronze, тендерів: {bd.num(kept)} -> {bronze.name} ({bd.num(bronze.stat().st_size / 1e6, 1)} МБ)")

    con = duckdb.connect()
    con.sql("SET TimeZone = 'UTC'")  # read_json дає TIMESTAMP без поясу (в UTC); інакше CAST до TIMESTAMPTZ зсуне час на зміщення поясу машини
    con.sql("CREATE TABLE sector_map(cpv2 VARCHAR, sector VARCHAR)")
    con.executemany("INSERT INTO sector_map VALUES (?, ?)", list(bd.CPV2_TO_SECTOR.items()))
    silver = OUT / f"zt_silver_{tag}.parquet"
    con.sql(f"""COPY (
        SELECT tenders.*, sector_map.sector FROM ({SILVER_SQL.format(src=bronze)}) tenders
        LEFT JOIN sector_map USING (cpv2)
        ORDER BY created_at
    ) TO '{silver}' (FORMAT parquet, COMPRESSION zstd)""")
    print(con.sql(f"SELECT sector, count(*) FROM '{silver}' GROUP BY ALL ORDER BY 2 DESC").fetchall())

    # Перевірка, що у файлі Bronze (з нього будується Silver) не лишилося персональних даних
    with gzip.open(bronze, "rt", encoding="utf-8") as bronze_file:
        text = bronze_file.read()
    for marker in ("contactPoint", "telephone", "email", "tenderers", '"bids"'):
        assert marker not in text, f"знайдено {marker} у Bronze"
    assert not bd._EMAIL.search(text), "знайдено адресу електронної пошти у вільному тексті Bronze"
    print("Контроль ПДн пройдено")
    (PRIV / f"{tag}_details.jsonl").unlink()  # мінімізація даних: сирі повні записи з контактами більше не потрібні
    print("Сирі повні записи з ПДн видалено")


if __name__ == "__main__":
    main(sys.argv[1])
