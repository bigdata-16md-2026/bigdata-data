"""Допоміжні функції курсу Big Data. Імпорт у блокноті: `import bd`.

    bd.me()              -> ім'я і сектор здобувача з config.toml
    bd.machine()         -> опис машини: процесори, пам'ять, вільне місце на диску
    bd.data("файл")      -> шлях до файлу даних (завантажує його, якщо файлу ще немає)
    bd.check(умова, ...) -> «Перевірка пройдена: …» або зупинка з підказкою (запис у results/checks.jsonl)
    bd.record("01", k=v)  -> ключові результати ЛР у results/lab01.json (файл комітиться разом з блокнотом)
    bd.num(x), bd.pct(x) -> число в українському записі: «22 541», «3,6», «9,1 %»
    bd.minimize(тендер)  -> тендер без персональних даних
    bd.THRESHOLDS        -> CSV порогів закупівель за датою, категорією й типом замовника
    bd.progress()        -> стан ризик-монітора: які функції monitor/ написано, які файли results/ є, що далі
    bd.git_status()      -> файли, зміни в яких ще не збережено в git
    bd.SECTORS           -> сектори і їхні розділи CPV
"""
import datetime
import http.client
import json
import os
import pathlib
import platform
import re
import shutil
import subprocess
import tomllib
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA_URL = os.environ.get(
    "BD_DATA_URL",
    "https://github.com/bigdata-16md-2026/bigdata-data/releases/download/v1",
)
REGION = "Житомирська область"
THRESHOLDS = ROOT / "bd" / "thresholds.csv"  # пороги закупівель (п. 10 постанови КМУ № 1178), див. docs

# Сектор: набір розділів CPV (перші дві цифри коду предмета закупівлі за ДК 021:2015).
SECTORS = {
    "food":      ("Продукти харчування", ["15"]),
    "energy":    ("Паливо, енергія, електрообладнання", ["09", "31"]),
    "medicine":  ("Медицина й фармацевтика", ["33"]),
    "materials": ("Будівельні матеріали й конструкції", ["44"]),
    "repair":    ("Будівництво, ремонт, технічне обслуговування", ["45", "50"]),
    "household": ("Меблі, побутові товари, одяг", ["18", "19", "39"]),
    "it":        ("Техніка, зв'язок, ІТ", ["30", "32", "48", "72"]),
    "agro":      ("Сільське господарство, довкілля, комунальні послуги", ["03", "65", "77", "90"]),
    "transport": ("Транспорт і машини", ["34", "42", "60"]),
    "services":  ("Послуги: інженерні, ділові, освітні, медичні й соціальні, фінансові", ["66", "71", "79", "80", "85"]),
    "lab":       ("Хімічна продукція, лабораторне обладнання, друкована продукція", ["22", "24", "38"]),
}
CPV2_TO_SECTOR = {code: sector for sector, (_, codes) in SECTORS.items() for code in codes}


class CheckFailed(AssertionError):
    """Помилка перевірки: у Jupyter показує лише підказку, без довгого трасування стека (traceback)."""

    def _render_traceback_(self):
        return [str(self)]


if "'" in str(ROOT):  # шляхи підставляються в SQL-запити курсу, а апостроф обриває рядок SQL
    raise CheckFailed(f"Шлях до репозиторію містить апостроф ({ROOT}), а SQL-запити курсу його не підтримують. "
                      "Слід склонувати репозиторій у теку без апострофа, кирилиці й пробілів, наприклад C:\\bd "
                      "або ~/bd (docs/setup.md).")


def check(ok, success: str, hint: str) -> None:
    """Перевірка кроку: друкує «Перевірка пройдена: success» або зупиняється з підказкою hint.

    Кожна перевірка дописується рядком у results/checks.jsonl (час, ЛР, результат); помилка запису не заважає перевірці.
    """
    ok = bool(ok)
    folder = re.match(r"(\d\d)-", pathlib.Path.cwd().name)  # з теки labs/01-ingest виходить ЛР «01»
    entry = {"time": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
             "lab": folder and folder.group(1), "ok": ok, "text": success if ok else hint}
    try:
        (ROOT / "results").mkdir(exist_ok=True)
        with open(ROOT / "results" / "checks.jsonl", "a", encoding="utf-8") as log:
            log.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass
    if ok:
        print("Перевірка пройдена:", success)
    else:
        raise CheckFailed("Перевірка не пройдена: " + hint)


def record(lab: str, **values) -> None:
    """Записує ключові результати ЛР у results/lab{lab}.json: нові ключі додаються, наявні оновлюються.

    Значення: числа, рядки, True/False, None і списки з них; типи numpy перетворюються на звичайні числа Python.
    """
    path = ROOT / "results" / f"lab{str(lab).zfill(2)}.json"
    values = json.loads(json.dumps(values, default=lambda value: value.tolist()))  # numpy -> Python
    try:
        saved = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):  # файлу ще немає або його зіпсовано (наприклад, конфліктом злиття)
        saved = {}
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(saved | values, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Результати записано в results/{path.name}")


def num(value, digits: int = 0) -> str:
    """Число в українському записі: 22 541, 3,6, −2,2 (нерозривний пробіл між розрядами, десяткова кома, знак «−»).

    Не число (наприклад, None у заготовці) повертається як str(value), щоб повідомлення перевірки не падало.
    """
    try:
        text = f"{value:,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)
    if text.startswith("-") and not text.strip("-0.,"):  # -0.04 з digits=0 дає «0», а не «−0»
        text = text[1:]
    return text.replace(",", "\u00a0").replace(".", ",").replace("-", "\u2212")


def pct(share, digits: int = 1) -> str:
    """Частка 0..1 у відсотках: 0.091 -> «9,1 %»."""
    return num(share * 100, digits) + "\u00a0%"


def machine() -> str:
    """Опис машини: процесори, пам'ять, вільне місце на диску. Працює у Windows, macOS і Linux."""
    ram = None
    try:
        ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9   # Linux, macOS
    except (AttributeError, ValueError, OSError):
        try:  # Windows
            import ctypes

            class _Mem(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            memory = _Mem()
            memory.dwLength = ctypes.sizeof(_Mem)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory))
            ram = memory.ullTotalPhys / 1e9
        except Exception:
            pass
    system = {"Darwin": "macOS"}.get(platform.system(), platform.system())
    where = "GitHub Codespaces" if os.environ.get("CODESPACES") else f"власний комп'ютер ({system})"
    ram_text = f"{num(ram, 1)} ГБ" if ram else "невідомо"
    free = shutil.disk_usage(ROOT).free / 1e9
    return f"{where}. Процесорів: {os.cpu_count()}; оперативна пам'ять: {ram_text}; вільне місце на диску: {num(free)} ГБ"


def me() -> dict:
    """Налаштування здобувача з config.toml: ім'я, сектор, його назва і розділи CPV."""
    path = pathlib.Path(os.environ.get("BD_CONFIG", ROOT / "config.toml"))
    try:
        config = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise CheckFailed(f"Не вдалося прочитати config.toml ({error}). Слід відновити файл "
                          "(Source Control → правою кнопкою на config.toml → Discard Changes) "
                          "або зв'язатися з викладачем.") from None
    sector = config.get("sector", "")
    name = str(config.get("name", "")).strip()
    problems = []
    if not name:
        problems.append("поле name порожнє: вписати між лапками прізвище й ім'я так, як у файлі SECTORS.md (Прізвище Ім'я)")
    if sector not in SECTORS:
        problems.append(f"поле sector порожнє або має невідоме значення «{sector}»: вписати між лапками ключ сектора зі свого рядка "
                        f"у файлі SECTORS.md ({', '.join(SECTORS)}); сектор призначено заздалегідь, а якщо прізвища немає в SECTORS.md, "
                        "зв'язатися з викладачем")
    if problems:
        raise CheckFailed("У config.toml " + "; ".join(problems) + ". Зберегти файл (Ctrl+S) і виконати комірку ще раз; "
                          "якщо файл змінено після виконання блокнота, виконати Restart → Run All.")
    sector_name, codes = SECTORS[sector]
    return {"name": name, "sector": sector, "sector_name": sector_name, "cpv2": codes}


def data(filename: str) -> pathlib.Path:
    """Шлях до файлу в data/. Якщо файлу немає, завантажує його з репозиторію даних курсу (реліз v1 на GitHub)."""
    DATA.mkdir(exist_ok=True)
    path = DATA / filename
    url = f"{DATA_URL}/{filename}"
    mirror = os.environ.get("BD_MIRROR")  # локальна копія даних
    if path.exists():
        return path
    mirror_file = pathlib.Path(mirror) / filename if mirror else None
    if mirror_file and mirror_file.exists():
        try:
            path.symlink_to(mirror_file)
        except OSError:  # у Windows символьні посилання потребують прав адміністратора
            shutil.copyfile(mirror_file, path)
        return path
    print(f"Завантаження {filename}:", end=" ", flush=True)
    part_path = path.with_name(f"{path.name}.{os.getpid()}.part")  # окремий файл для кожного процесу
    try:
        with urllib.request.urlopen(url, timeout=60) as response, open(part_path, "wb") as part_file:
            total = int(response.headers.get("Content-Length") or 0)
            received, shown_tenths = 0, 0
            while chunk := response.read(1 << 20):
                part_file.write(chunk)
                received += len(chunk)
                if total and received * 10 // total > shown_tenths:  # друкує хід завантаження кожні 10 %
                    shown_tenths = received * 10 // total
                    print(f"{shown_tenths * 10} %", end=" ", flush=True)
            if total and received < total:  # з'єднання обірвалося, тому частково завантажений файл не зберігається
                raise OSError(f"отримано байтів: {num(received)} з {num(total)}")
        part_path.replace(path)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise CheckFailed(f"Не вдалося завантажити {filename} (HTTP {error.code}). "
                              "Слід трохи зачекати й виконати комірку ще раз.") from None
        raise CheckFailed(f"Файлу {filename} немає в репозиторії даних курсу. Слід перевірити назву файлу "
                          "або зв'язатися з викладачем.") from None
    except (OSError, http.client.HTTPException) as error:  # немає мережі, тайм-аут, обрив
        where = "" if os.environ.get("CODESPACES") else " Якщо це не допоможе, слід відкрити репозиторій у Codespaces (docs/setup.md)."
        raise CheckFailed(f"Не вдалося завантажити {filename} ({error}). Слід перевірити підключення до інтернету "
                          f"і виконати комірку ще раз.{where}") from None
    finally:
        part_path.unlink(missing_ok=True)  # після Interrupt чи помилки недозавантажений файл не лишається
    print(f"{num(path.stat().st_size / 1e6, 1)} МБ")
    return path


def progress() -> None:
    """Друкує стан ризик-монітора (те саме, що uv run python -m monitor): функції monitor/, файли results/, що далі."""
    try:
        from monitor import report
    except ImportError:
        print("Пакета monitor/ у репозиторії немає: зв'язатися з викладачем.")
        return
    print(report.status())


def git_status() -> None:
    """Друкує файли, зміни в яких ще не закомічено, і чи надіслано коміти на GitHub (Sync)."""
    try:
        result = subprocess.run(["git", "status", "--short", "--branch"], cwd=ROOT, capture_output=True, text=True,
                                encoding="utf-8")
    except FileNotFoundError:
        print("Програму git не знайдено: слід встановити Git (docs/setup.md) і перезапустити VS Code.")
        return
    if result.returncode != 0:
        print(result.stderr.strip())
        return
    branch, *files = result.stdout.splitlines()  # перший рядок: «## main...origin/main [ahead 1]»
    print("\n".join(files) or "Усі зміни збережено в git.")
    ahead = re.search(r"ahead (\d+)", branch)
    if ahead:
        print(f"Комітів, не надісланих на GitHub: {ahead.group(1)}. Слід натиснути Sync Changes.")
    elif "behind" in branch:
        print("На GitHub є коміти, яких тут ще немає. Слід натиснути Sync Changes.")
    elif "..." in branch:
        print("Усі коміти надіслано на GitHub.")


def cpv2(tender: dict) -> str | None:
    """Перші дві цифри CPV (розділ) першої позиції тендера, наприклад '15' (продукти харчування)."""
    for item in tender.get("items") or []:
        code = ((item.get("classification") or {}).get("id") or "")
        if len(code) >= 2:
            return code[:2]
    return None


def _pick(obj, spec):
    """Залишає в obj лише поля зі spec (рекурсивно, для словників і списків)."""
    if obj is None:
        return None
    if isinstance(obj, list):
        return [_pick(element, spec) for element in obj]
    picked = {}
    for field, sub_spec in spec.items():
        if field in obj:
            picked[field] = obj[field] if sub_spec is None else _pick(obj[field], sub_spec)
    return picked


_MONEY = {"amount": None, "currency": None}
_KEEP = {
    "id": None, "tenderID": None, "title": None, "description": None, "status": None,
    "procurementMethod": None, "procurementMethodType": None, "mainProcurementCategory": None,
    "dateCreated": None, "dateModified": None, "numberOfBids": None,
    "value": {"amount": None, "currency": None, "valueAddedTaxIncluded": None},
    "tenderPeriod": {"startDate": None, "endDate": None},
    "procuringEntity": {
        "name": None, "kind": None,
        "identifier": {"id": None, "scheme": None, "legalName": None},
        "address": {"region": None, "locality": None, "postalCode": None},
    },
    "items": {"description": None, "quantity": None,
              "classification": {"id": None, "description": None},
              "unit": {"name": None}},
    "lots": {"id": None, "title": None, "status": None, "value": _MONEY},
    "awards": {"id": None, "status": None, "date": None, "lotID": None, "value": _MONEY},
    "contracts": {"id": None, "status": None, "dateSigned": None, "awardID": None, "value": _MONEY},
}


def _supplier(supplier: dict) -> dict:
    """Код юридичної особи (ЄДРПОУ, 8 цифр) зберігається; код ФОП (10 цифр, РНОКПП фізичної особи) та інші коди приховуються."""
    identifier = supplier.get("identifier") or {}
    code = identifier.get("id") or ""
    if len(code) == 8 and code.isdigit():
        return {"edrpou": code, "name": identifier.get("legalName") or supplier.get("name")}
    return {"edrpou": None, "name": "ФОП (приховано)"}  # мітку не змінювати: так позначено переможців у знімках A і B


_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)*\.[A-Za-z]{2,}")


def minimize(tender: dict) -> dict:
    """Тендер лише з потрібними полями і без персональних даних.

    Прибирає контактних осіб (ПІБ, телефони, адреси електронної пошти), пропозиції учасників і дані ФОП;
    адреси електронної пошти у вільному тексті (назви, описи) замінює на [пошта].
    Принцип мінімізації даних: не зберігати те, що не потрібне для аналізу.
    """
    minimal = _pick(tender, _KEEP)
    for part in [minimal, *(minimal.get("items") or []), *(minimal.get("lots") or [])]:
        for field in ("title", "description"):
            if isinstance(part.get(field), str):
                part[field] = _EMAIL.sub("[пошта]", part[field])
    if "bids" in tender:  # у пропозиціях є дані учасників, тому зберігається лише кількість пропозицій
        minimal["numberOfBids"] = minimal.get("numberOfBids") or len(tender["bids"])
    for award, source_award in zip(minimal.get("awards") or [], tender.get("awards") or []):
        award["suppliers"] = [_supplier(supplier) for supplier in source_award.get("suppliers") or []]
    return minimal


def _name_error_hint(shell, etype, value, tb, tb_offset=None):
    """Показує звичайне повідомлення про NameError і пораду, що робити після перезапуску ядра."""
    stb = shell.InteractiveTB.structured_traceback(etype, value, tb, tb_offset=tb_offset) + [
        "Якщо цю назву визначено в комірках вище (наприклад, після перезапуску ядра чи повторного відкриття "
        "Codespace), слід виконати блокнот з початку: Run All. Інакше слід перевірити, чи правильно написано назву."]
    shell._showtraceback(etype, value, stb)  # у Jupyter це звичайний вивід помилки, і Run All зупиняється


try:  # get_ipython є лише в Jupyter; у скриптах (streamlit, dbt) обробник не потрібен
    get_ipython().set_custom_exc((NameError,), _name_error_hint)  # noqa: F821
except NameError:
    pass
