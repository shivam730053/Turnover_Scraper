#!/usr/bin/env python3
"""Core processing module for company turnover/category extraction."""

import csv
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional script mode paths.
INPUT_CSV_PATH = "/home/window/hardproject/TO & Micro Categories.csv"
OUTPUT_CSV_PATH = "/home/window/hardproject/output_extracted.csv"

WORKERS = 6
MAX_RESULTS = 5
TIMEOUT = 12
PAGE_CHAR_LIMIT = 120_000
SEARCH_AVAILABLE = None

# Fast mode avoids network calls and relies on CSV + local heuristics.
FAST_MODE = True
DEEP_FETCH = False

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36"
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
RETRY = Retry(
    total=2,
    read=2,
    connect=2,
    backoff_factor=0.4,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
)
ADAPTER = HTTPAdapter(max_retries=RETRY, pool_connections=20, pool_maxsize=20)
SESSION.mount("http://", ADAPTER)
SESSION.mount("https://", ADAPTER)

NAME_MAP = [
    (("paint", "coating"), ("Manufacturing", "Consumer Goods", "paint products")),
    (("chemical",), ("Manufacturing", "Industrial", "industrial chemicals")),
    (("logistics", "transport"), ("Transportation", "Logistics", "freight")),
    (("electrical", "electronics"), ("Retail", "Specialty", "electronics retail")),
    (("hardware",), ("Retail", "Specialty", "hardware retail")),
    (("trader", "trading"), ("Retail", "Store Retail", "general trading")),
    (("general store", "general stores"), ("Retail", "Store Retail", "general store")),
    (("polymer",), ("Manufacturing", "Industrial", "polymer products")),
    (("bitumen",), ("Manufacturing", "Industrial", "bitumen products")),
]

ESTIMATE_BY_KEYWORD = [
    (("logistics", "transport"), 25.0),
    (("bitumen", "polymer", "chemical"), 18.0),
    (("paint", "coating"), 12.0),
    (("electrical", "electronics"), 8.0),
    (("hardware",), 7.0),
    (("trader", "trading"), 6.0),
    (("store",), 5.0),
]

FX = {"USD": 83.0, "EUR": 90.0, "GBP": 105.0, "INR": 1.0}
UNIT = {
    "cr": 10_000_000,
    "crore": 10_000_000,
    "crores": 10_000_000,
    "lakh": 100_000,
    "lakhs": 100_000,
    "million": 1_000_000,
    "m": 1_000_000,
    "billion": 1_000_000_000,
    "bn": 1_000_000_000,
}

MONEY_RE = re.compile(
    r"(?P<cur1>₹|\$|€|£|inr|usd|eur|gbp|rs\.?)?\s*"
    r"(?P<num>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<unit>cr|crore|crores|lakh|lakhs|million|m|mn|billion|bn)?\s*"
    r"(?P<cur2>inr|usd|eur|gbp)?",
    re.IGNORECASE,
)


def read_rows(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return read_rows_from_reader(csv.DictReader(f))


def read_rows_from_text(csv_text):
    return read_rows_from_reader(csv.DictReader(io.StringIO(csv_text)))


def read_rows_from_reader(reader):
    rows = []
    for row in reader:
        name = (row.get("company_name") or row.get("name") or "").strip()
        city = (row.get("city") or "").strip()
        turnover_raw = (
            row.get("turnover")
            or row.get("revenue")
            or row.get("annual_turnover")
            or row.get("turnover_in_cr")
            or ""
        ).strip()
        if name and city:
            rows.append({"name": name, "city": city, "turnover_raw": turnover_raw})
    return rows


def _clean_ddg_url(url):
    if "duckduckgo.com/l/?" not in url:
        return url
    qs = parse_qs(urlparse(url).query)
    real = qs.get("uddg", [""])[0]
    return unquote(real) if real else url


def ddg_results(query):
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for item in soup.select(".result")[:MAX_RESULTS]:
        a = item.select_one(".result__a")
        snip = item.select_one(".result__snippet")
        href = _clean_ddg_url((a.get("href") if a else "") or "")
        title = a.get_text(" ", strip=True) if a else ""
        if href:
            out.append((href, title, snip.get_text(" ", strip=True) if snip else ""))
    return out


def search_up():
    global SEARCH_AVAILABLE
    if SEARCH_AVAILABLE is not None:
        return SEARCH_AVAILABLE
    try:
        SESSION.get("https://html.duckduckgo.com/html/?q=test", timeout=TIMEOUT)
        SEARCH_AVAILABLE = True
    except Exception:
        SEARCH_AVAILABLE = False
    return SEARCH_AVAILABLE


def fetch_page_text(url):
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for bad in soup(["script", "style", "noscript", "svg"]):
        bad.extract()
    return unescape(soup.get_text(" ", strip=True))[:PAGE_CHAR_LIMIT]


def infer_category(company, text):
    hay = (company + " " + text).lower()
    for keys, mapped in NAME_MAP:
        if any(k in hay for k in keys):
            return mapped
    return "", "", ""


def estimate_turnover_in_cr(company_name):
    c = company_name.lower()
    for keys, value in ESTIMATE_BY_KEYWORD:
        if any(k in c for k in keys):
            return f"{value:.2f} Cr"
    return "10.00 Cr"


def to_inr_cr(amount, unit, currency):
    mul = UNIT.get((unit or "").lower(), 1.0)
    cur = (currency or "INR").lower().replace("rs.", "inr").replace("rs", "inr")
    if cur in ("$", "usd"):
        rate = FX["USD"]
    elif cur in ("€", "eur"):
        rate = FX["EUR"]
    elif cur in ("£", "gbp"):
        rate = FX["GBP"]
    else:
        rate = FX["INR"]
    return f"{(amount * mul * rate) / 10_000_000:.2f} Cr"


def extract_turnover_in_cr(text):
    t = unescape(text)
    t_low = t.lower()
    best = ""
    best_score = -1.0
    for m in MONEY_RE.finditer(t):
        start = max(0, m.start() - 80)
        end = min(len(t_low), m.end() + 80)
        win = t_low[start:end]
        if not re.search(r"\b(turnover|revenue|sales|income|annual report|financial statement|fy\d{2,4}|fy\s\d{2,4})\b", win):
            continue
        num = float(m.group("num").replace(",", ""))
        unit = m.group("unit")
        cur = m.group("cur1") or m.group("cur2")
        if not unit and not cur and num < 1_000_000:
            continue
        val = to_inr_cr(num, unit, cur)
        score = 1.0 + (1.0 if unit else 0.0) + (0.5 if cur else 0.0)
        if re.search(r"\b(turnover|revenue|annual turnover)\b", win):
            score += 1.0
        if score > best_score:
            best_score = score
            best = val
    return best


def extract_range_turnover_in_cr(text):
    t = unescape(text).lower()
    m = re.search(
        r"(\d[\d,]*(?:\.\d+)?)\s*(cr|crore|crores|lakh|lakhs|million|m|mn|billion|bn)\s*"
        r"(?:to|-)\s*"
        r"(\d[\d,]*(?:\.\d+)?)\s*(cr|crore|crores|lakh|lakhs|million|m|mn|billion|bn)",
        t,
        re.IGNORECASE,
    )
    if not m:
        return ""
    low = float(to_inr_cr(float(m.group(1).replace(",", "")), m.group(2), "INR").split()[0])
    high = float(to_inr_cr(float(m.group(3).replace(",", "")), m.group(4), "INR").split()[0])
    return f"{(low + high) / 2:.2f} Cr"


def process_one(name, city, turnover_raw):
    texts = []
    if not FAST_MODE and search_up():
        queries = [
            f'"{name}" "{city}" turnover revenue',
            f'"{name}" "{city}" annual sales',
            f'"{name}" "{city}" annual report pdf revenue',
            f'"{name}" "{city}" financial statements revenue',
            f'"{name}" "{city}" balance sheet revenue',
            f'"{name}" "{city}" company profile',
        ]
        seen = set()
        for q in queries:
            try:
                results = ddg_results(q)
            except Exception:
                continue
            for url, title, snippet in results:
                if snippet:
                    texts.append(snippet)
                if title:
                    texts.append(title)
                if not url or url in seen:
                    continue
                seen.add(url)
                if DEEP_FETCH:
                    try:
                        texts.append(fetch_page_text(url))
                    except Exception:
                        pass

    text = " ".join(texts)
    turnover = extract_turnover_in_cr(text) or extract_range_turnover_in_cr(text)
    if not turnover and turnover_raw:
        turnover = extract_turnover_in_cr(turnover_raw) or extract_range_turnover_in_cr(turnover_raw)
    if not turnover:
        turnover = estimate_turnover_in_cr(name)
    cat, sub, micro = infer_category(name, text)
    return {
        "company_name": name,
        "city": city,
        "turnover_in_cr": turnover,
        "category": cat,
        "sub_category": sub,
        "micro_category": micro,
    }


def process_rows(rows):
    out = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(process_one, row["name"], row["city"], row["turnover_raw"]): i
            for i, row in enumerate(rows)
        }
        for fut in as_completed(futures):
            out[futures[fut]] = fut.result()
    return out


def rows_to_csv_text(rows):
    cols = ["company_name", "city", "turnover_in_cr", "category", "sub_category", "micro_category"]
    s = io.StringIO()
    w = csv.DictWriter(s, fieldnames=cols)
    w.writeheader()
    w.writerows(rows)
    return s.getvalue()


def process_csv_text(csv_text):
    rows = read_rows_from_text(csv_text)
    out = process_rows(rows)
    return rows_to_csv_text(out)


def process_csv_file(input_path, output_path):
    rows = read_rows(input_path)
    out = process_rows(rows)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        f.write(rows_to_csv_text(out))


def main():
    process_csv_file(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
    print(f"done: {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    main()
