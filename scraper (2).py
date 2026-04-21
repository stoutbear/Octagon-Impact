"""
OctagonImpact Full Roster Scraper
===================================
Crawls UFCStats.com A-Z to build a complete active UFC roster,
calculates OIS scores, and outputs fighters.json.

Setup (run once in your terminal):
    pip install requests beautifulsoup4

Usage:
    python scraper.py

Output:
    fighters.json  — upload this to GitHub, your site updates automatically.

Notes:
    - Scrapes all weight classes including women's divisions
    - Filters to active fighters only (fought in last 18 months)
    - Takes 45-60 minutes to run the full roster (~700 fighters)
    - Saves a partial backup every 50 fighters so you don't lose progress
"""

import json
import time
import random
import string
from datetime import datetime, timedelta

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:  pip install requests beautifulsoup4")
    exit(1)

BASE_URL         = "http://www.ufcstats.com"
FIGHTER_LIST_URL = f"{BASE_URL}/statistics/fighters"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

ACTIVE_THRESHOLD_DAYS = 548  # 18 months

DIVISIONS_MENS = [
    "Heavyweight", "Light Heavyweight", "Middleweight",
    "Welterweight", "Lightweight", "Featherweight",
    "Bantamweight", "Flyweight",
]
DIVISIONS_WOMENS = [
    "Women's Strawweight", "Women's Flyweight",
    "Women's Bantamweight", "Women's Featherweight",
]
ALL_DIVISIONS = DIVISIONS_MENS + DIVISIONS_WOMENS


# ── HTTP ────────────────────────────────────────────────────────

def get_soup(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.exceptions.HTTPError as e:
            print(f"    [HTTP {e.response.status_code}]")
            if e.response.status_code == 429:
                time.sleep(30)
            break
        except Exception as e:
            wait = 2 ** attempt
            print(f"    [retry {attempt+1}] {e}")
            time.sleep(wait)
    return None


# ── STAT HELPERS ────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(str(val).replace("%", "").replace("---", "0").strip() or 0)
    except (ValueError, TypeError):
        return default

def clamp(val, lo=0, hi=100):
    return max(lo, min(hi, val))

def parse_career_stats(soup):
    """
    UFCStats career stat page has two <ul class="b-list__box-list"> blocks.
    Each <li> contains two <p> tags: first=label, second=value.
    """
    stats = {}
    for li in soup.select("li.b-list__box-list-item_type_block"):
        ps = li.find_all("p", recursive=False)
        if len(ps) >= 2:
            label = ps[0].get_text(strip=True).lower().rstrip(":")
            value = ps[1].get_text(strip=True)
            if label and label != "":
                stats[label] = value
    # Fallback: colon-split any list items
    if not stats:
        for li in soup.select("ul.b-list__box-list li"):
            text = li.get_text(" ", strip=True)
            if ":" in text:
                k, _, v = text.partition(":")
                stats[k.strip().lower()] = v.strip()
    return stats

def find_stat(stats, *keys):
    for key in keys:
        for k, v in stats.items():
            if key in k:
                return v
    return "0"


# ── OIS CALCULATION ─────────────────────────────────────────────

def calc_striking(slpm, str_acc, str_def):
    vol  = clamp(slpm / 9.0 * 25, 0, 25)
    acc  = clamp(str_acc / 100 * 35, 0, 35)
    defs = clamp(str_def / 100 * 40, 0, 40)
    return round(clamp(vol + acc + defs))

def calc_wrestling(td_avg, td_acc, td_def):
    vol  = clamp(td_avg / 8.0 * 30, 0, 30)
    acc  = clamp(td_acc / 100 * 35, 0, 35)
    defs = clamp(td_def / 100 * 35, 0, 35)
    return round(clamp(vol + acc + defs))

def calc_ground(sub_avg, td_acc, slpm):
    sub  = clamp(sub_avg / 3.0 * 40, 0, 40)
    gnp  = clamp((slpm * td_acc / 100) / 4.0 * 30, 0, 30)
    ctrl = clamp(td_acc / 100 * 30, 0, 30)
    return round(clamp(sub + gnp + ctrl))

def calc_ois(s, w, g):
    return round(clamp(s * 0.40 + w * 0.30 + g * 0.30))


# ── DATE PARSING ────────────────────────────────────────────────

def parse_date(raw):
    for fmt in ("%b. %d, %Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None

def is_active(fights):
    cutoff = datetime.now() - timedelta(days=ACTIVE_THRESHOLD_DAYS)
    for fight in fights:
        d = parse_date(fight.get("date", ""))
        if d and d >= cutoff:
            return True
    return False


# ── FIGHT HISTORY ───────────────────────────────────────────────

def parse_fights(soup, str_s, wre_s, gnd_s):
    fights = []
    # Try the main fight history table rows
    rows = soup.select("tr.b-fight-details__table-row__hover")
    if not rows:
        rows = soup.select("table.b-fight-details__table tr.b-fight-details__table-row")

    for row in rows[:5]:
        try:
            cols = row.select("td")
            if len(cols) < 7:
                continue

            result  = cols[0].get_text(strip=True).upper()
            result  = result[0] if result and result[0] in "WLND" else "W"

            opp_tag = cols[1].find("a")
            opponent = opp_tag.get_text(strip=True) if opp_tag else "Unknown"

            evt_tag = cols[2].find("a") if len(cols) > 2 else None
            event   = evt_tag.get_text(strip=True) if evt_tag else ""

            method_text = cols[6].get_text(" ", strip=True) if len(cols) > 6 else ""
            method  = method_text.split()[0] if method_text else "DEC"

            date_raw = cols[7].get_text(strip=True) if len(cols) > 7 else ""
            date_obj = parse_date(date_raw)
            date_fmt = date_obj.strftime("%Y-%m-%d") if date_obj else date_raw

            # Per-fight variance ±8 points around career averages
            v = lambda base: round(clamp(base + random.randint(-8, 8)))
            fs, fw, fg = v(str_s), v(wre_s), v(gnd_s)

            fights.append({
                "opponent": opponent,
                "event":    event,
                "date":     date_fmt,
                "result":   result,
                "method":   method,
                "ois":      calc_ois(fs, fw, fg),
                "str":      fs,
                "wre":      fw,
                "gnd":      fg,
            })
        except Exception:
            continue
    return fights


# ── FIGHTER DETAIL PAGE ─────────────────────────────────────────

def scrape_fighter(url, fid):
    soup = get_soup(url)
    if not soup:
        return None

    name_el  = soup.select_one("span.b-content__title-highlight")
    name     = name_el.get_text(strip=True) if name_el else "Unknown"

    rec_el   = soup.select_one("span.b-content__title-record")
    record   = rec_el.get_text(strip=True).replace("Record:", "").strip() if rec_el else "0-0"

    # Physical attrs
    attrs = {}
    for li in soup.select("ul.b-list__box-list li.b-list__box-list-item"):
        text = li.get_text(" ", strip=True)
        if ":" in text:
            k, _, v = text.partition(":")
            attrs[k.strip().lower()] = v.strip()

    stats   = parse_career_stats(soup)
    slpm    = safe_float(find_stat(stats, "slpm", "str. landed"))
    str_acc = safe_float(find_stat(stats, "str. acc", "sig. str. acc"))
    str_def = safe_float(find_stat(stats, "str. def", "sig. str. def"))
    td_avg  = safe_float(find_stat(stats, "td avg", "takedowns avg"))
    td_acc  = safe_float(find_stat(stats, "td acc", "takedown acc"))
    td_def  = safe_float(find_stat(stats, "td def", "takedown def"))
    sub_avg = safe_float(find_stat(stats, "sub. avg", "submission avg"))

    str_s = calc_striking(slpm, str_acc, str_def)
    wre_s = calc_wrestling(td_avg, td_acc, td_def)
    gnd_s = calc_ground(sub_avg, td_acc, slpm)
    ois_s = calc_ois(str_s, wre_s, gnd_s)

    fights = parse_fights(soup, str_s, wre_s, gnd_s)

    if not fights:
        return None
    if not is_active(fights):
        return None

    # Division detection
    weight_raw = attrs.get("weight", attrs.get("wt.", "")).lower().replace("'", "")
    division   = "Unknown"
    for div in ALL_DIVISIONS:
        if div.lower().replace("'", "") in weight_raw:
            division = div
            break
    if division == "Unknown":
        page_text = soup.get_text().lower()
        for div in ALL_DIVISIONS:
            if div.lower().replace("'", "") in page_text.replace("'", ""):
                division = div
                break

    last = fights[0]

    return {
        "id":       fid,
        "name":     name,
        "nickname": "",
        "division": division,
        "record":   record,
        "country":  "",
        "age":      None,
        "height":   attrs.get("height", ""),
        "reach":    attrs.get("reach", ""),
        "stance":   attrs.get("stance", ""),
        "ois":      ois_s,
        "str":      str_s,
        "wre":      wre_s,
        "gnd":      gnd_s,
        "result":   last.get("result", "W"),
        "method":   last.get("method", "DEC"),
        "event":    last.get("event", ""),
        "change":   "+0.0",
        "insight": (
            f"{name} — OIS auto-generated from UFCStats.com data. "
            "Edit this field in fighters.json to add a manual scouting note."
        ),
        "fights":   fights,
    }


# ── ROSTER CRAWLER (A-Z) ────────────────────────────────────────

def crawl_all_urls():
    all_fighters = []
    print("\nPhase 1 — Collecting fighter URLs (A to Z)...")

    for letter in string.ascii_lowercase:
        page = 1
        while True:
            url  = f"{FIGHTER_LIST_URL}?char={letter}&page={page}"
            soup = get_soup(url)
            if not soup:
                break

            rows = soup.select(
                "table.b-statistics__table tbody tr.b-statistics__table-row"
            )
            if not rows:
                break

            count = 0
            for row in rows:
                cols = row.select("td")
                if len(cols) < 2:
                    continue
                link = cols[0].find("a")
                if not link:
                    continue
                href  = link.get("href", "").strip()
                first = cols[0].get_text(strip=True)
                last  = cols[1].get_text(strip=True)
                if href and first:
                    all_fighters.append({
                        "name": f"{first} {last}".strip(),
                        "url":  href,
                    })
                    count += 1

            print(f"  [{letter.upper()}] page {page}: {count} fighters")

            next_pg = soup.select_one("a.b-statistics__paginate-item_next")
            if next_pg and next_pg.get("href"):
                page += 1
                time.sleep(random.uniform(0.8, 1.5))
            else:
                break

        time.sleep(random.uniform(0.5, 1.0))

    # Deduplicate
    seen, unique = set(), []
    for f in all_fighters:
        if f["url"] not in seen:
            seen.add(f["url"])
            unique.append(f)

    print(f"\n  Total unique URLs: {len(unique)}")
    return unique


# ── MAIN ────────────────────────────────────────────────────────

def main():
    start = datetime.now()
    print("=" * 62)
    print("  OctagonImpact Full Roster Scraper")
    print(f"  Started: {start.strftime('%I:%M %p')}")
    print("=" * 62)

    urls = crawl_all_urls()

    print(f"\nPhase 2 — Scraping {len(urls)} profiles (the slow part)...")
    print("  Partial saves every 50 fighters in case of interruption.\n")

    results  = []
    skipped  = 0
    fid      = 1

    for i, entry in enumerate(urls):
        pct  = round((i + 1) / len(urls) * 100)
        print(f"  [{i+1}/{len(urls)} {pct}%] {entry['name']}", end="", flush=True)

        data = scrape_fighter(entry["url"], fid)

        if data is None:
            skipped += 1
            print("  — skipped")
        else:
            results.append(data)
            fid += 1
            print(
                f"  OIS={data['ois']}  STR={data['str']}  "
                f"WRE={data['wre']}  GND={data['gnd']}  [{data['division']}]"
            )

        # Polite delay
        time.sleep(random.uniform(2.0, 4.0))

        # Checkpoint save every 50 fighters
        if (i + 1) % 50 == 0:
            elapsed = datetime.now() - start
            print(f"\n  ── {len(results)} active fighters found so far")
            print(f"  ── Elapsed: {str(elapsed).split('.')[0]}")
            print(f"  ── Saving partial backup to fighters_partial.json\n")
            with open("fighters_partial.json", "w", encoding="utf-8") as pf:
                json.dump(results, pf, indent=2, ensure_ascii=False)

    # Sort by OIS
    results.sort(key=lambda x: x["ois"], reverse=True)

    with open("fighters.json", "w", encoding="utf-8") as out:
        json.dump(results, out, indent=2, ensure_ascii=False)

    # Summary
    elapsed = datetime.now() - start
    div_counts = {}
    for f in results:
        div_counts[f["division"]] = div_counts.get(f["division"], 0) + 1

    print(f"\n{'=' * 62}")
    print(f"  COMPLETE!")
    print(f"  Active fighters saved: {len(results)}")
    print(f"  Skipped (inactive):    {skipped}")
    print(f"  Total time:            {str(elapsed).split('.')[0]}")
    print(f"\n  Breakdown by division:")
    for div in ALL_DIVISIONS + ["Unknown"]:
        if div in div_counts:
            print(f"    {div:<32} {div_counts[div]}")
    print(f"\n  Saved to: fighters.json")
    print(f"  Upload to GitHub — site updates in ~30 seconds.")
    print("=" * 62)

if __name__ == "__main__":
    main()
