"""
OctagonImpact Full Roster Scraper — Fixed Version
====================================================
Crawls UFCStats.com A-Z, calculates OIS scores, outputs fighters.json.

Setup (run once):
    pip install requests beautifulsoup4

Usage:
    python scraper.py

Takes 45-60 minutes for the full roster.
Saves fighters_partial.json every 50 fighters as a backup.
"""

import json, time, random, string
from datetime import datetime, timedelta

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run:  pip install requests beautifulsoup4")
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

# Fighter must have fought within this many days to be "active"
ACTIVE_DAYS = 600  # ~20 months — generous to catch fighters returning from injury

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


# ── HTTP ──────────────────────────────────────────────────────

def get_soup(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code
            print(f"    [HTTP {code}]")
            if code == 429:
                time.sleep(30)
            return None
        except Exception as e:
            wait = 2 ** attempt
            print(f"    [retry {attempt+1}] {e}")
            time.sleep(wait)
    return None


# ── HELPERS ───────────────────────────────────────────────────

def sf(val, default=0.0):
    try:
        return float(str(val).replace("%","").replace("---","0").strip() or 0)
    except:
        return default

def clamp(v, lo=0, hi=100):
    return max(lo, min(hi, v))

def parse_date(raw):
    raw = str(raw).strip()
    for fmt in ("%b. %d, %Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except:
            continue
    return None


# ── OIS SCORING ───────────────────────────────────────────────

def calc_str(slpm, acc, def_):
    return round(clamp(
        clamp(slpm/9*25, 0, 25) +
        clamp(acc/100*35, 0, 35) +
        clamp(def_/100*40, 0, 40)
    ))

def calc_wre(td_avg, td_acc, td_def):
    return round(clamp(
        clamp(td_avg/8*30, 0, 30) +
        clamp(td_acc/100*35, 0, 35) +
        clamp(td_def/100*35, 0, 35)
    ))

def calc_gnd(sub_avg, td_acc, slpm):
    return round(clamp(
        clamp(sub_avg/3*40, 0, 40) +
        clamp((slpm*td_acc/100)/4*30, 0, 30) +
        clamp(td_acc/100*30, 0, 30)
    ))

def calc_ois(s, w, g):
    return round(clamp(s*0.40 + w*0.30 + g*0.30))


# ── CAREER STATS ──────────────────────────────────────────────

def get_career_stats(soup):
    """
    UFCStats career stats are in two <ul class="b-list__box-list"> blocks.
    Each <li> has the format:  LABEL: VALUE  as plain text.
    We split on the colon to get key/value pairs.
    """
    stats = {}
    for ul in soup.select("ul.b-list__box-list"):
        for li in ul.select("li.b-list__box-list-item"):
            text = li.get_text(" ", strip=True)
            if ":" in text:
                k, _, v = text.partition(":")
                key = k.strip().lower()
                val = v.strip()
                if key and val and key != "":
                    stats[key] = val
    return stats

def find(stats, *keywords):
    for kw in keywords:
        for k, v in stats.items():
            if kw in k:
                return v
    return "0"


# ── FIGHT HISTORY ─────────────────────────────────────────────

def get_fights(soup, str_s, wre_s, gnd_s):
    """
    Fight history on UFCStats fighter pages is in a <tbody> inside
    a <table> with class b-fight-details__table. Each data row has
    class b-fight-details__table-row and the hover class.
    Columns: 0=result, 1=opponent, 2=event, 3=method, 4=round, 5=time, 6=date
    Note: UFCStats actually puts columns in this order on the fighter page:
      W/L | opponent | event | method | round | time | date
    """
    fights = []
    rows = soup.select("tr.b-fight-details__table-row__hover")

    for row in rows[:5]:
        try:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            # Result — first <p> inside col 0
            res_p = cols[0].find("p")
            result = res_p.get_text(strip=True).upper()[0] if res_p else "W"
            if result not in "WLND":
                result = "W"

            # Opponent — first <a> in col 1
            opp_a = cols[1].find("a")
            opponent = opp_a.get_text(strip=True) if opp_a else "Unknown"

            # Event — first <a> in col 2
            evt_a = cols[2].find("a")
            event = evt_a.get_text(strip=True) if evt_a else ""

            # Method — first <p> in col 7 (method column on fighter page)
            # UFCStats fighter page column order varies; grab all <p> text from col 7
            method = "DEC"
            if len(cols) > 7:
                method_p = cols[7].find("p")
                if method_p:
                    method = method_p.get_text(strip=True).split()[0]

            # Date — last column that contains a 4-digit year
            date_fmt = ""
            for col in reversed(cols):
                text = col.get_text(strip=True)
                if len(text) > 4 and any(y in text for y in ["202","201","200","199"]):
                    d = parse_date(text)
                    if d:
                        date_fmt = d.strftime("%Y-%m-%d")
                        break

            # Per-fight OIS with small variance
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


# ── ACTIVE CHECK ──────────────────────────────────────────────

def is_active(fights):
    """
    Return True if the fighter has at least one fight with a parseable
    date within ACTIVE_DAYS, OR if we simply could not parse any dates
    at all (give benefit of the doubt rather than excluding valid fighters).
    """
    if not fights:
        return False

    cutoff = datetime.now() - timedelta(days=ACTIVE_DAYS)
    parseable_dates = 0

    for fight in fights:
        d = parse_date(fight.get("date", ""))
        if d:
            parseable_dates += 1
            if d >= cutoff:
                return True

    # If we found fights but couldn't parse ANY dates, include the fighter
    # rather than silently dropping them
    if parseable_dates == 0 and len(fights) > 0:
        return True

    return False


# ── DIVISION DETECTION ────────────────────────────────────────

def detect_division(soup, attrs):
    # Try weight attribute first
    weight = attrs.get("weight", attrs.get("wt.", "")).lower().replace("'", "").replace("\u2019", "")
    for div in ALL_DIVISIONS:
        if div.lower().replace("'", "") in weight:
            return div

    # Scan full page text as fallback
    page = soup.get_text().lower().replace("'", "").replace("\u2019", "")
    for div in ALL_DIVISIONS:
        if div.lower().replace("'", "") in page:
            return div

    return "Unknown"


# ── FIGHTER DETAIL ────────────────────────────────────────────

def scrape_fighter(url, fid):
    soup = get_soup(url)
    if not soup:
        return None

    # Name
    name_el = soup.select_one("span.b-content__title-highlight")
    name = name_el.get_text(strip=True) if name_el else "Unknown"
    if name == "Unknown":
        return None

    # Record
    rec_el = soup.select_one("span.b-content__title-record")
    record = rec_el.get_text(strip=True).replace("Record:", "").strip() if rec_el else "0-0"

    # Physical attrs (height, reach, stance, weight)
    attrs = {}
    for li in soup.select("ul.b-list__box-list li.b-list__box-list-item"):
        text = li.get_text(" ", strip=True)
        if ":" in text:
            k, _, v = text.partition(":")
            attrs[k.strip().lower()] = v.strip()

    # Career stats
    stats = get_career_stats(soup)
    slpm    = sf(find(stats, "slpm", "sig. str. landed"))
    str_acc = sf(find(stats, "str. acc", "sig. str. acc"))
    str_def = sf(find(stats, "str. def", "sig. str. def"))
    td_avg  = sf(find(stats, "td avg",  "takedowns avg"))
    td_acc  = sf(find(stats, "td acc",  "takedown acc"))
    td_def  = sf(find(stats, "td def",  "takedown def"))
    sub_avg = sf(find(stats, "sub. avg","submission avg"))

    str_s = calc_str(slpm, str_acc, str_def)
    wre_s = calc_wre(td_avg, td_acc, td_def)
    gnd_s = calc_gnd(sub_avg, td_acc, slpm)
    ois_s = calc_ois(str_s, wre_s, gnd_s)

    fights = get_fights(soup, str_s, wre_s, gnd_s)

    if not fights:
        return None
    if not is_active(fights):
        return None

    division = detect_division(soup, attrs)
    last     = fights[0]

    return {
        "id":       fid,
        "name":     name,
        "nickname": "",
        "division": division,
        "record":   record,
        "country":  "",
        "age":      None,
        "height":   attrs.get("height", ""),
        "reach":    attrs.get("reach",  ""),
        "stance":   attrs.get("stance", ""),
        "ois":      ois_s,
        "str":      str_s,
        "wre":      wre_s,
        "gnd":      gnd_s,
        "result":   last.get("result", "W"),
        "method":   last.get("method", "DEC"),
        "event":    last.get("event",  ""),
        "change":   "+0.0",
        "insight": (
            f"{name} — OIS auto-generated from UFCStats.com data. "
            "Edit this field in fighters.json to add a manual scouting note."
        ),
        "fights": fights,
    }


# ── URL CRAWLER (A-Z) ─────────────────────────────────────────

def crawl_urls():
    all_entries = []
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
                    all_entries.append({
                        "name": f"{first} {last}".strip(),
                        "url":  href,
                    })
                    count += 1

            print(f"  [{letter.upper()}] page {page}: {count} fighters")

            nxt = soup.select_one("a.b-statistics__paginate-item_next")
            if nxt and nxt.get("href"):
                page += 1
                time.sleep(random.uniform(0.8, 1.5))
            else:
                break

        time.sleep(random.uniform(0.5, 1.0))

    # Deduplicate by URL
    seen, unique = set(), []
    for e in all_entries:
        if e["url"] not in seen:
            seen.add(e["url"])
            unique.append(e)

    print(f"\n  Total unique URLs: {len(unique)}")
    return unique


# ── MAIN ──────────────────────────────────────────────────────

def main():
    start = datetime.now()
    print("=" * 62)
    print("  OctagonImpact Full Roster Scraper (Fixed)")
    print(f"  Started: {start.strftime('%I:%M %p')}")
    print(f"  Active window: last {ACTIVE_DAYS} days")
    print("=" * 62)

    urls    = crawl_urls()
    results = []
    skipped = 0
    fid     = 1

    print(f"\nPhase 2 — Scraping {len(urls)} profiles...")
    print("  Partial saves every 50 fighters.\n")

    for i, entry in enumerate(urls):
        pct = round((i + 1) / len(urls) * 100)
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

        time.sleep(random.uniform(2.0, 3.5))

        if (i + 1) % 50 == 0:
            elapsed = datetime.now() - start
            print(f"\n  ── {len(results)} active fighters so far")
            print(f"  ── Elapsed: {str(elapsed).split('.')[0]}")
            print(f"  ── Saving fighters_partial.json\n")
            with open("fighters_partial.json", "w", encoding="utf-8") as pf:
                json.dump(results, pf, indent=2, ensure_ascii=False)

    results.sort(key=lambda x: x["ois"], reverse=True)

    with open("fighters.json", "w", encoding="utf-8") as out:
        json.dump(results, out, indent=2, ensure_ascii=False)

    elapsed = datetime.now() - start
    div_counts = {}
    for f in results:
        div_counts[f["division"]] = div_counts.get(f["division"], 0) + 1

    print(f"\n{'=' * 62}")
    print(f"  COMPLETE!")
    print(f"  Active fighters saved:  {len(results)}")
    print(f"  Skipped:                {skipped}")
    print(f"  Total time:             {str(elapsed).split('.')[0]}")
    print(f"\n  Fighters by division:")
    for div in ALL_DIVISIONS + ["Unknown"]:
        if div in div_counts:
            print(f"    {div:<34} {div_counts[div]}")
    print(f"\n  Output: fighters.json")
    print(f"  Upload to GitHub — site updates in ~30 seconds.")
    print("=" * 62)

if __name__ == "__main__":
    main()
