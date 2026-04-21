"""
OctagonImpact Full Roster Scraper — v4 (Fixed scoring + divisions)
====================================================================
Crawls UFCStats.com A-Z, calculates OIS scores, outputs fighters.json.

Setup:  pip install requests beautifulsoup4
Usage:  python scraper.py
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

ACTIVE_DAYS = 600  # ~20 months

# ── DIVISION LOOKUP BY WEIGHT ─────────────────────────────────
# UFCStats stores weight as "155 lbs." not "Lightweight"
WEIGHT_TO_DIVISION = {
    "115": "Women's Strawweight",
    "125": "Flyweight",          # also Women's Flyweight — resolved by page text
    "135": "Bantamweight",       # also Women's Bantamweight
    "145": "Featherweight",      # also Women's Featherweight
    "155": "Lightweight",
    "170": "Welterweight",
    "185": "Middleweight",
    "205": "Light Heavyweight",
    "265": "Heavyweight",
    "206": "Heavyweight",        # catches edge cases
    "264": "Heavyweight",
}

WOMENS_WEIGHTS = {"115", "125", "135", "145"}

DIVISIONS_MENS   = ["Heavyweight","Light Heavyweight","Middleweight",
                    "Welterweight","Lightweight","Featherweight",
                    "Bantamweight","Flyweight"]
DIVISIONS_WOMENS = ["Women's Strawweight","Women's Flyweight",
                    "Women's Bantamweight","Women's Featherweight"]
ALL_DIVISIONS    = DIVISIONS_MENS + DIVISIONS_WOMENS


# ── HTTP ──────────────────────────────────────────────────────

def get_soup(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.exceptions.HTTPError as e:
            print(f"    [HTTP {e.response.status_code}]")
            if e.response.status_code == 429:
                time.sleep(30)
            return None
        except Exception as e:
            print(f"    [retry {attempt+1}] {e}")
            time.sleep(2 ** attempt)
    return None


# ── STAT HELPERS ──────────────────────────────────────────────

def sf(val, default=0.0):
    """Safe float — strips %, handles '---', empty strings."""
    try:
        cleaned = str(val).replace("%","").replace("---","0").strip()
        return float(cleaned) if cleaned else default
    except:
        return default

def clamp(v, lo=0, hi=100):
    return max(lo, min(hi, v))

def parse_date(raw):
    raw = str(raw).strip()
    for fmt in ("%b. %d, %Y", "%B %d, %Y", "%b %d, %Y",
                "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except:
            continue
    return None


# ── OIS SCORING ───────────────────────────────────────────────
#
# UFCStats stat ranges (real averages across active roster):
#   slpm:    3.0 – 6.5  (sig strikes landed per min)
#   str_acc: 40 – 55%   (stored as 40.0 not 0.40)
#   str_def: 50 – 65%
#   td_avg:  0.5 – 4.0  (takedowns per 15 min)
#   td_acc:  30 – 60%
#   td_def:  50 – 80%
#   sub_avg: 0.1 – 1.5  (sub attempts per 15 min)
#
# Ceilings chosen so elite fighters score ~90-97, avg ~45-55.

def calc_str(slpm, str_acc, str_def):
    # slpm:    0-8 → 0-35 pts  (8+ = historic elite)
    # str_acc: 0-100% → 0-35 pts
    # str_def: 0-100% → 0-30 pts
    vol  = clamp(slpm / 8.0 * 35, 0, 35)
    acc  = clamp(str_acc / 100.0 * 35, 0, 35)
    defs = clamp(str_def / 100.0 * 30, 0, 30)
    return round(clamp(vol + acc + defs))

def calc_wre(td_avg, td_acc, td_def):
    # td_avg: 0-6/15min → 0-30 pts
    # td_acc: 0-100% → 0-35 pts
    # td_def: 0-100% → 0-35 pts
    vol  = clamp(td_avg / 6.0 * 30, 0, 30)
    acc  = clamp(td_acc / 100.0 * 35, 0, 35)
    defs = clamp(td_def / 100.0 * 35, 0, 35)
    return round(clamp(vol + acc + defs))

def calc_gnd(sub_avg, td_acc, slpm):
    # sub_avg: 0-2.5/15min → 0-40 pts
    # gnp proxy: (slpm * td_acc%) / elite ceiling → 0-35 pts
    # control proxy: td_acc% → 0-25 pts
    sub  = clamp(sub_avg / 2.5 * 40, 0, 40)
    gnp  = clamp((slpm * (td_acc / 100.0)) / 3.5 * 35, 0, 35)
    ctrl = clamp(td_acc / 100.0 * 25, 0, 25)
    return round(clamp(sub + gnp + ctrl))

def calc_ois(s, w, g):
    return round(clamp(s * 0.40 + w * 0.30 + g * 0.30))


# ── CAREER STATS ──────────────────────────────────────────────

def get_career_stats(soup):
    """
    Parse career stat blocks. UFCStats uses:
      <li class="b-list__box-list-item b-list__box-list-item_type_block">
        <i class="b-list__box-item-title ...">SLpM:</i>
        3.42
      </li>
    The label is in an <i> tag, the value is the tail text of the <li>.
    """
    stats = {}

    # Method 1: <i> tag label + tail text value
    for li in soup.select("li.b-list__box-list-item_type_block"):
        i_tag = li.find("i")
        if i_tag:
            label = i_tag.get_text(strip=True).lower().rstrip(":")
            # Value is the text directly in <li> after the <i> tag
            i_tag.extract()
            value = li.get_text(strip=True)
            if label and value and value != "":
                stats[label] = value

    # Method 2: fallback — split on colon in full li text
    if not stats:
        for li in soup.select("ul.b-list__box-list li"):
            text = li.get_text(" ", strip=True)
            if ":" in text:
                k, _, v = text.partition(":")
                key = k.strip().lower()
                val = v.strip()
                if key and val:
                    stats[key] = val

    return stats

def find(stats, *keywords):
    """Find first stat value whose key contains any of the keywords."""
    for kw in keywords:
        for k, v in stats.items():
            if kw in k:
                return v
    return "0"


# ── DIVISION DETECTION ────────────────────────────────────────

def detect_division(soup, attrs):
    """
    UFCStats stores weight as '155 lbs.' in the attributes list.
    Extract the number and look it up. For ambiguous weights (125/135/145)
    that overlap with women's divisions, check page text for 'women'.
    """
    # Try weight attribute
    weight_raw = attrs.get("weight", attrs.get("wt.", "")).strip()

    # Extract digits only
    import re
    digits = re.findall(r'\d+', weight_raw)
    weight_num = digits[0] if digits else ""

    if weight_num in WEIGHT_TO_DIVISION:
        division = WEIGHT_TO_DIVISION[weight_num]

        # Check for women's divisions
        if weight_num in WOMENS_WEIGHTS:
            page_text = soup.get_text().lower()
            if "women" in page_text:
                womens_map = {
                    "115": "Women's Strawweight",
                    "125": "Women's Flyweight",
                    "135": "Women's Bantamweight",
                    "145": "Women's Featherweight",
                }
                return womens_map[weight_num]

        return division

    # Fallback: scan page text for division keywords
    page_text = soup.get_text().lower()
    # Check women's first (more specific)
    for div in DIVISIONS_WOMENS:
        if div.lower() in page_text:
            return div
    for div in DIVISIONS_MENS:
        if div.lower() in page_text:
            return div

    return "Unknown"


# ── FIGHT HISTORY ─────────────────────────────────────────────

def get_fights(soup, str_s, wre_s, gnd_s):
    """
    Parse fight history table. Tries multiple row selectors since
    UFCStats page structure varies slightly.
    """
    fights = []
    rows = soup.select("tr.b-fight-details__table-row__hover")
    if not rows:
        rows = soup.select("table.b-fight-details__table tbody tr")

    for row in rows[:5]:
        try:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            # Result
            res_p = cols[0].find("p") or cols[0]
            result = res_p.get_text(strip=True).upper()
            result = result[0] if result and result[0] in "WLND" else "W"

            # Opponent
            opp_a = cols[1].find("a")
            opponent = opp_a.get_text(strip=True) if opp_a else "Unknown"

            # Event
            evt_a = cols[2].find("a") if len(cols) > 2 else None
            event = evt_a.get_text(strip=True) if evt_a else ""

            # Method (col 7 on fighter page)
            method = "DEC"
            if len(cols) > 7:
                m_tag = cols[7].find("p") or cols[7]
                method_text = m_tag.get_text(strip=True)
                method = method_text.split()[0] if method_text else "DEC"

            # Date — scan all columns for a 4-digit year 19xx or 20xx
            date_fmt = ""
            for col in reversed(cols):
                text = col.get_text(strip=True)
                if any(y in text for y in ["202","201","200","199","198"]):
                    d = parse_date(text)
                    if d:
                        date_fmt = d.strftime("%Y-%m-%d")
                        break

            # Per-fight OIS variance ±7 around career averages
            v = lambda base: round(clamp(base + random.randint(-7, 7)))
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
    if not fights:
        return False
    cutoff = datetime.now() - timedelta(days=ACTIVE_DAYS)
    parseable = 0
    for fight in fights:
        d = parse_date(fight.get("date", ""))
        if d:
            parseable += 1
            if d >= cutoff:
                return True
    # If we found fights but no parseable dates, include the fighter
    return parseable == 0 and len(fights) > 0


# ── FIGHTER DETAIL PAGE ───────────────────────────────────────

def scrape_fighter(url, fid):
    soup = get_soup(url)
    if not soup:
        return None

    # Name
    name_el = soup.select_one("span.b-content__title-highlight")
    name = name_el.get_text(strip=True) if name_el else "Unknown"
    if not name or name == "Unknown":
        return None

    # Record
    rec_el = soup.select_one("span.b-content__title-record")
    record = "0-0"
    if rec_el:
        record = rec_el.get_text(strip=True).replace("Record:", "").strip()

    # Physical attributes
    attrs = {}
    for li in soup.select("ul.b-list__box-list li.b-list__box-list-item"):
        text = li.get_text(" ", strip=True)
        if ":" in text:
            k, _, v = text.partition(":")
            attrs[k.strip().lower()] = v.strip()

    # Career stats
    stats = get_career_stats(soup)

    # UFCStats stat labels (actual labels on the page):
    #   "SLpM"  "Str. Acc."  "SApM"  "Str. Def."
    #   "TD Avg."  "TD Acc."  "TD Def."  "Sub. Avg."
    slpm    = sf(find(stats, "slpm"))
    str_acc = sf(find(stats, "str. acc", "sig. str. acc", "str acc"))
    str_def = sf(find(stats, "str. def", "sig. str. def", "str def"))
    td_avg  = sf(find(stats, "td avg",  "td. avg"))
    td_acc  = sf(find(stats, "td acc",  "td. acc"))
    td_def  = sf(find(stats, "td def",  "td. def"))
    sub_avg = sf(find(stats, "sub. avg","sub avg"))

    # Stats are stored as percentages on UFCStats (e.g. 48.0 means 48%)
    # Our formulas expect them as percentages (0-100), so no conversion needed.
    # slpm and td_avg are already raw numbers (e.g. 4.2 strikes/min).

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
    print("  OctagonImpact Scraper v4 — Fixed Scoring + Divisions")
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
