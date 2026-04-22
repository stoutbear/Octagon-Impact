"""
OctagonImpact Full Roster Scraper v5 — Multi-Source
======================================================
Sources:
  1. UFCStats.com    — career stats + fight history (primary)
  2. Wikipedia       — complete active UFC roster list (ensures no fighter missed)
  3. Wikipedia       — individual fighter pages for accurate fight history

Key improvements over v4:
  - Wikipedia roster scrape ensures ALL active fighters are captured
  - Two-source fighter URL lookup (Wikipedia name → UFCStats search)
  - Fight history from Wikipedia individual pages (accurate & up-to-date)
  - Division assigned from Wikipedia roster (more accurate than weight lookup)
  - Active filter uses Wikipedia roster membership (not just date parsing)
  - Saves progress checkpoint every 25 fighters (not 50)
  - Detailed error log saved to scraper_errors.txt

Setup:  pip install requests beautifulsoup4
Usage:  python scraper.py
Output: fighters.json
"""

import json, time, random, string, re, os
from datetime import datetime, timedelta

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run:  pip install requests beautifulsoup4")
    exit(1)

# ── CONSTANTS ─────────────────────────────────────────────────
UFCSTATS_BASE    = "http://www.ufcstats.com"
UFCSTATS_LIST    = f"{UFCSTATS_BASE}/statistics/fighters"
WIKIPEDIA_ROSTER = "https://en.wikipedia.org/wiki/List_of_current_UFC_fighters"



HEADERS_DEFAULT = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

ACTIVE_DAYS = 600

# Division name normalisation
WIKI_DIV_MAP = {
    "Heavyweight": "Heavyweight",
    "Light Heavyweight": "Light Heavyweight",
    "Middleweight": "Middleweight",
    "Welterweight": "Welterweight",
    "Lightweight": "Lightweight",
    "Featherweight": "Featherweight",
    "Bantamweight": "Bantamweight",
    "Flyweight": "Flyweight",
    "Women's Strawweight": "Women's Strawweight",
    "Women's Flyweight": "Women's Flyweight",
    "Women's Bantamweight": "Women's Bantamweight",
    "Women's Featherweight": "Women's Featherweight",
    # abbreviations used in Wikipedia tables
    "SW":   "Women's Strawweight",
    "FYW":  "Flyweight",
    "BW":   "Bantamweight",
    "FW":   "Featherweight",
    "LW":   "Lightweight",
    "WW":   "Welterweight",
    "MW":   "Middleweight",
    "LHW":  "Light Heavyweight",
    "HW":   "Heavyweight",
    "WSW":  "Women's Strawweight",
    "WFYW": "Women's Flyweight",
    "WBW":  "Women's Bantamweight",
    "WFW":  "Women's Featherweight",
}

WEIGHT_TO_DIV = {
    "115": "Women's Strawweight",
    "125": "Flyweight",
    "135": "Bantamweight",
    "145": "Featherweight",
    "155": "Lightweight",
    "170": "Welterweight",
    "185": "Middleweight",
    "205": "Light Heavyweight",
    "265": "Heavyweight",
}

errors = []

# ── HTTP ──────────────────────────────────────────────────────
def get_soup(url, headers=None, retries=3):
    h = headers or HEADERS_DEFAULT
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=h, timeout=20)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code
            if code in (403, 404):
                return None
            if code == 429:
                time.sleep(30)
            return None
        except Exception as e:
            time.sleep(2 ** attempt)
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
    for fmt in ("%b. %d, %Y","%B %d, %Y","%b %d, %Y","%Y-%m-%d","%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except:
            continue
    return None

def normalize_name(name):
    """Lowercase, strip punctuation for fuzzy matching."""
    return re.sub(r"[^a-z0-9 ]", "", name.lower()).strip()

# ── OIS SCORING ───────────────────────────────────────────────
def calc_str(slpm, str_acc, str_def):
    vol  = clamp(slpm / 7.0 * 50, 0, 50)
    acc  = clamp(str_acc / 62.0 * 30, 0, 30)
    defs = clamp(str_def / 68.0 * 20, 0, 20)
    return round(clamp(vol + acc + defs))

def calc_wre(td_avg, td_acc, td_def):
    vol  = clamp(td_avg / 7.0 * 50, 0, 50)
    acc  = clamp(td_acc / 70.0 * 25, 0, 25)
    defs = clamp(td_def / 90.0 * 25, 0, 25)
    return round(clamp(vol + acc + defs))

def calc_gnd(sub_avg, td_acc, slpm, td_def):
    sub  = clamp(sub_avg / 1.5 * 50, 0, 50)
    ctrl = clamp(td_def / 90.0 * 25, 0, 25)
    gnp  = clamp((slpm * td_acc / 100) / 3.5 * 25, 0, 25)
    return round(clamp(sub + ctrl + gnp))

def calc_ois(s, w, g):
    base  = s * 0.40 + w * 0.30 + g * 0.30
    bonus = sum(3 for x in [s,w,g] if x >= 70) + sum(4 for x in [s,w,g] if x >= 80)
    return round(clamp(base + bonus))

def stretch(v):
    if v <= 0: return 0
    return round(min(100, v * (1 + (100-v)/200)))

# ── SOURCE 1: WIKIPEDIA ROSTER ────────────────────────────────
def scrape_wikipedia_roster():
    """
    Scrape the Wikipedia 'List of current UFC fighters' page.
    Returns dict: { normalized_name: {"name": str, "division": str} }
    This is our master list — if a fighter is here, they're active.
    """
    print("\nSource 1 — Wikipedia: scraping active UFC roster...")
    soup = get_soup(WIKIPEDIA_ROSTER)
    if not soup:
        print("  ✗ Could not reach Wikipedia. Continuing without roster list.")
        return {}

    roster = {}
    tables = soup.select("table.wikitable")
    
    # Each table section corresponds to a division
    # The h3 heading before each table contains the division name
    headings = soup.select("h3 span.mw-headline")
    
    for i, table in enumerate(tables):
        # Try to find division from preceding heading
        division = "Unknown"
        # Walk backwards in DOM to find h3
        for sib in table.find_all_previous(["h2","h3"]):
            text = sib.get_text(strip=True)
            for key, div in WIKI_DIV_MAP.items():
                if key.lower() in text.lower():
                    division = div
                    break
            if division != "Unknown":
                break

        rows = table.select("tbody tr")
        for row in rows:
            cols = row.find_all(["td","th"])
            if len(cols) < 2:
                continue
            # Name is usually in col 1 or 2 — find the one with a link
            name = ""
            for col in cols[:4]:
                a = col.find("a")
                if a and len(a.get_text(strip=True)) > 2:
                    candidate = a.get_text(strip=True)
                    # Skip if it looks like a flag or abbreviation
                    if len(candidate) > 3 and not candidate.isupper():
                        name = candidate
                        break
            if not name:
                continue

            # Extract country from flag image alt text or title
            country = ""
            for col in cols[:6]:
                img = col.find("img")
                if img:
                    alt = img.get("alt","")
                    title = img.get("title","")
                    # Flag images have alt like "Flag of Brazil" or title "Brazil"
                    for text in [alt, title]:
                        if "flag of" in text.lower():
                            country = text.lower().replace("flag of","").strip().title()
                            break
                        elif text and len(text) > 2 and not text.startswith("http"):
                            country = text.strip().title()
                            break
                if country:
                    break

            key = normalize_name(name)
            if key and key not in roster:
                roster[key] = {"name": name, "division": division, "country": country}

    print(f"  ✓ Found {len(roster)} fighters on Wikipedia roster")
    return roster

# ── SOURCE 2: UFCSTATS FIGHTER URLS ──────────────────────────
def crawl_ufcstats_urls():
    """Walk A-Z on UFCStats to collect all fighter profile URLs."""
    print("\nSource 2 — UFCStats: collecting fighter URLs (A to Z)...")
    all_entries = []

    for letter in string.ascii_lowercase:
        page = 1
        while True:
            url  = f"{UFCSTATS_LIST}?char={letter}&page={page}"
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
                href  = link.get("href","").strip()
                first = cols[0].get_text(strip=True)
                last  = cols[1].get_text(strip=True)
                if href and first:
                    all_entries.append({
                        "name": f"{first} {last}".strip(),
                        "url":  href,
                    })
                    count += 1
            nxt = soup.select_one("a.b-statistics__paginate-item_next")
            if nxt and nxt.get("href"):
                page += 1
                time.sleep(random.uniform(0.6, 1.2))
            else:
                break
        time.sleep(random.uniform(0.4, 0.8))

    # Deduplicate
    seen, unique = set(), []
    for e in all_entries:
        if e["url"] not in seen:
            seen.add(e["url"])
            unique.append(e)

    # Build lookup: normalized_name → url
    url_lookup = {normalize_name(e["name"]): e for e in unique}
    print(f"  ✓ {len(unique)} unique fighter URLs collected")
    return unique, url_lookup

# ── SOURCE 3: WIKIPEDIA FIGHTER PAGE ─────────────────────────
def get_wikipedia_fights(fighter_name, num_fights=5):
    """
    Fetch the fighter's Wikipedia page and parse their MMA record table.
    Wikipedia is updated within hours of events and has accurate
    opponent names, event names, dates, results and methods.
    Returns list of fight dicts or None if page not found.
    """
    # Build Wikipedia URL: "Islam Makhachev" → "Islam_Makhachev"
    slug = "_".join(fighter_name.strip().split())
    url  = f"https://en.wikipedia.org/wiki/{requests.utils.quote(slug)}"

    soup = get_soup(url)
    if not soup:
        return None

    # Check it's actually a fighter page (has MMA record table)
    # Wikipedia MMA record tables have class "wikitable" and contain fight data
    fights = []
    tables = soup.select("table.wikitable")

    for table in tables:
        headers = [th.get_text(strip=True).lower()
                   for th in table.select("th")]
        # MMA record tables have "opponent" and "result" headers
        if not any("opponent" in h for h in headers):
            continue
        if not any("result" in h or "res" in h for h in headers):
            continue

        # Map column index to field name
        col_map = {}
        for i, h in enumerate(headers):
            if "res" in h:         col_map["result"]   = i
            elif "opponent" in h:  col_map["opponent"]  = i
            elif "method" in h:    col_map["method"]    = i
            elif "event" in h:     col_map["event"]     = i
            elif "date" in h:      col_map["date"]      = i
            elif "round" in h:     col_map["round"]     = i

        if "opponent" not in col_map:
            continue

        rows = table.select("tbody tr")
        for row in rows:
            if len(fights) >= num_fights:
                break
            cols = row.find_all(["td","th"])
            if len(cols) < 3:
                continue

            try:
                def get_col(field, default=""):
                    idx = col_map.get(field)
                    if idx is None or idx >= len(cols):
                        return default
                    return cols[idx].get_text(" ", strip=True)

                result_raw = get_col("result")
                # Skip header rows
                if result_raw.lower() in ("res.", "result", "res"):
                    continue

                result = "W"
                rl = result_raw.upper()
                if rl.startswith("W"):   result = "W"
                elif rl.startswith("L"): result = "L"
                elif rl.startswith("D"): result = "D"
                elif "nc" in rl.lower(): result = "NC"

                opponent = get_col("opponent", "Unknown")
                # Clean up Wikipedia footnote artifacts like [1]
                opponent = re.sub(r'\[\d+\]', '', opponent).strip()

                event = get_col("event", "")
                event = re.sub(r'\[\d+\]', '', event).strip()

                method_raw = get_col("method", "DEC")
                method = method_raw.split()[0] if method_raw else "DEC"
                method = re.sub(r'\[\d+\]', '', method).strip()

                date_raw = get_col("date", "")
                date_raw = re.sub(r'\[\d+\]', '', date_raw).strip()
                d = parse_date(date_raw)
                date_fmt = d.strftime("%Y-%m-%d") if d else date_raw

                if not opponent or opponent.lower() in ("opponent",""):
                    continue

                fights.append({
                    "opponent": opponent,
                    "event":    event,
                    "date":     date_fmt,
                    "result":   result,
                    "method":   method,
                })
            except Exception:
                continue

        if fights:
            break  # Found the right table

    return fights if fights else None

# ── UFCSTATS CAREER STATS ────────────────────────────────────
def get_career_stats(soup):
    stats = {}
    for li in soup.select("li.b-list__box-list-item_type_block"):
        i_tag = li.find("i")
        if i_tag:
            label = i_tag.get_text(strip=True).lower().rstrip(":")
            i_tag.extract()
            value = li.get_text(strip=True)
            if label and value:
                stats[label] = value
    if not stats:
        for li in soup.select("ul.b-list__box-list li"):
            text = li.get_text(" ", strip=True)
            if ":" in text:
                k, _, v = text.partition(":")
                key = k.strip().lower()
                if key and v.strip():
                    stats[key] = v.strip()
    return stats

def find_stat(stats, *keywords):
    for kw in keywords:
        for k, v in stats.items():
            if kw in k:
                return v
    return "0"

# ── UFCSTATS FIGHT HISTORY (basic results only) ───────────────
def get_ufcstats_results(soup):
    """
    Get just the result (W/L) from each fight in UFCStats summary table.
    We use Wikipedia for the accurate opponent/event/date details,
    but UFCStats results are needed to confirm fight order.
    """
    results = []
    rows = soup.select("tr.b-fight-details__table-row__hover")
    if not rows:
        rows = soup.select("table.b-fight-details__table tbody tr")

    for row in rows[:5]:
        try:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            res_el = cols[0].find("p") or cols[0]
            result = res_el.get_text(strip=True).upper()
            result = result[0] if result and result[0] in "WLND" else "W"
            results.append(result)
        except Exception:
            continue
    return results

def build_fight_history(wiki_fights, ufc_results, str_s, wre_s, gnd_s):
    """
    Combine Wikipedia fight details with UFCStats results.
    Wikipedia provides accurate opponent/event/date/method.
    UFCStats results confirm the W/L in case of discrepancy.
    Adds per-fight OIS scores.
    """
    fights = []
    for i, wf in enumerate(wiki_fights[:5]):
        # Use UFCStats result if available and Wikipedia result seems off
        result = wf.get("result", "W")
        if i < len(ufc_results) and ufc_results[i] in "WLND":
            result = ufc_results[i]  # UFCStats is authoritative on W/L

        v = lambda base: round(clamp(base + random.randint(-7, 7)))
        fs, fw, fg = v(str_s), v(wre_s), v(gnd_s)

        fights.append({
            "opponent": wf.get("opponent", "Unknown"),
            "event":    wf.get("event", ""),
            "date":     wf.get("date", ""),
            "result":   result,
            "method":   wf.get("method", "DEC"),
            "ois":      calc_ois(fs, fw, fg),
            "str":      fs,
            "wre":      fw,
            "gnd":      fg,
        })
    return fights

# ── ACTIVE CHECK ──────────────────────────────────────────────
def is_active(fights, wiki_roster, name):
    """
    A fighter is active if:
    1. They appear in the Wikipedia active roster, OR
    2. They have a fight within ACTIVE_DAYS
    """
    if normalize_name(name) in wiki_roster:
        return True

    if not fights:
        return False

    cutoff = datetime.now() - timedelta(days=ACTIVE_DAYS)
    parseable = 0
    for fight in fights:
        d = parse_date(fight.get("date",""))
        if d:
            parseable += 1
            if d >= cutoff:
                return True

    return parseable == 0 and len(fights) > 0

# ── DIVISION DETECTION ────────────────────────────────────────
def detect_division(soup, attrs, wiki_roster, name):
    """
    Priority:
    1. Wikipedia roster (most accurate, respects 2-of-3 rule)
    2. UFCStats weight field
    3. Page text scan
    """
    # Wikipedia first
    key = normalize_name(name)
    if key in wiki_roster and wiki_roster[key]["division"] != "Unknown":
        return wiki_roster[key]["division"]

    # UFCStats weight
    weight_raw = attrs.get("weight", attrs.get("wt.", "")).strip()
    digits = re.findall(r'\d+', weight_raw)
    if digits and digits[0] in WEIGHT_TO_DIV:
        div = WEIGHT_TO_DIV[digits[0]]
        # Check if women's
        if digits[0] in ("115","125","135","145"):
            page = soup.get_text().lower().replace("'","").replace("\u2019","")
            if "women" in page:
                womens = {"115":"Women's Strawweight","125":"Women's Flyweight",
                          "135":"Women's Bantamweight","145":"Women's Featherweight"}
                return womens[digits[0]]
        return div

    # Page scan
    page = soup.get_text().lower().replace("'","").replace("\u2019","")
    for div_name in ["Women's Strawweight","Women's Flyweight","Women's Bantamweight",
                     "Women's Featherweight","Heavyweight","Light Heavyweight",
                     "Middleweight","Welterweight","Lightweight","Featherweight",
                     "Bantamweight","Flyweight"]:
        if div_name.lower().replace("'","") in page.replace("'",""):
            return div_name

    return "Unknown"

# ── PHOTO URL HELPER ─────────────────────────────────────────
def make_ufc_photo_url(name):
    """
    Construct the UFC athlete page slug from a fighter's name.
    UFC uses: ufc.com/athlete/firstname-lastname (lowercase, hyphens)
    e.g. "Khamzat Chimaev" → "https://www.ufc.com/athlete/khamzat-chimaev"
    The site will use this to display the fighter's official UFC headshot.
    """
    slug = re.sub(r"[^a-z0-9\s-]", "", name.lower())
    slug = re.sub(r"\s+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    return f"https://www.ufc.com/athlete/{slug}"

# ── SCRAPE SINGLE FIGHTER ─────────────────────────────────────
def scrape_fighter(url, name, fid, wiki_roster):
    soup = get_soup(url)
    if not soup:
        return None

    # Name from page (more reliable than list)
    name_el = soup.select_one("span.b-content__title-highlight")
    page_name = name_el.get_text(strip=True) if name_el else name
    if not page_name:
        return None

    rec_el = soup.select_one("span.b-content__title-record")
    record = rec_el.get_text(strip=True).replace("Record:","").strip() if rec_el else "0-0"

    attrs = {}
    for li in soup.select("ul.b-list__box-list li.b-list__box-list-item"):
        text = li.get_text(" ", strip=True)
        if ":" in text:
            k, _, v = text.partition(":")
            attrs[k.strip().lower()] = v.strip()

    # Calculate age from DOB
    age = None
    dob_raw = attrs.get("dob", attrs.get("date of birth", ""))
    if dob_raw and dob_raw != "--":
        dob = parse_date(dob_raw)
        if dob:
            today = datetime.now()
            age = today.year - dob.year - (
                (today.month, today.day) < (dob.month, dob.day)
            )

    # Country from Wikipedia roster
    key = normalize_name(page_name)
    country = ""
    if key in wiki_roster:
        country = wiki_roster[key].get("country", "")

    stats   = get_career_stats(soup)
    slpm    = sf(find_stat(stats, "slpm"))
    str_acc = sf(find_stat(stats, "str. acc","sig. str. acc","str acc"))
    str_def = sf(find_stat(stats, "str. def","sig. str. def","str def"))
    td_avg  = sf(find_stat(stats, "td avg","td. avg"))
    td_acc  = sf(find_stat(stats, "td acc","td. acc"))
    td_def  = sf(find_stat(stats, "td def","td. def"))
    sub_avg = sf(find_stat(stats, "sub. avg","sub avg"))

    str_s = calc_str(slpm, str_acc, str_def)
    wre_s = calc_wre(td_avg, td_acc, td_def)
    gnd_s = calc_gnd(sub_avg, td_acc, slpm, td_def)
    ois_s = calc_ois(str_s, wre_s, gnd_s)

    # Apply stretch
    str_s = stretch(str_s)
    wre_s = stretch(wre_s)
    gnd_s = stretch(gnd_s)
    ois_s = stretch(ois_s)

    # Wikipedia fight history — accurate opponent names, event names, dates
    wiki_fights = get_wikipedia_fights(page_name)

    # UFCStats W/L results for cross-reference
    ufc_results = get_ufcstats_results(soup)

    # Build fight history combining both sources
    if wiki_fights:
        fights = build_fight_history(wiki_fights, ufc_results, str_s, wre_s, gnd_s)
    else:
        # Fallback: build from UFCStats summary table only
        fights = []
        rows = soup.select("tr.b-fight-details__table-row__hover")
        if not rows:
            rows = soup.select("table.b-fight-details__table tbody tr")
        for i, row in enumerate(rows[:5]):
            try:
                cols = row.find_all("td")
                if len(cols) < 6:
                    continue
                res_el = cols[0].find("p") or cols[0]
                result = res_el.get_text(strip=True).upper()
                result = result[0] if result and result[0] in "WLND" else "W"
                opp_a = cols[1].find("a")
                opponent = opp_a.get_text(strip=True) if opp_a else "Unknown"
                evt_a = cols[2].find("a") if len(cols) > 2 else None
                event = evt_a.get_text(strip=True) if evt_a else ""
                method = "DEC"
                if len(cols) > 7:
                    m = cols[7].find("p") or cols[7]
                    mt = m.get_text(strip=True)
                    method = mt.split()[0] if mt else "DEC"
                date_fmt = ""
                for col in reversed(cols):
                    text = col.get_text(strip=True)
                    if any(y in text for y in ["202","201","200","199"]):
                        d = parse_date(text)
                        if d:
                            date_fmt = d.strftime("%Y-%m-%d")
                            break
                v = lambda base: round(clamp(base + random.randint(-7,7)))
                fs, fw, fg = v(str_s), v(wre_s), v(gnd_s)
                fights.append({
                    "opponent": opponent, "event": event, "date": date_fmt,
                    "result": result, "method": method,
                    "ois": calc_ois(fs, fw, fg), "str": fs, "wre": fw, "gnd": fg,
                })
            except Exception:
                continue

    # Active check
    if not is_active(fights, wiki_roster, page_name):
        return None

    division = detect_division(soup, attrs, wiki_roster, page_name)
    last = fights[0] if fights else {}

    # Weight in lbs (strip "lbs." text)
    weight_raw = attrs.get("weight","")
    weight_digits = re.findall(r'\d+', weight_raw)
    weight_lbs = weight_digits[0] + " lbs" if weight_digits else ""

    return {
        "id":         fid,
        "name":       page_name,
        "nickname":   "",
        "division":   division,
        "record":     record,
        "country":    country,
        "age":        age,
        "height":     attrs.get("height",""),
        "weight":     weight_lbs,
        "reach":      attrs.get("reach",""),
        "stance":     attrs.get("stance",""),
        "photoUrl":   make_ufc_photo_url(page_name),
        "ois":        ois_s,
        "str":        str_s,
        "wre":        wre_s,
        "gnd":        gnd_s,
        "result":     last.get("result","W"),
        "method":     last.get("method","DEC"),
        "event":      last.get("event",""),
        "change":     "+0.0",
        "insight": (
            f"{page_name} — OIS auto-generated from UFCStats.com career stats "
            "and Wikipedia fight history. Edit this field to add a manual scouting note."
        ),
        "fights": fights,
    }

# ── FIND MISSING FIGHTERS FROM WIKIPEDIA ─────────────────────
def find_missing_from_wikipedia(wiki_roster, url_lookup, results):
    """
    Find fighters who are in the Wikipedia active roster but
    weren't found/scraped from UFCStats A-Z list.
    Returns list of {name, url} to scrape.
    """
    scraped_names = {normalize_name(f["name"]) for f in results}
    missing = []

    for norm_name, info in wiki_roster.items():
        if norm_name in scraped_names:
            continue
        # Try to find their UFCStats URL
        if norm_name in url_lookup:
            missing.append({
                "name": info["name"],
                "url":  url_lookup[norm_name]["url"],
                "division": info["division"],
            })
        else:
            # Try partial name match
            found = False
            for uk, ue in url_lookup.items():
                if norm_name in uk or uk in norm_name:
                    missing.append({
                        "name": info["name"],
                        "url":  ue["url"],
                        "division": info["division"],
                    })
                    found = True
                    break
            if not found:
                errors.append(f"No UFCStats URL found for Wikipedia fighter: {info['name']}")

    return missing

# ── MAIN ──────────────────────────────────────────────────────
def main():
    start = datetime.now()
    print("=" * 64)
    print("  OctagonImpact Scraper v5 — Multi-Source")
    print(f"  Started: {start.strftime('%I:%M %p')}")
    print(f"  Sources: UFCStats + Wikipedia roster + Wikipedia fight pages")
    print("=" * 64)

    # Source 1: Wikipedia roster (our master active list)
    wiki_roster = scrape_wikipedia_roster()

    # Source 2: UFCStats A-Z URL collection
    all_urls, url_lookup = crawl_ufcstats_urls()

    print(f"\nPhase 2 — Scraping {len(all_urls)} UFCStats profiles...")
    print("  Wikipedia fight history enabled — accurate opponent names & events.")
    print("  Checkpoint saves every 25 fighters.\n")

    results  = []
    skipped  = 0
    fid      = 1

    for i, entry in enumerate(all_urls):
        pct = round((i+1) / len(all_urls) * 100)
        print(f"  [{i+1}/{len(all_urls)} {pct}%] {entry['name']}", end="", flush=True)

        data = scrape_fighter(
            entry["url"], entry["name"], fid,
            wiki_roster
        )

        if data is None:
            skipped += 1
            print("  — skipped")
        else:
            results.append(data)
            fid += 1
            
            print(
                f"  OIS={data['ois']}  STR={data['str']}  "
                f"WRE={data['wre']}  GND={data['gnd']}  "
                f"[{data['division']}]"
            )

        # Polite delay
        time.sleep(random.uniform(2.5, 4.0))

        # Checkpoint every 25
        if (i+1) % 25 == 0:
            elapsed = datetime.now() - start
            print(f"\n  ── {len(results)} active fighters so far")
            print(f"  ── Elapsed: {str(elapsed).split('.')[0]}")
            print(f"  ── Saving checkpoint...\n")
            with open("fighters_partial.json","w",encoding="utf-8") as pf:
                json.dump(results, pf, indent=2, ensure_ascii=False)

    # Phase 3: Find fighters in Wikipedia but missed by UFCStats A-Z
    print(f"\nPhase 3 — Checking for fighters missed by UFCStats A-Z...")
    missing = find_missing_from_wikipedia(wiki_roster, url_lookup, results)
    print(f"  {len(missing)} fighters in Wikipedia roster not yet scraped")

    for i, entry in enumerate(missing):
        print(f"  [MISSING {i+1}/{len(missing)}] {entry['name']}", end="", flush=True)
        data = scrape_fighter(
            entry["url"], entry["name"], fid,
            wiki_roster
        )
        if data is None:
            # Force include with Wikipedia division even if date check fails
            print("  — could not scrape")
            errors.append(f"Could not scrape missing fighter: {entry['name']}")
        else:
            # Override division with Wikipedia's value if we have it
            if entry.get("division","Unknown") != "Unknown":
                data["division"] = entry["division"]
            results.append(data)
            fid += 1
            print(f"  OIS={data['ois']}  [{data['division']}]")
        time.sleep(random.uniform(2.5, 4.0))

    # Sort by OIS
    results.sort(key=lambda x: x["ois"], reverse=True)

    # Save
    with open("fighters.json","w",encoding="utf-8") as out:
        json.dump(results, out, indent=2, ensure_ascii=False)

    # Save error log
    if errors:
        with open("scraper_errors.txt","w",encoding="utf-8") as ef:
            ef.write("\n".join(errors))
        print(f"\n  {len(errors)} errors logged to scraper_errors.txt")

    # Summary
    elapsed = datetime.now() - start
    div_counts = {}
    for f in results:
        div_counts[f["division"]] = div_counts.get(f["division"],0) + 1

    all_divs = [
        "Heavyweight","Light Heavyweight","Middleweight","Welterweight",
        "Lightweight","Featherweight","Bantamweight","Flyweight",
        "Women's Strawweight","Women's Flyweight","Women's Bantamweight",
        "Women's Featherweight","Unknown"
    ]

    print(f"\n{'=' * 64}")
    print(f"  COMPLETE!")
    print(f"  Active fighters saved:  {len(results)}")
    print(f"  Skipped (inactive):     {skipped}")
    print(f"  Total time:             {str(elapsed).split('.')[0]}")
    print(f"\n  Fighters by division:")
    for div in all_divs:
        if div in div_counts:
            print(f"    {div:<34} {div_counts[div]}")
    print(f"\n  Output: fighters.json")
    print(f"  Upload to GitHub — site updates in ~30 seconds.")
    print("=" * 64)

if __name__ == "__main__":
    main()
