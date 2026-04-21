"""
OctagonImpact Fighter Scraper
==============================
Scrapes fighter stats from UFCStats.com and outputs fighters.json
formatted for the OctagonImpact website.

Setup (run once):
    pip install requests beautifulsoup4

Usage:
    python scraper.py

Output:
    fighters.json  — drop this into your website folder and upload to GitHub.
"""

import json
import time
import re
import random
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run: pip install requests beautifulsoup4")
    exit(1)

BASE_URL = "http://www.ufcstats.com"
FIGHTER_LIST_URL = f"{BASE_URL}/statistics/fighters"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ──────────────────────────────────────────────────────────────
# OIS CALCULATION
# Weights: STR=0.40, WRE=0.30, GND=0.30
# All inputs are raw UFCStats stats normalized to 0-100 scale.
# ──────────────────────────────────────────────────────────────

def clamp(val, lo=0, hi=100):
    return max(lo, min(hi, val))

def safe_float(val, default=0.0):
    try:
        return float(str(val).replace("%", "").strip())
    except (ValueError, TypeError):
        return default

def calc_striking_impact(stats):
    """
    Striking Impact (0-100):
    - Sig strike accuracy:    30 pts  (pct / 100 * 30)
    - Sig strikes landed/min: 25 pts  (normalized vs division avg ~4.5/min)
    - Head strike defense:    25 pts  (pct / 100 * 25)
    - Knockdown rate:         20 pts  (KD per 15 min, capped at 1.0/15)
    """
    acc     = safe_float(stats.get("sig_str_acc", 0))
    slpm    = safe_float(stats.get("slpm", 0))
    def_pct = safe_float(stats.get("str_def", 0))
    kd      = safe_float(stats.get("kd", 0))

    acc_score  = clamp(acc / 100 * 30, 0, 30)
    slpm_norm  = clamp(slpm / 9.0 * 25, 0, 25)   # 9 slpm = elite ceiling
    def_score  = clamp(def_pct / 100 * 25, 0, 25)
    kd_norm    = clamp(kd / 1.0 * 20, 0, 20)      # 1 KD per fight = elite

    raw = acc_score + slpm_norm + def_score + kd_norm
    return round(clamp(raw))

def calc_wrestling_impact(stats):
    """
    Wrestling Impact (0-100):
    - Takedown accuracy:  35 pts
    - Takedown defense:   35 pts
    - TD attempts/15 min: 30 pts (volume matters — relentless wrestlers score high)
    """
    td_acc  = safe_float(stats.get("td_acc", 0))
    td_def  = safe_float(stats.get("td_def", 0))
    td_avg  = safe_float(stats.get("td_avg", 0))  # per 15 min

    acc_score  = clamp(td_acc / 100 * 35, 0, 35)
    def_score  = clamp(td_def / 100 * 35, 0, 35)
    vol_score  = clamp(td_avg / 8.0 * 30, 0, 30)  # 8 TDs/15min = elite ceiling

    raw = acc_score + def_score + vol_score
    return round(clamp(raw))

def calc_ground_impact(stats):
    """
    Ground Impact (0-100):
    - Sub attempts/15 min: 40 pts (threat rate)
    - Sig strikes landed while in control: 30 pts (GnP proxy via slpm * td_acc)
    - Overall control time proxy:          30 pts
    NOTE: UFCStats doesn't expose all ground stats directly.
          This uses available proxies and will improve with more granular data.
    """
    sub_avg = safe_float(stats.get("sub_avg", 0))   # sub attempts per 15 min
    td_acc  = safe_float(stats.get("td_acc", 0))
    slpm    = safe_float(stats.get("slpm", 0))

    sub_score  = clamp(sub_avg / 3.0 * 40, 0, 40)   # 3 sub att/15min = elite
    gnp_proxy  = clamp((slpm * td_acc / 100) / 4.0 * 30, 0, 30)
    ctrl_proxy = clamp(td_acc / 100 * 30, 0, 30)

    raw = sub_score + gnp_proxy + ctrl_proxy
    return round(clamp(raw))

def calc_ois(str_score, wre_score, gnd_score, opp_adj=1.0):
    """
    Overall OIS = weighted average * opponent adjustment factor.
    opp_adj range: 0.85 (weak opponents) to 1.15 (elite opponents).
    Default 1.0 = average opposition (sufficient for basic scraping).
    """
    raw = (str_score * 0.40) + (wre_score * 0.30) + (gnd_score * 0.30)
    return round(clamp(raw * opp_adj))

# ──────────────────────────────────────────────────────────────
# SCRAPING
# ──────────────────────────────────────────────────────────────

def get_soup(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"  [retry {attempt+1}] {url} — {e}")
            time.sleep(2 ** attempt)
    return None

def scrape_fighter_list(letter="a", page=1):
    """Get all fighters starting with a given letter."""
    url = f"{FIGHTER_LIST_URL}?char={letter}&page={page}"
    soup = get_soup(url)
    if not soup:
        return []

    rows = soup.select("table.b-statistics__table tbody tr.b-statistics__table-row")
    fighters = []
    for row in rows:
        cols = row.select("td")
        if len(cols) < 2:
            continue
        link_tag = cols[0].find("a") or cols[1].find("a")
        if not link_tag:
            continue
        href = link_tag.get("href", "")
        first = cols[0].get_text(strip=True)
        last  = cols[1].get_text(strip=True)
        if first and last and href:
            fighters.append({"name": f"{first} {last}", "url": href})
    return fighters

def scrape_fighter_detail(url):
    """Scrape individual fighter stats page."""
    soup = get_soup(url)
    if not soup:
        return None

    # Name
    name_el = soup.select_one("span.b-content__title-highlight")
    name = name_el.get_text(strip=True) if name_el else "Unknown"

    # Record
    record_el = soup.select_one("span.b-content__title-record")
    record = "0-0"
    if record_el:
        record = record_el.get_text(strip=True).replace("Record:", "").strip()

    # Attributes (height, weight, reach, stance, DoB)
    info_boxes = soup.select("ul.b-list__box-list li.b-list__box-list-item")
    attrs = {}
    for li in info_boxes:
        text = li.get_text(" ", strip=True)
        if ":" in text:
            k, _, v = text.partition(":")
            attrs[k.strip().lower()] = v.strip()

    # Stats
    stat_rows = soup.select("li.b-list__box-list-item_type_block")
    stats = {}
    for li in stat_rows:
        text = li.get_text(" ", strip=True)
        if ":" in text:
            k, _, v = text.partition(":")
            key = k.strip().lower().replace(" ", "_").replace(".", "")
            stats[key] = v.strip()

    # Parse stats into known keys
    def get_stat(raw_key, default="0"):
        for k in stats:
            if raw_key in k:
                return stats[k]
        return default

    parsed = {
        "slpm":       get_stat("slpm"),
        "sig_str_acc": get_stat("str_acc"),
        "sapm":       get_stat("sapm"),
        "str_def":    get_stat("str_def"),
        "td_avg":     get_stat("td_avg"),
        "td_acc":     get_stat("td_acc"),
        "td_def":     get_stat("td_def"),
        "sub_avg":    get_stat("sub_avg"),
    }

    # Fights history (last 5)
    fight_rows = soup.select("table.b-fight-details__table tbody tr.b-fight-details__table-row")
    fights = []
    for row in fight_rows[:5]:
        cols = row.select("td.b-fight-details__table-col")
        if len(cols) < 8:
            continue
        try:
            result   = cols[0].get_text(strip=True)
            opponent_links = cols[1].select("a")
            opponent = opponent_links[0].get_text(strip=True) if opponent_links else "Unknown"
            event_links = cols[6].select("a")
            event    = event_links[0].get_text(strip=True) if event_links else "Unknown"
            method   = cols[7].get_text(" ", strip=True).split()[0] if cols[7] else "DEC"
            date_el  = cols[8].select_one("p") if len(cols) > 8 else None
            date_str = date_el.get_text(strip=True) if date_el else ""

            # Normalize date
            try:
                date_obj = datetime.strptime(date_str, "%b. %d, %Y")
                date_fmt = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                date_fmt = date_str

            str_s = calc_striking_impact(parsed)
            wre_s = calc_wrestling_impact(parsed)
            gnd_s = calc_ground_impact(parsed)
            ois_s = calc_ois(str_s, wre_s, gnd_s)

            fights.append({
                "opponent": opponent,
                "event":    event,
                "date":     date_fmt,
                "result":   result[0].upper() if result else "W",
                "method":   method,
                "ois":      ois_s,
                "str":      str_s,
                "wre":      wre_s,
                "gnd":      gnd_s,
            })
        except Exception as e:
            print(f"    [fight parse error] {e}")
            continue

    str_score = calc_striking_impact(parsed)
    wre_score = calc_wrestling_impact(parsed)
    gnd_score = calc_ground_impact(parsed)
    ois_score = calc_ois(str_score, wre_score, gnd_score)

    # Division from weight class attribute
    weight = attrs.get("weight", "")
    division = "Unknown"
    for div in ["Strawweight","Flyweight","Bantamweight","Featherweight",
                "Lightweight","Welterweight","Middleweight","Light Heavyweight","Heavyweight"]:
        if div.lower() in weight.lower():
            division = div
            break

    return {
        "id":       None,          # assigned after collection
        "name":     name,
        "nickname": "",
        "division": division,
        "record":   record,
        "country":  "",
        "age":      None,
        "height":   attrs.get("height", ""),
        "reach":    attrs.get("reach", ""),
        "stance":   attrs.get("stance", ""),
        "ois":      ois_score,
        "str":      str_score,
        "wre":      wre_score,
        "gnd":      gnd_score,
        "result":   fights[0]["result"] if fights else "W",
        "method":   fights[0]["method"] if fights else "DEC",
        "event":    fights[0]["event"]  if fights else "",
        "change":   "+0.0",
        "insight":  f"{name} — OIS auto-generated from UFCStats.com data. Edit this field to add manual insight.",
        "fights":   fights,
    }

# ──────────────────────────────────────────────────────────────
# TARGET FIGHTERS
# Scrape a curated list by name rather than the full database.
# Add any fighter name here to include them in fighters.json.
# ──────────────────────────────────────────────────────────────

TARGET_FIGHTERS = [
    "Islam Makhachev",
    "Sean O'Malley",
    "Dustin Poirier",
    "Alex Pereira",
    "Leon Edwards",
    "Alexandre Pantoja",
    "Merab Dvalishvili",
    "Dricus du Plessis",
    "Ilia Topuria",
    "Jon Jones",
    "Tom Aspinall",
    "Paddy Pimblett",
    "Charles Oliveira",
    "Justin Gaethje",
    "Max Holloway",
    "Alexander Volkanovski",
    "Israel Adesanya",
    "Kamaru Usman",
    "Belal Muhammad",
    "Robert Whittaker",
    "Khamzat Chimaev",
    "Bo Nickal",
]

def search_fighter_url(name):
    """Search UFCStats for a fighter by name and return their profile URL."""
    parts = name.strip().split()
    if len(parts) < 2:
        return None
    first, last = parts[0], parts[-1]
    url = f"{FIGHTER_LIST_URL}?char={last[0].lower()}&page=1"
    soup = get_soup(url)
    if not soup:
        return None

    rows = soup.select("table.b-statistics__table tbody tr.b-statistics__table-row")
    for row in rows:
        cols = row.select("td")
        if len(cols) < 2:
            continue
        row_first = cols[0].get_text(strip=True).lower()
        row_last  = cols[1].get_text(strip=True).lower()
        if first.lower() in row_first and last.lower() in row_last:
            link = cols[0].find("a") or cols[1].find("a")
            if link:
                return link.get("href")
    return None

# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("OctagonImpact Scraper — UFCStats.com")
    print("=" * 60)

    results = []
    for i, name in enumerate(TARGET_FIGHTERS):
        print(f"\n[{i+1}/{len(TARGET_FIGHTERS)}] Searching: {name}")
        url = search_fighter_url(name)
        if not url:
            print(f"  ✗ Could not find URL for {name}")
            continue
        print(f"  → Found: {url}")

        data = scrape_fighter_detail(url)
        if not data:
            print(f"  ✗ Failed to scrape {name}")
            continue

        data["id"] = i + 1
        data["name"] = name   # use our canonical spelling
        results.append(data)
        print(f"  ✓ OIS={data['ois']}  STR={data['str']}  WRE={data['wre']}  GND={data['gnd']}")

        # Be polite — don't hammer the server
        delay = random.uniform(1.5, 3.0)
        time.sleep(delay)

    if not results:
        print("\n✗ No fighters scraped. Check your internet connection.")
        return

    output_path = "fighters.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"✓ Done! Scraped {len(results)} fighters.")
    print(f"✓ Output saved to: {output_path}")
    print(f"\nNext steps:")
    print(f"  1. Review fighters.json and edit 'insight' fields manually")
    print(f"  2. Upload fighters.json to your GitHub repository")
    print(f"  3. Vercel will auto-redeploy your site")
    print("=" * 60)

if __name__ == "__main__":
    main()
