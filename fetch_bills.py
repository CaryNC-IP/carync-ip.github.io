#!/usr/bin/env python3
"""
nc_feed_builder.py
==================
Builds a JSON feed of North Carolina General Assembly bills relevant to building
codes, permits, inspections, code-enforcement licensing, and related topics —
pulled DIRECTLY from ncleg.gov (no browser / no search-engine middle layer).

Output: feed.json  (drop next to the tracker HTML; the tracker can load it)

Usage
-----
    python nc_feed_builder.py                      # default: 2025 session, keyword-filtered
    python nc_feed_builder.py --session 2025 -o feed.json
    python nc_feed_builder.py --all                # keep every bill, don't keyword-filter
    python nc_feed_builder.py --serve              # write feed.json AND serve it on :8765 (CORS-enabled)

Scheduling (so it stays current on its own)
-------------------------------------------
Windows Task Scheduler:  run  pythonw nc_feed_builder.py --out "C:\\path\\feed.json"  daily.
macOS/Linux cron:        0 6 * * *  /usr/bin/python3 /path/nc_feed_builder.py --out /path/feed.json

Dependencies
------------
    pip install requests beautifulsoup4

Notes on ncleg.gov
------------------
ncleg publishes a machine-readable master listing per session at:
    https://www.ncleg.gov/Legislation/Legislation/BillsByType/{session}/{chamber}
and a per-bill history/status page at:
    https://www.ncleg.gov/BillLookUp/{session}/{billid}
This script primarily parses the per-session bill index, then enriches each hit
with its latest action from the BillLookUp page. Site markup shifts occasionally;
the two parse points that may need updating are flagged with  # >>> VERIFY  below.
"""

import argparse
import datetime as dt
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Missing deps. Run:  pip install requests beautifulsoup4")

BASE = "https://www.ncleg.gov"
HEADERS = {
    # A normal browser UA — this is the difference-maker vs. the in-browser search that was failing.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 25
RETRIES = 3

# ---------------------------------------------------------------------------
# Topic taxonomy — mirrors the tracker's filters. Priority tags (p=1) first.
# ---------------------------------------------------------------------------
PRIORITY_TAGS = [
    "Building Code", "Permits & Approvals", "Licensing & Qualifications",
    "Inspections & Enforcement", "Local Gov. Authority",
]

# tag -> list of lowercase keyword/phrase triggers matched against title+summary
TAG_RULES = {
    "Building Code":              ["building code", "state building code", "residential code",
                                   "code council", "single-exit", "single stair", "egress",
                                   "occupancy classification", "fire-resistant", "energizing buildings",
                                   "electrical code", "ungraded lumber"],
    "Permits & Approvals":        ["building permit", "permit applic", "plan review",
                                   "development approval", "sealed", "permitting"],
    "Licensing & Qualifications": ["licens", "qualification board", "coqb", "apprenticeship",
                                   "general contractor", "home inspector", "code official",
                                   "code-enforcement official", "code enforcement official"],
    "Inspections & Enforcement":  ["inspection", "inspector", "code enforcement",
                                   "code-enforcement", "private inspect"],
    "Local Gov. Authority":       ["local government", "cities shall not", "municipal",
                                   "restrict local", "local ordinance", "local act",
                                   "extraterritorial", "planning jurisdiction", "development regulation"],
    "Land Development":           ["land development", "subdivision", "development",
                                   "site plan", "land use"],
    "Zoning & Land Use":          ["zoning", "zoned", "down-zoning", "nonconform",
                                   "etj", "middle housing", "special use permit"],
    "Housing & ADUs":             ["housing", "accessory dwelling", "adu", "dwelling unit",
                                   "affordable housing"],
    "Disaster Recovery":          ["disaster", "helene", "hurricane", "flood"],
    "Fire / OSFM":                ["fire marshal", "fire prevention", "fire and rescue",
                                   "osfm", "state fire"],
    "Stormwater & Environment":   ["stormwater", "erosion", "sediment", "built-upon",
                                   "environmental", "area of environmental concern"],
    "Utilities & Infrastructure": ["water and sewer", "water andsewer", "sewer", "utility",
                                   "utilities", "transportation", "multimodal", "road"],
    "Child Care Facilities":      ["child care", "childcare", "child-care"],
    "Firearms-Related":           ["firearm", "gun ", "gun dealer", "concealed", "handgun",
                                   "door lock exemption"],
    "Budget / Appropriations":    ["appropriation", "base budget", "current operations"],
}

# A bill is KEPT (unless --all) if it matches any tag in this set.
RELEVANT_TAGS = set(PRIORITY_TAGS) | {
    "Land Development", "Zoning & Land Use", "Housing & ADUs", "Disaster Recovery",
    "Fire / OSFM", "Stormwater & Environment", "Utilities & Infrastructure",
    "Child Care Facilities", "Firearms-Related",
}

FIREARMS_TRIGGERS = ["firearm", "gun ", "gun dealer", "concealed", "handgun"]


# ---------------------------------------------------------------------------
# HTTP with retry/backoff
# ---------------------------------------------------------------------------
def fetch(url, session):
    last = None
    for attempt in range(RETRIES):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))  # backoff — polite during high-volume periods
    print(f"  ! could not fetch {url} ({last})", file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def classify(text):
    """Return (tags_list, is_firearms). Priority tags ordered first."""
    t = text.lower()
    hits = [tag for tag, kws in TAG_RULES.items() if any(k in t for k in kws)]
    firearms = any(k in t for k in FIREARMS_TRIGGERS)
    if firearms:
        # Firearms bills live only under the Firearms-Related filter (matches the tracker).
        return ["Firearms-Related"], True
    ordered = [x for x in PRIORITY_TAGS if x in hits] + [x for x in hits if x not in PRIORITY_TAGS]
    return ordered, False


def _sentence_case(s):
    """ncleg titles/summaries are ALL CAPS. Convert to readable sentence case:
    lowercase everything except recognized acronyms, then capitalize the first letter."""
    if not s:
        return s
    keep = {"NC", "N.C.", "OSFM", "COQB", "ADU", "ADUS", "ETJ", "CCRC", "DOI",
            "US", "U.S.", "LLC", "HOA", "EMS", "GIS", "SL", "S.L.", "PAH", "ERRC",
            "UNC", "NCDOI", "NCCOQB"}
    out = []
    for w in s.split():
        core = re.sub(r"[^\w.]", "", w).upper()
        if core in keep:
            out.append(w.upper() if w.isupper() else w)
        elif w.isupper():
            out.append(w.lower())
        else:
            out.append(w)  # mixed-case already; leave as-is
    res = " ".join(out)
    # Capitalize first alphabetical character.
    for i, ch in enumerate(res):
        if ch.isalpha():
            res = res[:i] + ch.upper() + res[i + 1:]
            break
    return res


def make_bullets(title, summary):
    """
    Turn the long 'AN ACT TO ...' summary into prioritized bullets.
    Splits on clause boundaries; marks code/permit/inspection/licensing clauses p=1.
    """
    src = (summary or title or "").strip()
    src = re.sub(r"\s+\d+\s+", " ", " " + src + " ")            # strip stray line numbers
    src = re.sub(r"\s+", " ", src).strip().rstrip(".")
    body = re.sub(r"^AN ACT (TO|PROVIDING|MANDATING|ESTABLISHING|AUTHORIZING)\s+", "",
                  src, flags=re.I)
    parts = re.split(r";\s+(?:AND\s+)?TO\s+|,?\s+AND\s+TO\s+|;\s+", body, flags=re.I)
    parts = [p.strip(" .,") for p in parts if len(p.strip()) > 8]

    p1_kw = ["building code", "permit", "inspection", "inspector", "licens",
             "code-enforcement", "code enforcement", "code official", "qualification board",
             "plan review", "electrical code", "egress", "fire-resistant", "occupancy"]
    bullets = []
    for p in parts[:10]:
        pr = 1 if any(k in p.lower() for k in p1_kw) else 2
        bullets.append({"t": _sentence_case(p), "p": pr})
    if not bullets:
        bullets = [{"t": _sentence_case(title) or "See bill text", "p": 2}]
    return bullets


def parse_bill_index(html):
    """
    Parse a bill index/search page into rows. Recognizes bill links in any of ncleg's
    formats: /BillLookUp/{session}/{Hxxx}, ?BillID=Hxxx, /Bills/House/HTML/Hxxx, etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = {}
    # Match a bill id (H123 / S45 / HB123 / SB45) appearing anywhere in an href.
    link_rx = re.compile(r"(?:BillLookUp/\d+/|BillID=|/)([HS]B?\d{1,4})(?:[/?&\"']|$)", re.I)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = link_rx.search(href)
        if not m:
            continue
        rawid = m.group(1).upper().replace("B", "")  # HB123 -> H123
        if not re.fullmatch(r"[HS]\d{1,4}", rawid):
            continue
        bid = rawid
        container = a.find_parent(["tr", "li", "div"]) or a.parent
        text = " ".join(container.get_text(" ", strip=True).split())
        title = a.get_text(" ", strip=True)
        if re.fullmatch(r"[HS]B?\d+", title, re.I) or len(title) < 4:
            title = text  # link text was just the number; use the row text
        rows.setdefault(bid, {"id": bid, "title": title[:300], "context": text[:600]})
    return list(rows.values())


def parse_bill_page(html):
    """
    Extract long title, latest action + date, and status from a BillLookUp page.
    # >>> VERIFY: the history/actions table lives in a panel; each action row has a
    # date and description. We take the most recent (last) row as 'lastAction'.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Long title: usually the "AN ACT ..." line.
    long_title = None
    m = re.search(r"(AN ACT [^\n]{5,600})", text, re.I)
    if m:
        long_title = re.sub(r"\s+", " ", m.group(1)).strip()

    # Session law chapter, if enacted.
    session_law = None
    m = re.search(r"S\.?L\.?\s*(\d{4}-\d+)|Session Law\s*(\d{4}-\d+)|Ch(?:apter)?\.?\s*SL?\s*(\d{4}-\d+)",
                  text, re.I)
    if m:
        session_law = next(g for g in m.groups() if g)

    # Latest action: scan lines shaped like a date + description.
    last_action, last_date = None, None
    date_line = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})\s+(.{4,120})")
    iso_line = re.compile(r"(\d{4}-\d{2}-\d{2})\s+(.{4,120})")
    candidates = []
    for line in text.split("\n"):
        for rx, fmt in ((date_line, "%m/%d/%Y"), (iso_line, "%Y-%m-%d")):
            mm = rx.match(line.strip())
            if mm:
                try:
                    d = dt.datetime.strptime(mm.group(1), fmt).date()
                    candidates.append((d, mm.group(2).strip()))
                except ValueError:
                    pass
    if candidates:
        candidates.sort(key=lambda x: x[0])
        last_date = candidates[-1][0].isoformat()
        last_action = candidates[-1][1]

    # Coarse stage inference from action keywords.
    joined = text.lower()
    if session_law or "ch. sl" in joined or "became law" in joined:
        stage = "law"
    elif "vetoed" in joined:
        stage = "vetoed"
    elif "ch. res" in joined or "ratified" in joined:
        stage = "passed_both"
    elif "passed 3rd reading" in joined and "senate" in joined and "house" in joined:
        stage = "passed_both"
    elif "passed 3rd reading" in joined:
        stage = "passed_house"       # coarse; refine if chamber known
    elif "ref to com" in joined or "committee" in joined:
        stage = "committee"
    elif "filed" in joined:
        stage = "filed"
    else:
        stage = "filed"

    return {
        "long_title": long_title,
        "sessionLaw": session_law,
        "lastAction": last_action,
        "lastActionDate": last_date,
        "stage": stage,
    }


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
def build(session, keep_all, workers=6):
    s = requests.Session()
    # Prime cookies/session by visiting the site root first (some ASP.NET sites
    # require this before search endpoints respond).
    fetch(f"{BASE}/", s)

    # Real ncleg.gov listing endpoints, most-reliable first. These are the bill
    # SEARCH result pages, which render a table of bills with links to BillLookUp.
    index_urls = [
        # Simple search returning all House/Senate bills for the session:
        f"{BASE}/Legislation/Legislation/BillsByStatus/{session}",
        f"{BASE}/BillLookup/{session}",
        f"{BASE}/Legislation/BillSearch/{session}",
        # Search results (GET form) — broad query that returns the full list:
        f"{BASE}/Legislation/Legislation/legislation.html?ID={session}",
        # Chamber "bills filed" listings:
        f"{BASE}/Legislation/Legislation/House/{session}",
        f"{BASE}/Legislation/Legislation/Senate/{session}",
    ]
    raw = {}
    tried = []
    for url in index_urls:
        html = fetch(url, s)
        tried.append(url)
        if not html:
            continue
        found = parse_bill_index(html)
        for row in found:
            raw.setdefault(row["id"], row)
        print(f"  {url} -> {len(found)} bill links")
        if raw:
            break  # first index that yields rows is enough

    if not raw:
        print("No bills parsed from any index endpoint. URLs tried:", file=sys.stderr)
        for u in tried:
            print("   " + u, file=sys.stderr)
        # DIAGNOSTIC: dump a sample of whatever the first reachable page returned so we
        # can see the real markup / real bill-link format and fix the URL or parser.
        for u in tried:
            html = fetch(u, s)
            if html:
                print(f"\n--- DIAGNOSTIC: first 1500 chars of {u} ---", file=sys.stderr)
                print(html[:1500], file=sys.stderr)
                # Show any hrefs that look like bill links, whatever their format:
                links = re.findall(r'href="([^"]*[Bb]ill[^"]*)"', html)[:20]
                print("\n--- sample bill-like links on that page ---", file=sys.stderr)
                for l in links:
                    print("   " + l, file=sys.stderr)
                break
        return None

    print(f"Index yielded {len(raw)} bills; enriching + filtering…")

    bills = []

    def process(row):
        bid = row["id"]
        tags, firearms = classify(row.get("context", "") + " " + row.get("title", ""))
        # Enrich from the bill page (long title, latest action, status).
        page = fetch(f"{BASE}/BillLookUp/{session}/{bid}", s)
        info = parse_bill_page(page) if page else {}
        title = (row.get("title") or "").strip() or bid
        long_title = info.get("long_title") or ""
        # Re-classify with the richer long title.
        tags, firearms = classify(f"{title} {long_title}")
        return {
            "id": bid,
            "chamber": "House" if bid.startswith("H") else "Senate",
            "title": _sentence_case(title.rstrip(".")),
            "summary": _sentence_case(long_title),
            "bullets": make_bullets(title, long_title),
            "tags": tags,
            "firearms": firearms,
            "stage": info.get("stage", "filed"),
            "sessionLaw": info.get("sessionLaw"),
            "lastAction": info.get("lastAction"),
            "lastActionDate": info.get("lastActionDate"),
            "introduced": None,
            "checkedAt": dt.datetime.now().isoformat(timespec="seconds"),
            "discovered": True,
        }

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process, r): r for r in raw.values()}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                b = fut.result()
            except Exception as e:
                print(f"  ! {futs[fut]['id']} failed: {e}", file=sys.stderr)
                continue
            if keep_all or (set(b["tags"]) & RELEVANT_TAGS):
                bills.append(b)
            if i % 25 == 0:
                print(f"  …processed {i}/{len(raw)}")

    # Sort by most recent activity.
    bills.sort(key=lambda b: (b["lastActionDate"] or "0000-00-00"), reverse=True)
    return bills


def main():
    ap = argparse.ArgumentParser(description="Build feed.json of NC building/permit legislation from ncleg.gov")
    ap.add_argument("--session", default="2025", help="Session year (default 2025)")
    ap.add_argument("-o", "--out", default="feed.json", help="Output path (default feed.json)")
    ap.add_argument("--all", action="store_true", help="Keep every bill (skip topic filtering)")
    ap.add_argument("--serve", action="store_true", help="After building, serve the file on :8765 with CORS")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()

    print(f"Building feed for {args.session} session from {BASE} …")
    bills = build(args.session, keep_all=args.all)
    if not bills:
        # ncleg.gov unreachable or markup changed. Do NOT overwrite a good existing feed —
        # leave the previous feed.json in place so the site keeps showing the last good data.
        import os
        if os.path.exists(args.out):
            print("Fetch returned nothing; keeping the existing feed.json unchanged.", file=sys.stderr)
            sys.exit(0)
        # No prior feed at all: write an empty-but-valid feed so the page has something to read.
        payload = {
            "generatedAt": dt.datetime.now().isoformat(timespec="seconds"),
            "session": args.session, "source": f"{BASE}/Legislation",
            "count": 0, "bills": [],
            "note": "No bills fetched on this run — ncleg.gov may have been unreachable.",
        }
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print("Wrote empty placeholder feed.json.")
        sys.exit(0)

    payload = {
        "generatedAt": dt.datetime.now().isoformat(timespec="seconds"),
        "session": args.session,
        "source": f"{BASE}/Legislation",
        "count": len(bills),
        "bills": bills,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(bills)} bills to {args.out}")

    if args.serve:
        serve(args.out, args.port)


def serve(path, port):
    """Tiny CORS-enabled static server so the tracker HTML can fetch() the feed locally."""
    import http.server, socketserver, os
    directory = os.path.dirname(os.path.abspath(path)) or "."

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=directory, **kw)
        def end_headers(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            super().end_headers()
        def log_message(self, *a):
            pass

    with socketserver.TCPServer(("127.0.0.1", port), Handler) as httpd:
        print(f"Serving {directory} at http://127.0.0.1:{port}/  (Ctrl+C to stop)")
        print(f"  feed URL: http://127.0.0.1:{port}/{os.path.basename(path)}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped.")


if __name__ == "__main__":
    main()
