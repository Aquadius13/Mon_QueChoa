#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler Trận Đấu — quechoa9.live  v9                      ║
║   Nguồn thumbnail: quechoa.json CDN (thumbnail cả 2 đội)    ║
║   Nguồn streams:   quechoa.json CDN hoặc quechoa9.live      ║
╚══════════════════════════════════════════════════════════════╝

Cài đặt: pip install cloudscraper beautifulsoup4 lxml requests

Chạy:
    python crawler_quechoa9_v9.py                   # mặc định
    python crawler_quechoa9_v9.py --all             # tất cả trận
    python crawler_quechoa9_v9.py --json-only       # chỉ dùng quechoa.json
    python crawler_quechoa9_v9.py --output out.json
"""

import argparse, hashlib, json, re, sys, time
from datetime import datetime
from urllib.parse import urljoin

try:
    import cloudscraper
    from bs4 import BeautifulSoup, NavigableString
    import requests
except ImportError:
    print("Cài đặt: pip install cloudscraper beautifulsoup4 lxml requests"); sys.exit(1)

# ── Constants ──────────────────────────────────────────────────
BASE_URL      = "https://quechoa9.live"
QUECHOA_JSON  = "https://pub-26bab83910ab4b5781549d12d2f0ef6f.r2.dev/quechoa.json"
OUTPUT_FILE   = "quechoa9_iptv.json"
CHROME_UA     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# ── Helpers ────────────────────────────────────────────────────
def make_id(*parts):
    raw  = "-".join(str(p) for p in parts if p)
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", raw).strip("-").lower()
    return (slug[:48] + "-" + hashlib.md5(raw.encode()).hexdigest()[:8]
            if len(slug) > 56 else slug)

def log(msg): print(msg, flush=True)

def normalize_name(name: str) -> str:
    """Chuẩn hóa tên đội để so sánh fuzzy."""
    return re.sub(r"[^a-z0-9]", "", name.lower())

# ── HTTP ───────────────────────────────────────────────────────
def make_scraper():
    sc = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    sc.headers.update({"Accept-Language": "vi-VN,vi;q=0.9", "Referer": BASE_URL + "/"})
    return sc

def fetch_html(url, scraper, retries=3):
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=25, allow_redirects=True)
            r.raise_for_status()
            log(f"  ✓ [{r.status_code}] {url[:72]}")
            return r.text
        except Exception as e:
            wait = 2 ** i
            log(f"  ⚠ Lần {i+1}/{retries}: {e} → chờ {wait}s")
            if i < retries - 1: time.sleep(wait)
    return None

def fetch_quechoa_json() -> dict:
    """Fetch quechoa.json từ CDN — chứa thumbnail cả 2 đội + streams."""
    try:
        r = requests.get(QUECHOA_JSON, timeout=15,
                         headers={"User-Agent": CHROME_UA})
        r.raise_for_status()
        data = r.json()
        log(f"  ✓ quechoa.json: {sum(len(g.get('channels',[])) for g in data.get('groups',[]))} trận")
        return data
    except Exception as e:
        log(f"  ⚠ Không fetch được quechoa.json: {e}")
        return {}

# ── Build lookup từ quechoa.json ───────────────────────────────
def build_quechoa_lookup(qdata: dict) -> dict:
    """
    Tạo dict: normalized_team_pair → channel_info
    Key: "teamA_vs_teamB" (normalized)
    Value: {thumb, streams, league, logo_a, logo_b, labels, referer_url}
    """
    lookup = {}
    for group in qdata.get("groups", []):
        for ch in group.get("channels", []):
            meta = ch.get("org_metadata", {})
            team_a = meta.get("team_a", "")
            team_b = meta.get("team_b", "")
            if not team_a or not team_b:
                continue

            # Lấy thumbnail từ org_metadata.thumb (chính xác nhất)
            thumb = (meta.get("thumb")
                     or (ch.get("image") or {}).get("url", ""))

            # Lấy streams + referer từ sources
            streams = []
            referer_url = ""
            for src in ch.get("sources", []):
                for cnt in src.get("contents", []):
                    for strm in cnt.get("streams", []):
                        for sl in strm.get("stream_links", []):
                            url = sl.get("url", "")
                            stype = sl.get("type", "hls")
                            sname = sl.get("name", "")
                            # Lấy Referer từ request_headers
                            if not referer_url:
                                for hdr in sl.get("request_headers", []):
                                    if hdr.get("key","").lower() == "referer":
                                        referer_url = hdr.get("value","")
                            if url:
                                streams.append({
                                    "name": sname,
                                    "url":  url,
                                    "type": stype,
                                })

            info = {
                "thumb":       thumb,
                "streams":     streams,
                "league":      meta.get("league", ""),
                "logo_a":      meta.get("logo_a", ""),
                "logo_b":      meta.get("logo_b", ""),
                "labels":      ch.get("labels", []),
                "referer_url": referer_url,
                "ch_id":       ch.get("id", ""),
                "ch_name":     ch.get("name", ""),
            }

            # Index theo cả 2 chiều (home/away có thể đổi)
            key_ab = normalize_name(team_a) + "_" + normalize_name(team_b)
            key_ba = normalize_name(team_b) + "_" + normalize_name(team_a)
            lookup[key_ab] = info
            lookup[key_ba] = info

            # Index thêm theo từng tên đội riêng lẻ (fallback)
            lookup["_" + normalize_name(team_a)] = info
            lookup["_" + normalize_name(team_b)] = info

    log(f"  → Lookup: {len(lookup)} entries từ quechoa.json")
    return lookup

def find_in_lookup(lookup: dict, home: str, away: str) -> dict | None:
    """Tìm thông tin trận trong lookup, dùng fuzzy match nếu cần."""
    if not lookup:
        return None

    h = normalize_name(home)
    a = normalize_name(away)

    # Exact pair match
    for key in (f"{h}_{a}", f"{a}_{h}"):
        if key in lookup:
            return lookup[key]

    # Substring match: team name xuất hiện trong key
    for key, info in lookup.items():
        if key.startswith("_"):
            continue
        parts = key.split("_", 1)
        if len(parts) == 2:
            k1, k2 = parts
            if (h in k1 or k1 in h) and (a in k2 or k2 in a):
                return info
            if (a in k1 or k1 in a) and (h in k2 or k2 in h):
                return info

    # Single team fallback
    for name in (h, a):
        if len(name) >= 4 and f"_{name}" in lookup:
            return lookup[f"_{name}"]

    return None

# ── HTML Parser (quechoa9.live) ────────────────────────────────
def parse_html(html): return BeautifulSoup(html, "lxml")

def has_classes(tag, *classes):
    j = " ".join(tag.get("class", []))
    return all(c in j for c in classes)

def find_all_with(tag, element, *classes):
    if not classes: return []
    return [t for t in tag.find_all(element, class_=classes[0])
            if has_classes(t, *classes[1:])]

def find_match_cards(bs, only_featured=True):
    all_cards = find_all_with(bs, "a", "hover:border-[#83ff65]", "rounded-xl", "block")
    if not only_featured or not all_cards:
        log(f"  → {len(all_cards)} card (toàn trang)"); return all_cards
    header_node = None
    for node in bs.find_all(string=re.compile(r"TÂM ĐIỂM|tâm điểm", re.I)):
        header_node = node.find_parent()
        if header_node: break
    if not header_node:
        return all_cards
    section = header_node
    for _ in range(8):
        parent = section.find_parent()
        if not parent: break
        if find_all_with(parent, "a", "hover:border-[#83ff65]"):
            section = parent; break
        section = parent
    featured = find_all_with(section, "a", "hover:border-[#83ff65]", "rounded-xl")
    if featured:
        log(f"  ✅ {len(featured)} trận 'Tâm điểm'"); return featured
    return all_cards

def _extract_blv(card):
    avatar = next((s for s in card.find_all("span", class_="rounded-full")
                   if has_classes(s, "bg-neutral-700")), None)
    if not avatar: return ""
    parent = avatar.find_parent("div")
    if not parent: return ""
    for child in parent.children:
        if isinstance(child, NavigableString):
            t = str(child).strip()
            if len(t) >= 2: return t
    letter = avatar.get_text(strip=True)
    full   = parent.get_text(" ", strip=True)
    if full.startswith(letter):
        c = full[len(letter):].strip()
        if len(c) >= 2: return c
    return ""

def parse_card(card) -> dict | None:
    href       = card.get("href", "")
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
    raw_text   = card.get_text(" ", strip=True)

    if re.search(r"\bLive\b", raw_text, re.I):               status = "live"
    elif re.search(r"Kết thúc|Finished|\bFT\b", raw_text, re.I): status = "finished"
    else:                                                      status = "upcoming"

    time_div = next((d for d in card.find_all("div", class_="from-[#051f00]")
                     if d.get_text(strip=True)), None)
    match_time = time_div.get_text(strip=True) if time_div else ""
    if not match_time:
        m = re.search(r"\d{1,2}:\d{2}\s*\|\s*\d{2}\.\d{2}", raw_text)
        if m: match_time = m.group(0)

    league = ""
    for d in card.find_all("div", class_="justify-center"):
        if not has_classes(d, "gap-1", "w-full"): continue
        txt = d.get_text(strip=True)
        if txt and len(txt) > 3 and not re.fullmatch(r"[\d:\s]+", txt):
            league = txt; break

    home_team = away_team = ""
    team_texts = []
    for d in [d for d in card.find_all("div", class_="flex-1")
              if has_classes(d, "flex-col", "items-center")]:
        t = d.get_text(" ", strip=True)
        if t and len(t) >= 2 and not re.fullmatch(r"[\d\s:]+", t):
            team_texts.append(t)
    if len(team_texts) >= 2: home_team, away_team = team_texts[0], team_texts[1]
    elif len(team_texts) == 1: home_team = team_texts[0]
    if not home_team:
        vm = re.search(
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34}?)"
            r"\s+(?:VS|vs)\s+"
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34})",
            raw_text, re.UNICODE)
        if vm: home_team, away_team = vm.group(1).strip(), vm.group(2).strip()

    score = ""
    score_div = next((d for d in card.find_all("div", class_="rounded-[20px]")
                      if has_classes(d, "border-[#83ff65]")), None)
    if score_div:
        nums = re.findall(r"\d+", score_div.get_text())
        score = (f"{nums[0]}:{nums[1]}" if len(nums) >= 2
                 else ("VS" if score_div.get_text(strip=True).upper() == "VS" else ""))

    blv = _extract_blv(card)

    title = (f"{home_team} vs {away_team}" if home_team and away_team
             else home_team or re.sub(r"\s{2,}", " ", raw_text)[:60])
    if not title or not detail_url: return None

    return {
        "title": title, "home_team": home_team, "away_team": away_team,
        "score": score, "status": status, "league": league,
        "match_time": match_time, "detail_url": detail_url, "blv": blv,
    }

def extract_matches(html, only_featured=True):
    bs = parse_html(html)
    result, seen = [], set()
    for card in find_match_cards(bs, only_featured):
        m = parse_card(card)
        if m and m["title"].lower() not in seen:
            seen.add(m["title"].lower()); result.append(m)
    return result

# ── Stream fallback từ detail page ────────────────────────────
_QUALITY_RE  = re.compile(r"[_-](?:full[_-]?hd|fhd|1080p?|720p?|480p?|360p?|hd|sd)$", re.I)
_QUALITY_MAP = {"hd":"HD","sd":"SD","full-hd":"Full HD","full_hd":"Full HD","fhd":"Full HD",
                "1080":"Full HD","1080p":"Full HD","720":"HD","720p":"HD",
                "480":"SD","480p":"SD","360":"360p","360p":"360p"}
_QUALITY_ORDER = {"Auto":0,"Full HD":1,"HD":2,"SD":3}

def _stream_base(url):
    return _QUALITY_RE.sub("", re.sub(r"\.\w+$","",url.rstrip("/").split("/")[-1])).lower()

def _quality_label(url):
    fname = re.sub(r"\.\w+$","",url.rstrip("/").split("/")[-1]).lower()
    m = _QUALITY_RE.search(fname)
    return _QUALITY_MAP.get(m.group(0).lstrip("-_").lower(), m.group(0).upper()) if m else "Auto"

def filter_streams(streams):
    hls = [s for s in streams if s["type"] == "hls"]
    other = [s for s in streams if s["type"] != "hls"]
    if hls:
        base  = _stream_base(hls[0]["url"])
        group = [{**s, "name": _quality_label(s["url"])} for s in hls
                 if _stream_base(s["url"]) == base]
        group.sort(key=lambda x: _QUALITY_ORDER.get(x["name"], 99))
        return group + other
    return other

def extract_streams_from_page(detail_url, scraper):
    html = fetch_html(detail_url, scraper, retries=2)
    if not html: return []
    bs   = parse_html(html)
    seen, raw = set(), []
    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url) > 12:
            seen.add(url); raw.append({"name":name,"url":url,"type":kind})
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("HLS", m.group(1), "hls")
    for script in bs.find_all("script"):
        c = script.string or ""
        for m in re.finditer(r'"(?:file|src|url|hls|streamUrl|videoUrl)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live", u, re.I): add("HLS", u, "hls")
    for iframe in bs.find_all("iframe", src=True):
        if re.search(r"live|stream|embed|player", iframe["src"], re.I):
            add("embed", iframe["src"], "iframe")
    if not raw: add("Trang trực tiếp", detail_url, "iframe")
    hls = [s for s in raw if s["type"] == "hls"]
    return filter_streams(hls) if hls else raw

# ── Build channel ──────────────────────────────────────────────
def build_channel(m: dict, streams: list, index: int,
                  thumb: str, logo_a: str, logo_b: str,
                  referer_url: str, qc_league: str) -> dict:

    ch_id = make_id("qc", str(index),
                    re.sub(r"[^a-z0-9]", "-", m["title"].lower())[:24])
    blv    = m.get("blv", "")
    score  = m.get("score", "")
    league = qc_league or m.get("league", "")
    ref    = referer_url or m["detail_url"]

    # ── request_headers ──────────────────────────────────────
    req_headers = [
        {"key": "Referer",    "value": ref},
        {"key": "User-Agent", "value": "Mozilla/5.0"},
    ]

    # ── Stream links ─────────────────────────────────────────
    slinks = []
    for j, sl in enumerate(streams):
        slinks.append({
            "id":              make_id(ch_id, "lnk", str(j)),
            "name":            sl.get("name", f"Link {j+1}"),
            "type":            sl["type"],
            "default":         j == 0,
            "url":             sl["url"],
            "request_headers": req_headers,
        })
    if not slinks:
        slinks.append({
            "id":              make_id(ch_id, "lnk", "0"),
            "name":            "Link 1",
            "type":            "iframe",
            "default":         True,
            "url":             m["detail_url"],
            "request_headers": req_headers,
        })

    # ── Labels ───────────────────────────────────────────────
    labels = []

    # Status (top-left)
    status_cfg = {
        "live":     {"text": "● Live",        "color": "#E73131", "text_color": "#ffffff"},
        "upcoming": {"text": "⏳ Sắp diễn ra", "color": "#d54f1a", "text_color": "#ffffff"},
        "finished": {"text": "✅ Kết thúc",    "color": "#555555", "text_color": "#ffffff"},
    }.get(m["status"], {"text": "● Live", "color": "#E73131", "text_color": "#ffffff"})
    labels.append({"text": status_cfg["text"], "position": "top-left",
                   "color": status_cfg["color"], "text_color": status_cfg["text_color"]})

    # BLV (top-right)
    if blv:
        labels.append({"text": f"🎙 {blv}", "position": "top-right",
                       "color": "#006020", "text_color": "#ffffff"})

    # Thời gian (bottom-left)
    if m.get("match_time"):
        labels.append({"text": f"⏰ {m['match_time']}", "position": "bottom-left",
                       "color": "#00000099", "text_color": "#ffffff"})

    # Tỉ số hoặc giải đấu (bottom-right)
    if score and score != "VS" and m["status"] in ("live", "finished"):
        labels.append({"text": f"🥅 {score}", "position": "bottom-right",
                       "color": "#00000099", "text_color": "#ffff00"})
    elif league:
        labels.append({"text": league[:30], "position": "bottom-right",
                       "color": "#00000099", "text_color": "#ffffff"})

    # ── Image (thumbnail cả 2 đội từ quechoa CDN) ────────────
    img_obj = None
    if thumb:
        img_obj = {
            "padding":          1,
            "background_color": "#ececec",
            "display":          "contain",
            "url":              thumb,
            "width":            1600,
            "height":           1200,
        }

    stream_name = f"🎙 {blv}" if blv else "Trực tiếp"

    return {
        "id":            ch_id,
        "name":          m["title"],
        "type":          "single",
        "display":       "thumbnail-only",
        "enable_detail": False,
        "image":         img_obj,
        "labels":        labels,
        "sources": [{
            "id":   make_id(ch_id, "src"),
            "name": "QueCho9 Live",
            "contents": [{
                "id":   make_id(ch_id, "ct"),
                "name": m["title"],
                "streams": [{
                    "id":           make_id(ch_id, "st"),
                    "name":         stream_name,
                    "stream_links": slinks,
                }],
            }],
        }],
    }

def build_iptv_json(channels, group_name):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "id":          "quechoa9-live",
        "name":        "QueCho9 — Trực tiếp bóng đá",
        "url":         BASE_URL + "/",
        "description": f"Cập nhật: {now_str}",
        "disable_ads": True,
        "color":       "#0f3460",
        "grid_number": 2,
        "image":       {"type": "cover", "url": f"{BASE_URL}/favicon.ico"},
        "groups": [{
            "id":       "tam-diem",
            "name":     group_name,
            "image":    None,
            "channels": channels,
        }],
    }

# ── Main ───────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all",       action="store_true",  help="Tất cả trận")
    ap.add_argument("--json-only", action="store_true",  help="Chỉ dùng quechoa.json (không crawl quechoa9.live)")
    ap.add_argument("--no-stream-fallback", action="store_true",
                    help="Không crawl stream từ detail page khi quechoa.json không có")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🏟  CRAWLER — quechoa9.live  v9")
    log("  📸  Thumbnail: quechoa CDN (ảnh cả 2 đội)")
    log("═"*62 + "\n")

    only_featured = not args.all
    group_name    = "🔥 Tất cả trận đấu" if args.all else "🔥 Trận đấu tâm điểm"

    # ── Bước 1: Fetch quechoa.json (thumbnail + streams chuẩn) ──
    log("📥 Bước 1: Fetch quechoa.json từ CDN...")
    qdata  = fetch_quechoa_json()
    lookup = build_quechoa_lookup(qdata)

    # ── Bước 2: Crawl danh sách trận từ quechoa9.live ──────────
    matches = []
    if args.json_only:
        log("\n⚡ --json-only: lấy toàn bộ trận từ quechoa.json")
        for group in qdata.get("groups", []):
            for ch in group.get("channels", []):
                meta = ch.get("org_metadata", {})
                ta = meta.get("team_a",""); tb = meta.get("team_b","")
                title = f"{ta} vs {tb}" if ta and tb else ch.get("name","?")
                # Parse time từ name: "⚽ X vs Y | HH:MM DD/MM"
                time_m = re.search(r"(\d{1,2}:\d{2}\s*\d{2}/\d{2}|\d{1,2}:\d{2}\s*\|\s*\d{2}\.\d{2})", ch.get("name",""))
                matches.append({
                    "title":      title,
                    "home_team":  ta,
                    "away_team":  tb,
                    "score":      "",
                    "status":     "live" if any(l.get("text","").startswith("●") for l in ch.get("labels",[])) else "upcoming",
                    "league":     meta.get("league",""),
                    "match_time": time_m.group(1) if time_m else "",
                    "detail_url": (ch.get("sources",[{}])[0].get("contents",[{}])[0]
                                   .get("streams",[{}])[0].get("stream_links",[{}])[0]
                                   .get("request_headers",[{}])[0].get("value","") or BASE_URL),
                    "blv":        "",
                })
    else:
        scraper = make_scraper()
        log(f"\n📥 Bước 2: Tải trang chủ quechoa9.live...")
        html = fetch_html(BASE_URL, scraper)
        if not html:
            log("❌ Không tải được trang chủ."); sys.exit(1)
        if "Just a moment" in html or "cf-browser-verification" in html:
            log("⚠ Cloudflare challenge — thử lại sau."); sys.exit(1)
        log(f"\n🔍 Bước 2b: Phân tích {'tâm điểm' if only_featured else 'tất cả'}...")
        matches = extract_matches(html, only_featured)
        if not matches:
            log("  ⚠ Không tìm thấy trận nào trên quechoa9.live")
            if lookup:
                log("  → Dùng dữ liệu từ quechoa.json thay thế")
                args.json_only = True
                # Re-run với json_only
                for group in qdata.get("groups", []):
                    for ch in group.get("channels", []):
                        meta = ch.get("org_metadata", {})
                        ta = meta.get("team_a",""); tb = meta.get("team_b","")
                        title = f"{ta} vs {tb}" if ta and tb else ch.get("name","?")
                        matches.append({
                            "title": title, "home_team": ta, "away_team": tb,
                            "score": "", "status": "live", "league": meta.get("league",""),
                            "match_time": "", "detail_url": BASE_URL, "blv": "",
                        })
            else:
                sys.exit(1)

    log(f"\n  ✅ {len(matches)} trận:\n")
    for i, m in enumerate(matches, 1):
        icon = {"live":"🔴","finished":"✅","upcoming":"🕐"}.get(m["status"],"⚽")
        log(f"  {icon} [{i:02d}] {m['title']}" + (f"  🎙 {m['blv']}" if m.get("blv") else ""))

    # ── Bước 3: Ghép thumbnail + streams từ quechoa.json ───────
    log(f"\n🖼  Bước 3: Ghép thumbnail từ quechoa.json CDN...")
    channels = []
    matched_count = 0

    for i, m in enumerate(matches, 1):
        info = find_in_lookup(lookup, m["home_team"], m["away_team"])

        if info:
            matched_count += 1
            thumb       = info["thumb"]
            streams     = info["streams"]
            logo_a      = info["logo_a"]
            logo_b      = info["logo_b"]
            referer_url = info["referer_url"]
            qc_league   = info["league"]
            log(f"  ✅ [{i:02d}] {m['title'][:45]}")
            log(f"       🖼  {thumb[:65]}")
        else:
            thumb = logo_a = logo_b = referer_url = qc_league = ""
            streams = []
            log(f"  ⚠  [{i:02d}] {m['title'][:45]} — không tìm thấy trong quechoa.json")

            # Fallback: crawl stream từ detail page
            if not args.json_only and not args.no_stream_fallback and hasattr(args, '_scraper'):
                log(f"       → Crawl stream từ detail page...")
                streams = extract_streams_from_page(m["detail_url"], scraper)

        channels.append(build_channel(
            m, streams, i, thumb, logo_a, logo_b, referer_url, qc_league
        ))

    log(f"\n  📊 Ghép thumbnail: {matched_count}/{len(matches)} trận")

    # ── Bước 4: Ghi file ────────────────────────────────────────
    result = build_iptv_json(channels, group_name)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*62}")
    log(f"  ✅ Xong!  📁 {args.output}  ⚽ {len(channels)} trận")
    log("═"*62 + "\n")

if __name__ == "__main__":
    main()
