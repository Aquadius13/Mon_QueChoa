#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler Trận Đấu Tâm Điểm — quechoa9.live  v9             ║
║   + Gộp trận trùng tên thành 1 channel                      ║
║   + Mỗi BLV = 1 stream riêng để chọn                        ║
║   + Label hiển thị số lượng BLV nếu có nhiều                ║
╚══════════════════════════════════════════════════════════════╝
Cài đặt: pip install cloudscraper beautifulsoup4 lxml
"""

import argparse, hashlib, json, re, sys, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

try:
    import cloudscraper
    from bs4 import BeautifulSoup, NavigableString, Tag
except ImportError:
    print("Cài đặt: pip install cloudscraper beautifulsoup4 lxml"); sys.exit(1)

BASE_URL    = "https://quechoa9.live"
OUTPUT_FILE = "quechoa9_iptv.json"
CHROME_UA   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
VN_TZ       = timezone(timedelta(hours=7))

# Ảnh placeholder khi trận không có thumbnail
# (MonPlayer báo lỗi nếu image = null với display = thumbnail-only)
PLACEHOLDER_IMG = {
    "padding":          2,
    "background_color": "#0f3460",
    "display":          "contain",
    "url":              f"https://quechoa9.live/favicon.ico",
    "width":            512,
    "height":           512,
}

# ── Helpers ───────────────────────────────────────────────────

def make_id(*parts):
    raw  = "-".join(str(p) for p in parts if p)
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", raw).strip("-").lower()
    return (slug[:48] + "-" + hashlib.md5(raw.encode()).hexdigest()[:8] if len(slug) > 56 else slug)

def log(msg): print(msg, flush=True)

def has_classes(tag, *classes):
    j = " ".join(tag.get("class", []))
    return all(c in j for c in classes)

def find_all_with(tag, element, *classes):
    if not classes: return []
    return [t for t in tag.find_all(element, class_=classes[0]) if has_classes(t, *classes[1:])]

def make_scraper():
    sc = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","mobile":False})
    sc.headers.update({"Accept-Language":"vi-VN,vi;q=0.9","Referer":BASE_URL+"/"})
    return sc

def fetch(url, scraper, retries=3):
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=25, allow_redirects=True)
            r.raise_for_status()
            log(f"  ✓ [{r.status_code}] {url[:72]}")
            return r.text
        except Exception as e:
            wait = 2**i
            log(f"  ⚠ Lần {i+1}/{retries}: {e} → chờ {wait}s")
            if i < retries-1: time.sleep(wait)
    return None

# ── Stream quality ────────────────────────────────────────────

_QUALITY_RE  = re.compile(r"[_-](?:full[_-]?hd|fhd|1080p?|720p?|480p?|360p?|hd|sd)$", re.I)
_QUALITY_MAP = {"hd":"HD","sd":"SD","full-hd":"Full HD","full_hd":"Full HD","fhd":"Full HD",
                "1080":"Full HD","1080p":"Full HD","720":"HD","720p":"HD",
                "480":"SD","480p":"SD","360":"360p","360p":"360p"}
_QUALITY_ORDER = {"Auto":0,"Full HD":1,"HD":2,"SD":3}

def _stream_base(url):
    fname = re.sub(r"\.\w+$","",url.rstrip("/").split("/")[-1])
    return _QUALITY_RE.sub("",fname).lower()

def _quality_label(url):
    fname = re.sub(r"\.\w+$","",url.rstrip("/").split("/")[-1]).lower()
    m = _QUALITY_RE.search(fname)
    return _QUALITY_MAP.get(m.group(0).lstrip("-_").lower(), m.group(0).upper()) if m else "Auto"

def filter_match_streams(streams):
    if not streams: return streams
    hls   = [s for s in streams if s["type"]=="hls"]
    other = [s for s in streams if s["type"]!="hls"]
    if hls:
        base  = _stream_base(hls[0]["url"])
        group = [{**s,"name":_quality_label(s["url"])} for s in hls if _stream_base(s["url"])==base]
        group.sort(key=lambda x:_QUALITY_ORDER.get(x["name"],99))
        return group + other
    return other

# ── Date/time parse ───────────────────────────────────────────

def parse_match_datetime(match_time: str):
    if not match_time:
        return ("", "", "")
    m = re.search(r"(\d{1,2}):(\d{2})\s*\|?\s*(\d{1,2})[./](\d{1,2})", match_time)
    if m:
        hh, mm, day, mon = m.group(1), m.group(2), m.group(3).zfill(2), m.group(4).zfill(2)
        return (f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}")
    m2 = re.search(r"(\d{1,2}):(\d{2})", match_time)
    if m2:
        hh, mm = m2.group(1), m2.group(2)
        today  = datetime.now(VN_TZ)
        return (f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}")
    return (match_time, "", "")

# ── Card parsing ──────────────────────────────────────────────

def parse_html(html): return BeautifulSoup(html,"lxml")

def find_match_cards(bs, only_featured=True):
    all_cards = find_all_with(bs,"a","hover:border-[#83ff65]","rounded-xl","block")
    if not only_featured or not all_cards:
        log(f"  → {len(all_cards)} card (toàn trang)"); return all_cards
    header_node = None
    for node in bs.find_all(string=re.compile(r"TÂM ĐIỂM|tâm điểm",re.I)):
        header_node = node.find_parent()
        if header_node: break
    if not header_node:
        log("  ℹ Không thấy 'Tâm điểm' → toàn trang"); return all_cards
    section = header_node
    for _ in range(8):
        parent = section.find_parent()
        if not parent: break
        if find_all_with(parent,"a","hover:border-[#83ff65]"):
            section = parent; break
        section = parent
    featured = find_all_with(section,"a","hover:border-[#83ff65]","rounded-xl")
    if featured:
        log(f"  ✅ {len(featured)} trận 'Tâm điểm'"); return featured
    return all_cards

def _extract_blv(card):
    avatar = next((s for s in card.find_all("span",class_="rounded-full")
                   if has_classes(s,"bg-neutral-700")), None)
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

def parse_card(card):
    href       = card.get("href","")
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
    raw_text   = card.get_text(" ", strip=True)

    if re.search(r"\bLive\b", raw_text, re.I):                    status = "live"
    elif re.search(r"Kết thúc|Finished|\bFT\b", raw_text, re.I): status = "finished"
    else:                                                           status = "upcoming"

    time_div       = next((d for d in card.find_all("div",class_="from-[#051f00]")
                           if d.get_text(strip=True)), None)
    match_time_raw = time_div.get_text(strip=True) if time_div else ""
    if not match_time_raw:
        m = re.search(r"\d{1,2}:\d{2}\s*\|?\s*\d{2}[./]\d{2}", raw_text)
        if m: match_time_raw = m.group(0)
        else:
            m2 = re.search(r"\d{1,2}:\d{2}", raw_text)
            if m2: match_time_raw = m2.group(0)

    time_str, date_str, sort_key = parse_match_datetime(match_time_raw)

    league = ""
    for d in card.find_all("div", class_="justify-center"):
        if not has_classes(d,"gap-1","w-full"): continue
        txt = d.get_text(strip=True)
        if txt and len(txt)>3 and not re.fullmatch(r"[\d:\s]+",txt): league=txt; break

    home_team = away_team = ""
    team_texts = []
    for d in [d for d in card.find_all("div",class_="flex-1")
              if has_classes(d,"flex-col","items-center")]:
        t = d.get_text(" ", strip=True)
        if t and len(t)>=2 and not re.fullmatch(r"[\d\s:]+",t): team_texts.append(t)
    if len(team_texts)>=2: home_team,away_team = team_texts[0],team_texts[1]
    elif len(team_texts)==1: home_team = team_texts[0]
    if not home_team:
        vm = re.search(
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34}?)"
            r"\s+(?:VS|vs)\s+"
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34})",
            raw_text, re.UNICODE)
        if vm: home_team,away_team = vm.group(1).strip(),vm.group(2).strip()

    score = ""
    score_div = next((d for d in card.find_all("div",class_="rounded-[20px]")
                      if has_classes(d,"border-[#83ff65]")), None)
    if score_div:
        nums = re.findall(r"\d+", score_div.get_text())
        score = (f"{nums[0]}-{nums[1]}" if len(nums)>=2
                 else ("VS" if score_div.get_text(strip=True).upper()=="VS" else ""))

    blv = _extract_blv(card)

    thumbnail = ""
    img = card.find("img")
    if img:
        src = img.get("src") or img.get("data-src") or ""
        thumbnail = src if src.startswith("http") else urljoin(BASE_URL,src)

    base_title = (f"{home_team} vs {away_team}" if home_team and away_team
                  else home_team or re.sub(r"\s{2,}"," ",raw_text)[:60])
    if not base_title or not detail_url: return None

    return {
        "base_title":  base_title,
        "home_team":   home_team,
        "away_team":   away_team,
        "score":       score,
        "status":      status,
        "league":      league,
        "match_time":  match_time_raw,
        "time_str":    time_str,
        "date_str":    date_str,
        "sort_key":    sort_key,
        "detail_url":  detail_url,
        "thumbnail":   thumbnail,
        "blv":         blv,
    }

# ── Gộp trận trùng tên ────────────────────────────────────────

def _normalize_title(title: str) -> str:
    """Chuẩn hóa tên để so sánh: bỏ dấu cách thừa, lower, bỏ ký tự đặc biệt."""
    t = title.lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^\w\s]", "", t)
    return t

def merge_matches(raw_matches: list) -> list:
    """
    Gộp các card cùng tên trận thành 1 match duy nhất.
    - Mỗi card riêng → 1 entry trong danh sách "blv_sources"
    - blv_sources: [{"blv": "Tên BLV", "detail_url": "..."}]
    - Lấy thông tin chung (score, status, league...) từ card đầu tiên
    """
    merged: dict[str, dict] = {}  # key = normalized title

    for m in raw_matches:
        key = _normalize_title(m["base_title"])
        if key not in merged:
            # Tạo entry mới, khởi tạo danh sách blv_sources
            merged[key] = {**m, "blv_sources": []}

        entry = merged[key]

        # Cập nhật score/status nếu card mới có thông tin tốt hơn
        if not entry["score"] and m["score"]:
            entry["score"] = m["score"]
        if entry["status"] == "upcoming" and m["status"] in ("live","finished"):
            entry["status"] = m["status"]
        if not entry["thumbnail"] and m["thumbnail"]:
            entry["thumbnail"] = m["thumbnail"]
        if not entry["league"] and m["league"]:
            entry["league"] = m["league"]

        # Thêm nguồn BLV (tránh duplicate URL)
        existing_urls = {s["detail_url"] for s in entry["blv_sources"]}
        if m["detail_url"] not in existing_urls:
            entry["blv_sources"].append({
                "blv":        m["blv"] or "",
                "detail_url": m["detail_url"],
            })

    result = list(merged.values())
    # Sắp xếp: live trước → upcoming → finished, rồi theo sort_key
    priority = {"live": 0, "upcoming": 1, "finished": 2}
    result.sort(key=lambda x: (priority.get(x["status"],9), x.get("sort_key","")))
    return result

def extract_matches(html, only_featured=True):
    bs = parse_html(html)
    raw, seen_urls = [], set()
    for card in find_match_cards(bs, only_featured):
        m = parse_card(card)
        if m and m["detail_url"] not in seen_urls:
            seen_urls.add(m["detail_url"])
            raw.append(m)
    # Gộp trận trùng tên
    merged = merge_matches(raw)
    log(f"  → {len(raw)} card → gộp còn {len(merged)} trận")
    return merged

# ── Lấy stream từ 1 URL ──────────────────────────────────────

def extract_streams_from_url(detail_url: str, scraper, blv_name: str = "") -> list:
    """
    Crawl 1 trang chi tiết, trả về list stream.
    Mỗi stream đã gắn blv_name để đặt tên link.
    """
    if not detail_url: return []
    html = fetch(detail_url, scraper, retries=2)
    if not html: return []
    bs = parse_html(html)
    seen, raw = set(), []

    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url)>12:
            seen.add(url)
            raw.append({"name":name,"url":url,"type":kind,"blv":blv_name,"referer":detail_url})

    for iframe in bs.find_all("iframe",src=True):
        src = iframe["src"]
        if re.search(r"live|stream|embed|player|sport|watch",src,re.I): add("embed",src,"iframe")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)',html):
        add("HLS",m.group(1),"hls")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.mpd(?:[?#][^\s\'"<>\]\\]*)?)',html):
        add("DASH",m.group(1),"dash")
    for script in bs.find_all("script"):
        c = script.string or ""
        for m in re.finditer(
            r'"(?:file|src|source|stream|url|hls|playlist|videoUrl|streamUrl)"\s*:\s*"(https?://[^"]+)"',c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video|play",u,re.I): add("Stream config",u,"hls")
        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl|playerUrl)\s*=\s*["\']([^"\']+)["\']',c):
            u = m.group(1)
            if u.startswith("http"): add("JS stream",u,"hls")
    for a in bs.find_all("a",href=True):
        href,txt = a["href"],a.get_text(strip=True)
        if re.search(r"xem|live|watch|stream|truc.?tiep|play|server",txt+href,re.I):
            if href.startswith("http") and href!=detail_url: add(txt or "Link",href,"hls")

    if not raw:
        raw.append({"name":"Trang trực tiếp","url":detail_url,
                    "type":"iframe","blv":blv_name,"referer":detail_url})
        return raw
    return filter_match_streams(raw)

def extract_all_streams(match: dict, scraper) -> list:
    """
    Crawl tất cả blv_sources của 1 trận.
    Trả về list stream đã gắn nhãn BLV, không trùng URL.
    """
    all_streams = []
    seen_urls   = set()

    for src in match["blv_sources"]:
        blv_name   = src["blv"] or ""
        detail_url = src["detail_url"]
        streams    = extract_streams_from_url(detail_url, scraper, blv_name)
        log(f"    🔗 {detail_url[-50:]} → {len(streams)} stream"
            + (f" (🎙 {blv_name})" if blv_name else ""))

        for s in streams:
            if s["url"] not in seen_urls:
                seen_urls.add(s["url"])
                all_streams.append(s)

    return all_streams

# ── Display title ─────────────────────────────────────────────

def build_display_title(m: dict) -> str:
    base  = m["base_title"]
    score = m["score"]
    t     = m["time_str"]
    d     = m["date_str"]

    if m["status"] == "live":
        if score and score != "VS":
            home, away = m["home_team"], m["away_team"]
            if home and away:
                return f"{home} {score} {away}  🔴"
        return f"{base}  🔴 LIVE"
    elif m["status"] == "finished":
        if score and score != "VS":
            home, away = m["home_team"], m["away_team"]
            if home and away:
                return f"{home} {score} {away}  ✅"
        return f"{base}  ✅ KT"
    else:
        time_info = ""
        if t and d:  time_info = f"  🕐 {t} | {d}"
        elif t:      time_info = f"  🕐 {t}"
        elif d:      time_info = f"  📅 {d}"
        return f"{base}{time_info}"

# ── Build channel ─────────────────────────────────────────────

def build_channel(m: dict, all_streams: list, index: int) -> dict:
    ch_id        = make_id("qc", str(index), re.sub(r"[^a-z0-9]","-",m["base_title"].lower())[:24])
    display_name = build_display_title(m)
    blv_sources  = m.get("blv_sources", [])
    n_blv        = len(blv_sources)  # số nguồn BLV

    # ── Labels ────────────────────────────────────────────────
    labels = []
    score     = m.get("score","")
    blv_names = [s["blv"] for s in blv_sources if s["blv"]]
    has_blv   = bool(blv_names) or n_blv > 1

    # Label 1 — trạng thái: top-left luôn
    status_cfg = {
        "live":     {"text":"● Live",         "color":"#E73131","text_color":"#ffffff"},
        "upcoming": {"text":"🕐 Sắp diễn ra","color":"#d54f1a","text_color":"#ffffff"},
        "finished": {"text":"✅ Kết thúc",    "color":"#444444","text_color":"#ffffff"},
    }.get(m["status"], {"text":"● Live","color":"#E73131","text_color":"#ffffff"})
    labels.append({**status_cfg, "position":"top-left"})

    # Label 2 — BLV: top-right (chỉ khi có BLV)
    # - Nhiều BLV → "🎙 3 BLV"
    # - 1 BLV     → "🎙 Tên BLV"
    if n_blv > 1:
        labels.append({"text":f"🎙 {n_blv} BLV","position":"top-right",
                       "color":"#00601f","text_color":"#ffffff"})
    elif blv_names:
        labels.append({"text":f"🎙 {blv_names[0]}","position":"top-right",
                       "color":"#00601f","text_color":"#ffffff"})

    # Label 3 — tỉ số: bottom-right
    if score and score != "VS":
        if m["status"] == "live":
            labels.append({"text":f"⚽ {score}","position":"bottom-right",
                           "color":"#E73131","text_color":"#ffffff"})
        elif m["status"] == "finished":
            labels.append({"text":f"KT {score}","position":"bottom-right",
                           "color":"#444444","text_color":"#ffffff"})

    # Label 4 — ngày giờ: bottom-left (chỉ upcoming)
    if m["status"] == "upcoming" and (m["time_str"] or m["date_str"]):
        tl = ""
        if m["time_str"] and m["date_str"]: tl = f"{m['time_str']} | {m['date_str']}"
        elif m["time_str"]:                 tl = m["time_str"]
        elif m["date_str"]:                 tl = m["date_str"]
        if tl:
            labels.append({"text":f"🕐 {tl}","position":"bottom-left",
                           "color":"#1a5fa8","text_color":"#ffffff"})

    # ── Stream links ──────────────────────────────────────────
    slinks    = []
    link_idx  = 0

    if all_streams:
        # Nhóm stream theo BLV
        blv_groups: dict[str, list] = {}
        for s in all_streams:
            key = s.get("blv") or "Không rõ"
            blv_groups.setdefault(key, []).append(s)

        for blv_name, streams in blv_groups.items():
            filtered = filter_match_streams(streams)
            for s in filtered:
                quality   = s.get("name","Auto")
                # Đặt tên link: "BLV Tên — HD" hoặc "BLV Tên" nếu chỉ 1 chất lượng
                if blv_name and blv_name != "Không rõ":
                    if len(filtered) > 1:
                        link_name = f"🎙 {blv_name} — {quality}"
                    else:
                        link_name = f"🎙 {blv_name}"
                else:
                    link_name = f"Link {link_idx+1}" if quality == "Auto" else quality

                # Referer từ trang BLV tương ứng
                referer = s.get("referer", m["blv_sources"][0]["detail_url"] if blv_sources else BASE_URL+"/")

                slinks.append({
                    "id":      make_id(ch_id, "lnk", str(link_idx)),
                    "name":    link_name,
                    "type":    s["type"],
                    "default": link_idx == 0,
                    "url":     s["url"],
                    "request_headers": [
                        {"key":"Referer",    "value":referer},
                        {"key":"User-Agent", "value":CHROME_UA},
                    ],
                })
                link_idx += 1

    # Fallback: không có stream → link trang đầu tiên
    if not slinks:
        fallback_url = blv_sources[0]["detail_url"] if blv_sources else BASE_URL+"/"
        slinks.append({
            "id":      make_id(ch_id, "lnk", "0"),
            "name":    "Link 1",
            "type":    "iframe",
            "default": True,
            "url":     fallback_url,
            "request_headers": [
                {"key":"Referer",    "value":fallback_url},
                {"key":"User-Agent", "value":CHROME_UA},
            ],
        })

    # ── Thumbnail ─────────────────────────────────────────────
    img_obj = None
    if m["thumbnail"]:
        img_obj = {"padding":1,"background_color":"#ececec","display":"contain",
                   "url":m["thumbnail"],"width":1600,"height":1200}

    # ── Stream group name ─────────────────────────────────────
    parts = ["Trực tiếp"]
    if m["league"]:  parts.append(m["league"])
    if n_blv > 1:    parts.append(f"{n_blv} BLV")
    stream_name = " · ".join(parts)

    return {
        "id":            ch_id,
        "name":          display_name,
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
                "name": display_name,
                "streams": [{
                    "id":           make_id(ch_id, "st"),
                    "name":         stream_name,
                    "stream_links": slinks,
                }],
            }],
        }],
    }

# ── Root JSON ─────────────────────────────────────────────────

def build_iptv_json(channels, now_str, group_name):
    return {
        "id":          "quechoa9-live",
        "name":        "QueCho9 — Trực tiếp bóng đá",
        "url":         BASE_URL + "/",
        "description": f"Cập nhật lúc {now_str} (ICT)",
        "disable_ads": True,
        "color":       "#0f3460",
        "grid_number": 3,
        "image":       {"type":"cover","url":f"{BASE_URL}/favicon.ico"},
        "groups": [{
            "id":       "tam-diem",
            "name":     group_name,
            "image":    None,
            "channels": channels,
        }],
    }

# ── Main ──────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all",       action="store_true")
    ap.add_argument("--no-stream", action="store_true")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🏟  CRAWLER — quechoa9.live  v9  (gộp BLV + tỉ số)")
    log("═"*62 + "\n")

    now_vn        = datetime.now(VN_TZ)
    now_str       = now_vn.strftime("%d/%m/%Y %H:%M ICT")
    only_featured = not args.all
    group_name    = "🔥 Tất cả trận đấu" if args.all else "🔥 Trận đấu tâm điểm"
    scraper       = make_scraper()

    log("📥 Bước 1: Tải trang chủ...")
    html = fetch(BASE_URL, scraper)
    if not html: log("❌ Không tải được trang chủ."); sys.exit(1)
    if "Just a moment" in html or "cf-browser-verification" in html:
        log("⚠ Cloudflare challenge — thử lại sau."); sys.exit(1)

    log(f"\n🔍 Bước 2: Phân tích + gộp trận trùng tên...")
    matches = extract_matches(html, only_featured)
    if not matches: log("  ⚠ Không tìm thấy trận nào."); sys.exit(1)

    log(f"\n  ✅ {len(matches)} trận (sau khi gộp):\n")
    for i, m in enumerate(matches, 1):
        icon    = {"live":"🔴","finished":"✅","upcoming":"🕐"}.get(m["status"],"⚽")
        score   = f"  [{m['score']}]" if m.get("score") and m["score"]!="VS" else ""
        dt      = f"  {m['time_str']}" if m.get("time_str") else ""
        dt     += f" {m['date_str']}"  if m.get("date_str") else ""
        n_src   = len(m.get("blv_sources",[]))
        blv_s   = f"  🎙 {n_src} BLV" if n_src > 1 else (f"  🎙 {m['blv_sources'][0]['blv']}" if n_src==1 and m["blv_sources"][0]["blv"] else "")
        log(f"  {icon} [{i:02d}] {m['base_title']}{score}{dt}{blv_s}")

    channels = []
    if args.no_stream:
        log("\n⏭ Bỏ qua stream")
        for i, m in enumerate(matches, 1):
            channels.append(build_channel(m, [], i))
    else:
        log(f"\n📡 Bước 3: Lấy stream links (tất cả BLV)...")
        for i, m in enumerate(matches, 1):
            n_src = len(m.get("blv_sources",[]))
            log(f"\n  [{i:02d}/{len(matches)}] {m['base_title']}  ({n_src} nguồn)")
            all_streams = extract_all_streams(m, scraper)
            log(f"    📡 Tổng {len(all_streams)} stream link")
            channels.append(build_channel(m, all_streams, i))
            time.sleep(1.0)

    result = build_iptv_json(channels, now_str, group_name)
    with open(args.output,"w",encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*62}")
    log(f"  ✅ Xong!  📁 {args.output}  ⚽ {len(channels)} trận")
    log(f"  🕐 Cập nhật: {now_str}")
    log("═"*62 + "\n")

if __name__ == "__main__":
    main()
