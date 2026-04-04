#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler Trận Đấu Tâm Điểm — quechoa9.live  v7             ║
║   Schema khớp 100% với MonPlayer (dựa trên tctv.pro mẫu)    ║
╚══════════════════════════════════════════════════════════════╝
Cài đặt: pip install cloudscraper beautifulsoup4 lxml
"""

import argparse, hashlib, json, re, sys, time
from datetime import datetime
from urllib.parse import urljoin

try:
    import cloudscraper
    from bs4 import BeautifulSoup, NavigableString, Tag
except ImportError:
    print("Cài đặt: pip install cloudscraper beautifulsoup4 lxml"); sys.exit(1)

BASE_URL    = "https://quechoa9.live"
OUTPUT_FILE = "quechoa9_iptv.json"
CHROME_UA   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

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

_QUALITY_RE  = re.compile(r"[_-](?:full[_-]?hd|fhd|1080p?|720p?|480p?|360p?|hd|sd)$", re.I)
_QUALITY_MAP = {"hd":"HD","sd":"SD","full-hd":"Full HD","full_hd":"Full HD","fhd":"Full HD",
                "1080":"Full HD","1080p":"Full HD","720":"HD","720p":"HD","480":"SD","480p":"SD","360":"360p","360p":"360p"}
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
    hls = [s for s in streams if s["type"]=="hls"]
    other = [s for s in streams if s["type"]!="hls"]
    if hls:
        base  = _stream_base(hls[0]["url"])
        group = [{**s,"name":_quality_label(s["url"])} for s in hls if _stream_base(s["url"])==base]
        group.sort(key=lambda x:_QUALITY_ORDER.get(x["name"],99))
        return group + other
    return other

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
    avatar = next((s for s in card.find_all("span",class_="rounded-full") if has_classes(s,"bg-neutral-700")),None)
    if not avatar: return ""
    parent = avatar.find_parent("div")
    if not parent: return ""
    for child in parent.children:
        if isinstance(child,NavigableString):
            t = str(child).strip()
            if len(t)>=2: return t
    letter = avatar.get_text(strip=True)
    full   = parent.get_text(" ",strip=True)
    if full.startswith(letter):
        c = full[len(letter):].strip()
        if len(c)>=2: return c
    return ""

def parse_card(card):
    href = card.get("href","")
    detail_url = href if href.startswith("http") else urljoin(BASE_URL,href)
    raw_text   = card.get_text(" ",strip=True)
    if re.search(r"\bLive\b",raw_text,re.I):              status = "live"
    elif re.search(r"Kết thúc|Finished|\bFT\b",raw_text,re.I): status = "finished"
    else:                                                   status = "upcoming"
    time_div = next((d for d in card.find_all("div",class_="from-[#051f00]") if d.get_text(strip=True)),None)
    match_time = time_div.get_text(strip=True) if time_div else ""
    if not match_time:
        m = re.search(r"\d{1,2}:\d{2}\s*\|\s*\d{2}\.\d{2}",raw_text)
        if m: match_time = m.group(0)
    league = ""
    for d in card.find_all("div",class_="justify-center"):
        if not has_classes(d,"gap-1","w-full"): continue
        txt = d.get_text(strip=True)
        if txt and len(txt)>3 and not re.fullmatch(r"[\d:\s]+",txt): league=txt; break
    home_team = away_team = ""
    team_texts = []
    for d in [d for d in card.find_all("div",class_="flex-1") if has_classes(d,"flex-col","items-center")]:
        t = d.get_text(" ",strip=True)
        if t and len(t)>=2 and not re.fullmatch(r"[\d\s:]+",t): team_texts.append(t)
    if len(team_texts)>=2: home_team,away_team = team_texts[0],team_texts[1]
    elif len(team_texts)==1: home_team = team_texts[0]
    if not home_team:
        vm = re.search(r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34}?)\s+(?:VS|vs)\s+([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34})",raw_text,re.UNICODE)
        if vm: home_team,away_team = vm.group(1).strip(),vm.group(2).strip()
    score = ""
    score_div = next((d for d in card.find_all("div",class_="rounded-[20px]") if has_classes(d,"border-[#83ff65]")),None)
    if score_div:
        nums = re.findall(r"\d+",score_div.get_text())
        score = f"{nums[0]}:{nums[1]}" if len(nums)>=2 else ("VS" if score_div.get_text(strip=True).upper()=="VS" else "")
    blv = _extract_blv(card)
    thumbnail = ""
    img = card.find("img")
    if img:
        src = img.get("src") or img.get("data-src") or ""
        thumbnail = src if src.startswith("http") else urljoin(BASE_URL,src)
    title = (f"{home_team} vs {away_team}" if home_team and away_team
             else home_team or re.sub(r"\s{2,}"," ",raw_text)[:60])
    if not title or not detail_url: return None
    return {"title":title,"home_team":home_team,"away_team":away_team,"score":score,
            "status":status,"league":league,"match_time":match_time,
            "detail_url":detail_url,"thumbnail":thumbnail,"blv":blv}

def extract_matches(html, only_featured=True):
    bs = parse_html(html)
    result, seen = [], set()
    for card in find_match_cards(bs, only_featured):
        m = parse_card(card)
        if m and m["title"].lower() not in seen:
            seen.add(m["title"].lower()); result.append(m)
    return result

def extract_streams(detail_url, scraper):
    if not detail_url: return []
    html = fetch(detail_url, scraper, retries=2)
    if not html: return []
    bs = parse_html(html)
    seen, raw = set(), []
    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url)>12:
            seen.add(url); raw.append({"name":name,"url":url,"type":kind})
    for iframe in bs.find_all("iframe",src=True):
        src = iframe["src"]
        if re.search(r"live|stream|embed|player|sport|watch",src,re.I): add("embed",src,"iframe")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)',html):
        add("HLS",m.group(1),"hls")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.mpd(?:[?#][^\s\'"<>\]\\]*)?)',html):
        add("DASH",m.group(1),"dash")
    for script in bs.find_all("script"):
        c = script.string or ""
        for m in re.finditer(r'"(?:file|src|source|stream|url|hls|playlist|videoUrl|streamUrl)"\s*:\s*"(https?://[^"]+)"',c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video|play",u,re.I): add("Stream config",u,"hls")
        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl|playerUrl)\s*=\s*["\']([^"\']+)["\']',c):
            u = m.group(1)
            if u.startswith("http"): add("JS stream",u,"hls")
    for a in bs.find_all("a",href=True):
        href,txt = a["href"],a.get_text(strip=True)
        if re.search(r"xem|live|watch|stream|truc.?tiep|play|server",txt+href,re.I):
            if href.startswith("http") and href!=detail_url: add(txt or "Link",href,"hls")
    if not raw: add("Trang trực tiếp",detail_url,"iframe"); return raw
    return filter_match_streams(raw)

# ── Schema khớp file mẫu MonPlayer (tctv.pro / quechoa.json) ──

def build_channel(m, streams, index):
    ch_id = make_id("qc", str(index), re.sub(r"[^a-z0-9]","-",m["title"].lower())[:24])
    blv   = m.get("blv","")

    # Labels — khớp với schema tctv.pro: dùng màu có alpha, không dùng #00ffffff
    labels = []
    if blv:
        labels.append({
            "text":       f"🎙 {blv}",
            "position":   "top-left",
            "color":      "#00601f",
            "text_color": "#ffffff",
        })
    status_label = {
        "live":     {"text": "● Live",       "color": "#E73131", "text_color": "#ffffff"},
        "upcoming": {"text": "🕐 Sắp diễn ra","color": "#d54f1a", "text_color": "#ffffff"},
        "finished": {"text": "✅ Kết thúc",   "color": "#555555", "text_color": "#ffffff"},
    }.get(m["status"], {"text": "● Live", "color": "#E73131", "text_color": "#ffffff"})
    labels.append({
        "text":       status_label["text"],
        "position":   "top-right" if blv else "top-left",
        "color":      status_label["color"],
        "text_color": status_label["text_color"],
    })

    # request_headers — chỉ Referer + User-Agent (bỏ Origin, khớp mẫu)
    req_headers = [
        {"key": "Referer",    "value": m["detail_url"]},
        {"key": "User-Agent", "value": CHROME_UA},
    ]

    # Stream links
    slinks = []
    for j, sl in enumerate(streams):
        quality   = sl.get("name","Auto")
        link_name = f"Link {j+1}" if quality == "Auto" else quality
        slinks.append({
            "id":              make_id(ch_id, "lnk", str(j)),
            "name":            link_name,
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

    # Image — khớp với mẫu quechoa.json
    img_obj = None
    if m["thumbnail"]:
        img_obj = {
            "padding":          1,
            "background_color": "#ececec",
            "display":          "contain",
            "url":              m["thumbnail"],
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

def build_iptv_json(channels, now_str, group_name):
    """
    Root schema khớp với tctv.pro / quechoa.json:
    - Có 'description' và 'disable_ads'
    - 'image' dùng {'type':'cover','url':...}
    - group KHÔNG có 'display', 'grid_number', 'enable_detail' ở root group
    """
    return {
        "id":          "quechoa9-live",
        "name":        "QueCho9 — Trực tiếp bóng đá",
        "url":         BASE_URL + "/",
        "description": "Xem trực tiếp bóng đá và thể thao tổng hợp",
        "disable_ads": True,
        "color":       "#0f3460",
        "grid_number": 3,
        "image":       {"type": "cover", "url": f"{BASE_URL}/favicon.ico"},
        "groups": [{
            "id":       "tam-diem",
            "name":     group_name,
            "image":    None,
            "channels": channels,
        }],
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all",       action="store_true")
    ap.add_argument("--no-stream", action="store_true")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🏟  CRAWLER — quechoa9.live  v7  (schema MonPlayer)")
    log("═"*62 + "\n")

    now_str       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    only_featured = not args.all
    group_name    = "🔥 Tất cả trận đấu" if args.all else "🔥 Trận đấu tâm điểm"
    scraper       = make_scraper()

    log("📥 Bước 1: Tải trang chủ...")
    html = fetch(BASE_URL, scraper)
    if not html: log("❌ Không tải được trang chủ."); sys.exit(1)
    if "Just a moment" in html or "cf-browser-verification" in html:
        log("⚠ Cloudflare challenge — thử lại sau."); sys.exit(1)

    log(f"\n🔍 Bước 2: Phân tích {'tâm điểm' if only_featured else 'tất cả'}...")
    matches = extract_matches(html, only_featured)
    if not matches: log("  ⚠ Không tìm thấy trận nào."); sys.exit(1)

    log(f"\n  ✅ {len(matches)} trận:\n")
    for i, m in enumerate(matches, 1):
        icon = {"live":"🔴","finished":"✅","upcoming":"🕐"}.get(m["status"],"⚽")
        log(f"  {icon} [{i:02d}] {m['title']}" + (f"  🎙 {m['blv']}" if m.get("blv") else ""))

    channels = []
    if args.no_stream:
        log("\n⏭ Bỏ qua stream")
        for i, m in enumerate(matches, 1): channels.append(build_channel(m, [], i))
    else:
        log(f"\n📡 Bước 3: Lấy stream links...")
        for i, m in enumerate(matches, 1):
            log(f"\n  [{i:02d}/{len(matches)}] {m['title']}")
            streams = extract_streams(m["detail_url"], scraper)
            log(f"    📡 {len(streams)} stream" if streams else "    ⚠  Không có stream")
            channels.append(build_channel(m, streams, i))
            time.sleep(1.0)

    result = build_iptv_json(channels, now_str, group_name)
    with open(args.output,"w",encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*62}")
    log(f"  ✅ Xong!  📁 {args.output}  ⚽ {len(channels)} trận")
    log("═"*62 + "\n")

if __name__ == "__main__":
    main()
