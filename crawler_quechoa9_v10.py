#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler Trận Đấu Tâm Điểm — quechoa9.live  v10            ║
║   Gộp toàn bộ tính năng từ v9 + v9_2:                       ║
║   + Gộp trận trùng tên, tách stream theo từng BLV            ║
║   + Thumbnail CDN từ quechoa.json (ảnh cả 2 đội)            ║
║   + Fuzzy match tên đội (Atletico/Atlético, Bayern/Munich)   ║
║   + Parse ngày/giờ chuẩn, label tỉ số + ngày giờ            ║
║   + Placeholder ảnh khi không có thumbnail                   ║
║   + Fallback crawl detail page khi không match CDN           ║
╚══════════════════════════════════════════════════════════════╝
Cài đặt: pip install cloudscraper beautifulsoup4 lxml requests

Chạy:
    python crawler_quechoa9_v10.py                  # mặc định (tâm điểm)
    python crawler_quechoa9_v10.py --all            # tất cả trận
    python crawler_quechoa9_v10.py --no-stream      # không crawl stream
    python crawler_quechoa9_v10.py --output out.json
"""

import argparse, base64, hashlib, io, json, os, re, sys, time, unicodedata
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

try:
    import cloudscraper
    from bs4 import BeautifulSoup, NavigableString
    import requests
except ImportError:
    print("Cài đặt: pip install cloudscraper beautifulsoup4 lxml requests"); sys.exit(1)

# ── Constants ─────────────────────────────────────────────────
BASE_URL     = "https://quechoa9.live"
QUECHOA_JSON = "https://pub-26bab83910ab4b5781549d12d2f0ef6f.r2.dev/quechoa.json"
OUTPUT_FILE  = "quechoa9_iptv.json"
CHROME_UA    = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36")
VN_TZ        = timezone(timedelta(hours=7))

PLACEHOLDER_IMG = {
    "padding": 2, "background_color": "#0f3460", "display": "contain",
    "url": "https://quechoa9.live/favicon.ico", "width": 512, "height": 512,
}
# Thumbnail nhúng base64 thẳng vào JSON — không cần file ngoài
THUMB_JPEG_QUALITY = 65  # 60-75 là hợp lý (~7-10 KB/ảnh)

# ── Tạo thumbnail PNG bằng Pillow (logo 2 đội thật) ─────────
try:
    from PIL import Image, ImageDraw, ImageFont
    _PILLOW_OK = True
except ImportError:
    _PILLOW_OK = False
    log = print  # temporary

_FONT_BOLD = _FONT_REG = None
for _fp in [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "C:/Windows/Fonts/arialbd.ttf",
]:
    try:
        if _PILLOW_OK:
            from PIL import ImageFont as _IF
            _FONT_BOLD = _IF.truetype(_fp, 24)
            _FONT_REG  = _IF.truetype(_fp.replace("Bold","").replace("bd",""), 18)
        break
    except Exception:
        pass

def _dl_logo(url: str, size: tuple = (150, 150)) -> "Image.Image | None":
    """Download logo từ URL, resize về size, nền trong suốt."""
    if not url or not _PILLOW_OK: return None
    try:
        r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        img = Image.open(io.BytesIO(r.content)).convert("RGBA")
        img.thumbnail(size, Image.LANCZOS)
        canvas = Image.new("RGBA", size, (0, 0, 0, 0))
        ox = (size[0] - img.width)  // 2
        oy = (size[1] - img.height) // 2
        canvas.paste(img, (ox, oy), img)
        return canvas
    except Exception:
        return None

def _initials(name: str) -> str:
    return "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"

def _draw_logo_or_initials(draw, img_canvas, logo_img, cx, cy, r, name):
    """Vẽ logo (nếu có) hoặc chữ tắt vào canvas."""
    # Vòng tròn nền mờ
    draw.ellipse([(cx-r-6, cy-r-6), (cx+r+6, cy+r+6)], fill=(255,255,255,20))
    if logo_img:
        lw, lh = logo_img.size
        img_canvas.paste(logo_img, (cx - lw//2, cy - lh//2), logo_img)
    else:
        draw.ellipse([(cx-r, cy-r), (cx+r, cy+r)], fill=(30, 60, 110, 200))
        try:
            fb = ImageFont.truetype(list({
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            })[0], r)
        except Exception:
            fb = ImageFont.load_default()
        draw.text((cx, cy), _initials(name), fill="white", font=fb, anchor="mm")

def make_match_thumbnail_png(
    home_team: str, away_team: str,
    logo_a_url: str = "", logo_b_url: str = "",
    time_str: str = "", date_str: str = "",
    status: str = "upcoming", score: str = "",
    league: str = "",
) -> bytes | None:
    """
    Tạo thumbnail PNG 800×450 với logo 2 đội thật.
    Trả về bytes PNG, hoặc None nếu Pillow không có.
    """
    if not _PILLOW_OK:
        return None

    W, H = 800, 450
    LOGO_R = 75   # bán kính vùng logo

    # Gradient nền
    img  = Image.new("RGBA", (W, H), (8, 20, 32, 255))
    draw = ImageDraw.Draw(img)
    c1 = (8, 20, 32); c2 = (15, 40, 65)
    for y in range(H):
        t = y / H
        r = int(c1[0] + (c2[0]-c1[0])*t)
        g = int(c1[1] + (c2[1]-c1[1])*t)
        b = int(c1[2] + (c2[2]-c1[2])*t)
        draw.line([(0, y), (W, y)], fill=(r, g, b, 255))

    # Trang trí sân
    draw.ellipse([(W//2-220, H//2+90), (W//2+220, H//2+310)],
                 outline=(255,255,255,10), width=2)
    draw.line([(W//2, 55), (W//2, H)], fill=(255,255,255,12), width=1)

    # Thanh league
    draw.rectangle([(0, 0), (W, 54)], fill=(0, 0, 0, 120))
    draw.line([(60, 54), (W-60, 54)], fill=(255,255,255,40), width=1)
    if league:
        try: fnt = ImageFont.truetype(
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18)
        except: fnt = ImageFont.load_default()
        draw.text((W//2, 27), league[:40], fill=(255,255,255,200),
                  font=fnt, anchor="mm")

    # Download logos (song song nếu có)
    logo_a = _dl_logo(logo_a_url, (LOGO_R*2, LOGO_R*2))
    logo_b = _dl_logo(logo_b_url, (LOGO_R*2, LOGO_R*2))

    # Vị trí logo
    lx, ly = 165, 175    # home center
    rx, ry = W-165, 175  # away center

    _draw_logo_or_initials(draw, img, logo_a, lx, ly, LOGO_R, home_team)
    _draw_logo_or_initials(draw, img, logo_b, rx, ry, LOGO_R, away_team)

    # Tên đội
    try: fn_team = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 20)
    except: fn_team = ImageFont.load_default()
    name_y = ly + LOGO_R + 18
    draw.text((lx, name_y), home_team[:16], fill=(255,255,255,230),
              font=fn_team, anchor="mm")
    draw.text((rx, name_y), away_team[:16], fill=(255,255,255,230),
              font=fn_team, anchor="mm")

    # Vùng giữa: tỉ số hoặc giờ thi đấu
    cx, cy = W//2, ly
    if status == "live" and score and score not in ("", "VS"):
        ctr = score; ctr_col = (255, 70, 70, 255)
        sub = "● LIVE"; sub_col = (255, 120, 120, 255)
    elif status == "finished" and score and score not in ("", "VS"):
        ctr = score; ctr_col = (255, 255, 255, 255)
        sub = "Kết thúc"; sub_col = (170, 170, 170, 255)
    else:
        ctr = time_str or "VS"; ctr_col = (255, 255, 255, 255)
        sub = date_str or ""; sub_col = (180, 180, 180, 255)

    try: fn_big = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 52)
    except: fn_big = ImageFont.load_default()
    try: fn_sub = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
    except: fn_sub = ImageFont.load_default()

    # Gạch ngang hai bên chữ giữa
    draw.line([(cx-72, cy-10), (cx-26, cy-10)], fill=(255,255,255,80), width=2)
    draw.line([(cx+26, cy-10), (cx+72, cy-10)], fill=(255,255,255,80), width=2)
    draw.text((cx, cy-8), ctr, fill=ctr_col, font=fn_big, anchor="mm")
    if sub:
        draw.text((cx, cy+40), sub, fill=sub_col, font=fn_sub, anchor="mm")

    # Fade bottom
    for y in range(H-60, H):
        alpha = int(255 * (y-(H-60)) / 60)
        draw.line([(0,y),(W,y)], fill=(8,20,32,alpha))

    out = io.BytesIO()
    img.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()

def make_match_thumbnail_b64(
    home_team: str, away_team: str,
    logo_a_url: str = "", logo_b_url: str = "",
    time_str: str = "", date_str: str = "",
    status: str = "upcoming", score: str = "",
    league: str = "",
) -> str:
    """
    Tạo thumbnail JPEG → encode base64 → trả về data URI.
    Nhúng thẳng vào JSON, không cần file ngoài, không cần server.
    Kích thước: ~7-10 KB base64 mỗi ảnh.
    """
    png_bytes = make_match_thumbnail_png(
        home_team=home_team, away_team=away_team,
        logo_a_url=logo_a_url, logo_b_url=logo_b_url,
        time_str=time_str, date_str=date_str,
        status=status, score=score, league=league,
    )
    if not png_bytes:
        return ""
    # Chuyển PNG → JPEG (nhẹ hơn ~3-4x) rồi base64
    try:
        from PIL import Image
        img  = Image.open(io.BytesIO(png_bytes)).convert("RGB")
        out  = io.BytesIO()
        img.save(out, format="JPEG", quality=THUMB_JPEG_QUALITY, optimize=True)
        b64  = base64.b64encode(out.getvalue()).decode("ascii")
        return f"data:image/jpeg;base64,{b64}"
    except Exception:
        # Fallback: dùng PNG base64 nếu không convert được
        b64 = base64.b64encode(png_bytes).decode("ascii")
        return f"data:image/png;base64,{b64}"



# ── Helpers ───────────────────────────────────────────────────
def make_id(*parts):
    raw  = "-".join(str(p) for p in parts if p)
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", raw).strip("-").lower()
    return (slug[:48] + "-" + hashlib.md5(raw.encode()).hexdigest()[:8]
            if len(slug) > 56 else slug)

def log(msg): print(msg, flush=True)

# ── Fuzzy match tên đội (từ v9_2) ────────────────────────────
_STOPWORDS = {
    "fc","cf","sc","ac","as","bv","vfl","vfb","ssv","rsc","asc",
    "w","u17","u18","u19","u20","u21","u23","b","ii","iii",
    "women","men","youth","ladies","reserves",
}

def normalize_name(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", ascii_str.lower())

def tokenize(name: str) -> list[str]:
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    tokens = re.sub(r"[^a-z0-9\s]", " ", ascii_str.lower()).split()
    return [t for t in tokens if t not in _STOPWORDS and len(t) >= 2]

def _common_prefix_len(s1: str, s2: str) -> int:
    i = 0
    while i < len(s1) and i < len(s2) and s1[i] == s2[i]:
        i += 1
    return i

def team_match_score(a: str, b: str) -> float:
    na, nb = normalize_name(a), normalize_name(b)
    if na == nb: return 1.0
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    if len(shorter) >= 4 and shorter in longer: return 0.85
    if _common_prefix_len(na, nb) >= 7: return 0.80
    ta, tb = set(tokenize(a)), set(tokenize(b))
    if ta and tb:
        inter = len(ta & tb); union = len(ta | tb)
        jaccard = inter / union
        if jaccard >= 0.5: return jaccard
        st = ta if len(ta) <= len(tb) else tb
        lt = ta if len(ta) > len(tb) else tb
        if st and st.issubset(lt): return 0.7
    return 0.0

def pair_match_score(ha: str, aa: str, hb: str, ab: str) -> float:
    s1 = min(team_match_score(ha, hb), team_match_score(aa, ab))
    s2 = min(team_match_score(ha, ab), team_match_score(aa, hb))
    return max(s1, s2)

# ── HTTP ──────────────────────────────────────────────────────
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
    try:
        r = requests.get(QUECHOA_JSON, timeout=15, headers={"User-Agent": CHROME_UA})
        r.raise_for_status()
        data = r.json()
        total = sum(len(g.get("channels", [])) for g in data.get("groups", []))
        log(f"  ✓ quechoa.json: {total} trận")
        return data
    except Exception as e:
        log(f"  ⚠ Không fetch được quechoa.json: {e}")
        return {}

# ── Lookup từ quechoa.json (từ v9_2) ─────────────────────────
def build_quechoa_lookup(qdata: dict) -> dict:
    lookup = {}
    for group in qdata.get("groups", []):
        for ch in group.get("channels", []):
            meta   = ch.get("org_metadata", {})
            team_a = meta.get("team_a", "")
            team_b = meta.get("team_b", "")
            if not team_a or not team_b:
                continue

            thumb = meta.get("thumb") or (ch.get("image") or {}).get("url", "")

            streams, referer_url = [], ""
            for src in ch.get("sources", []):
                for cnt in src.get("contents", []):
                    for strm in cnt.get("streams", []):
                        for sl in strm.get("stream_links", []):
                            url   = sl.get("url", "")
                            stype = sl.get("type", "hls")
                            sname = sl.get("name", "")
                            if not referer_url:
                                for hdr in sl.get("request_headers", []):
                                    if hdr.get("key", "").lower() == "referer":
                                        referer_url = hdr.get("value", "")
                            if url:
                                streams.append({"name": sname, "url": url, "type": stype})

            info = {
                "thumb":       thumb,
                "streams":     streams,
                "league":      meta.get("league", ""),
                "logo_a":      meta.get("logo_a", ""),
                "logo_b":      meta.get("logo_b", ""),
                "referer_url": referer_url,
            }

            key_ab = normalize_name(team_a) + "_" + normalize_name(team_b)
            key_ba = normalize_name(team_b) + "_" + normalize_name(team_a)
            lookup[key_ab] = info
            lookup[key_ba] = info
            lookup["_" + normalize_name(team_a)] = info
            lookup["_" + normalize_name(team_b)] = info
            lookup.setdefault("_teams_", []).append((team_a, team_b, info))

    log(f"  → Lookup: {len([k for k in lookup if not k.startswith('_')])} cặp đội")
    return lookup

def find_in_lookup(lookup: dict, home: str, away: str) -> dict | None:
    if not lookup or not home or not away:
        return None
    h = normalize_name(home)
    a = normalize_name(away)

    # ① Exact pair
    for key in (f"{h}_{a}", f"{a}_{h}"):
        if key in lookup: return lookup[key]

    # ② Substring trên key
    for key, info in lookup.items():
        if key.startswith("_"): continue
        parts = key.split("_", 1)
        if len(parts) == 2:
            k1, k2 = parts
            if (h in k1 or k1 in h) and (a in k2 or k2 in a): return info
            if (a in k1 or k1 in a) and (h in k2 or k2 in h): return info

    # ③ Fuzzy token similarity
    best_score, best_info = 0.0, None
    for ta, tb, info in lookup.get("_teams_", []):
        score = pair_match_score(home, away, ta, tb)
        if score > best_score:
            best_score = score
            best_info  = info
    if best_score >= 0.5:
        return best_info

    # ④ Single team fallback
    for name in (h, a):
        if len(name) >= 5 and f"_{name}" in lookup:
            return lookup[f"_{name}"]
    return None

# ── Parse ngày giờ (từ v9) ────────────────────────────────────
def parse_match_datetime(match_time: str):
    if not match_time:
        return ("", "", "")
    m = re.search(r"(\d{1,2}):(\d{2})\s*\|?\s*(\d{1,2})[./](\d{1,2})", match_time)
    if m:
        hh, mm = m.group(1), m.group(2)
        day, mon = m.group(3).zfill(2), m.group(4).zfill(2)
        return (f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}")
    m2 = re.search(r"(\d{1,2}):(\d{2})", match_time)
    if m2:
        hh, mm = m2.group(1), m2.group(2)
        today  = datetime.now(VN_TZ)
        return (f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}")
    return (match_time, "", "")

# ── HTML helpers ──────────────────────────────────────────────
def parse_html(html): return BeautifulSoup(html, "lxml")

def has_classes(tag, *classes):
    j = " ".join(tag.get("class", []))
    return all(c in j for c in classes)

def find_all_with(tag, element, *classes):
    if not classes: return []
    return [t for t in tag.find_all(element, class_=classes[0])
            if has_classes(t, *classes[1:])]

# ── Card parsing ──────────────────────────────────────────────
def find_match_cards(bs, only_featured=True):
    all_cards = find_all_with(bs, "a", "hover:border-[#83ff65]", "rounded-xl", "block")
    if not only_featured or not all_cards:
        log(f"  → {len(all_cards)} card (toàn trang)"); return all_cards
    header_node = None
    for node in bs.find_all(string=re.compile(r"TÂM ĐIỂM|tâm điểm", re.I)):
        header_node = node.find_parent()
        if header_node: break
    if not header_node:
        log("  ℹ Không thấy 'Tâm điểm' → toàn trang"); return all_cards
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

    if re.search(r"\bLive\b", raw_text, re.I):                    status = "live"
    elif re.search(r"Kết thúc|Finished|\bFT\b", raw_text, re.I): status = "finished"
    else:                                                           status = "upcoming"

    time_div = next((d for d in card.find_all("div", class_="from-[#051f00]")
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
        score = (f"{nums[0]}-{nums[1]}" if len(nums) >= 2
                 else ("VS" if score_div.get_text(strip=True).upper() == "VS" else ""))

    blv       = _extract_blv(card)
    thumbnail = ""
    img = card.find("img")
    if img:
        src = img.get("src") or img.get("data-src") or ""
        thumbnail = src if src.startswith("http") else urljoin(BASE_URL, src)

    base_title = (f"{home_team} vs {away_team}" if home_team and away_team
                  else home_team or re.sub(r"\s{2,}", " ", raw_text)[:60])
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

# ── Gộp trận trùng tên (từ v9) ───────────────────────────────
def _normalize_title(title: str) -> str:
    t = title.lower().strip()
    t = re.sub(r"\s+", " ", t)
    return re.sub(r"[^\w\s]", "", t)

def merge_matches(raw_matches: list) -> list:
    merged: dict[str, dict] = {}
    for m in raw_matches:
        key = _normalize_title(m["base_title"])
        if key not in merged:
            merged[key] = {**m, "blv_sources": []}
        entry = merged[key]
        if not entry["score"] and m["score"]:             entry["score"]     = m["score"]
        if not entry["thumbnail"] and m["thumbnail"]:     entry["thumbnail"] = m["thumbnail"]
        if not entry["league"] and m["league"]:           entry["league"]    = m["league"]
        if entry["status"] == "upcoming" and m["status"] in ("live", "finished"):
            entry["status"] = m["status"]
        existing_urls = {s["detail_url"] for s in entry["blv_sources"]}
        if m["detail_url"] not in existing_urls:
            entry["blv_sources"].append({"blv": m["blv"] or "", "detail_url": m["detail_url"]})

    result = list(merged.values())
    priority = {"live": 0, "upcoming": 1, "finished": 2}
    result.sort(key=lambda x: (priority.get(x["status"], 9), x.get("sort_key", "")))
    return result

def extract_matches(html, only_featured=True):
    bs = parse_html(html)
    raw, seen_urls = [], set()
    for card in find_match_cards(bs, only_featured):
        m = parse_card(card)
        if m and m["detail_url"] not in seen_urls:
            seen_urls.add(m["detail_url"])
            raw.append(m)
    merged = merge_matches(raw)
    log(f"  → {len(raw)} card → gộp còn {len(merged)} trận")
    return merged

# ── Stream quality ────────────────────────────────────────────
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
    hls   = [s for s in streams if s["type"] == "hls"]
    other = [s for s in streams if s["type"] != "hls"]
    if hls:
        base  = _stream_base(hls[0]["url"])
        group = [{**s, "name": _quality_label(s["url"])} for s in hls
                 if _stream_base(s["url"]) == base]
        group.sort(key=lambda x: _QUALITY_ORDER.get(x["name"], 99))
        return group + other
    return other

# ── Thumbnail từ detail page ───────────────────────────────────────────
CDN_THUMB_RE = re.compile(
    r'https?://pub-[a-f0-9]+\.r2\.dev/[^\s\'"<>\\]+\.webp(?:\?[^\s\'"<>\\]*)?',
    re.I
)
IMG_URL_RE = re.compile(
    r'https?://[^\s\'"<>\\]+\.(?:webp|jpg|jpeg|png)(?:\?[^\s\'"<>\\]*)?',
    re.I
)
_THUMB_DOMAINS = re.compile(
    r'(?:r2\.dev|cloudinary\.com|imgbb\.com|imgur\.com'
    r'|api-sports\.io|thesportsdb\.com|media\.|images\.|img\.|cdn\.|static\.)',
    re.I
)
_THUMB_EXCLUDE = re.compile(
    r'(?:favicon|logo-site|avatar|icon-\d|sprite|\d{1,2}x\d{1,2}|/ads?/)',
    re.I
)

def _is_valid_thumb(url: str) -> bool:
    return bool(url) and not _THUMB_EXCLUDE.search(url) and len(url) > 20

def extract_thumb_from_detail(html: str, bs) -> str:
    # 1) __NEXT_DATA__ CDN webp
    next_tag = bs.find('script', id='__NEXT_DATA__')
    if next_tag and next_tag.string:
        m = CDN_THUMB_RE.search(next_tag.string)
        if m and _is_valid_thumb(m.group(0)):
            return m.group(0)
    # 2) og:image / twitter:image
    for attr in [{'property':'og:image'}, {'name':'og:image'}, {'name':'twitter:image'}]:
        tag = bs.find('meta', attrs=attr)
        if tag:
            url = tag.get('content', '')
            if url and _is_valid_thumb(url) and IMG_URL_RE.match(url):
                return url
    # 3) CDN r2.dev webp bat ky trong HTML
    m = CDN_THUMB_RE.search(html)
    if m and _is_valid_thumb(m.group(0)):
        return m.group(0)
    # 4) <img> lon nhat (width >= 200)
    best_url, best_w = '', 0
    for img in bs.find_all('img', src=True):
        src = img.get('src', '') or img.get('data-src', '')
        if not src or not src.startswith('http'): continue
        if not _is_valid_thumb(src):              continue
        if not IMG_URL_RE.search(src):            continue
        try:    w = int(img.get('width', 0))
        except: w = 0
        if w > best_w: best_w, best_url = w, src
    if best_url and best_w >= 200:
        return best_url
    # 5) URL anh hop le trong HTML tu domain CDN
    for m in IMG_URL_RE.finditer(html):
        url = m.group(0)
        if _is_valid_thumb(url) and _THUMB_DOMAINS.search(url):
            return url
    return best_url

# ── Crawl stream từ 1 URL (từ v9) ────────────────────────────
def extract_streams_from_url(detail_url: str, html: str, bs) -> list:
    seen, raw = set(), []
    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url) > 12:
            seen.add(url); raw.append({"name":name,"url":url,"type":kind,"referer":detail_url})

    for iframe in bs.find_all("iframe", src=True):
        if re.search(r"live|stream|embed|player|sport|watch", iframe["src"], re.I):
            add("embed", iframe["src"], "iframe")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("HLS", m.group(1), "hls")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.mpd(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("DASH", m.group(1), "dash")
    for script in bs.find_all("script"):
        c = script.string or ""
        for m in re.finditer(
                r'"(?:file|src|source|stream|url|hls|playlist|videoUrl|streamUrl)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video|play", u, re.I): add("Stream config", u, "hls")
        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl|playerUrl)\s*=\s*["\']([^"\']+)["\']', c):
            u = m.group(1)
            if u.startswith("http"): add("JS stream", u, "hls")
    for a in bs.find_all("a", href=True):
        href, txt = a["href"], a.get_text(strip=True)
        if re.search(r"xem|live|watch|stream|truc.?tiep|play|server", txt+href, re.I):
            if href.startswith("http") and href != detail_url: add(txt or "Link", href, "hls")

    if not raw:
        raw.append({"name":"Trang trực tiếp","url":detail_url,"type":"iframe","referer":detail_url})
        return raw
    hls = [s for s in raw if s["type"] == "hls"]
    return filter_streams(hls) if hls else raw

def crawl_blv_source(detail_url: str, blv_name: str, scraper) -> tuple[list, str]:
    """Crawl 1 trang BLV → (streams, thumb). Tái sử dụng HTML đã fetch."""
    html = fetch_html(detail_url, scraper, retries=2)
    if not html:
        return [], ""
    bs     = parse_html(html)
    thumb  = extract_thumb_from_detail(html, bs)
    streams = extract_streams_from_url(detail_url, html, bs)
    # Gắn tên BLV vào từng stream
    for s in streams:
        s["blv"] = blv_name
    return streams, thumb

# ── Display title (từ v9) ─────────────────────────────────────
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

# ── Build channel (gộp cả 2 phiên bản) ───────────────────────
def build_channel(m: dict, all_streams: list, thumb: str,
                  index: int, qc_league: str = "") -> dict:

    ch_id        = make_id("qc", str(index), re.sub(r"[^a-z0-9]","-",m["base_title"].lower())[:24])
    display_name = build_display_title(m)
    blv_sources  = m.get("blv_sources", [])
    n_blv        = len(blv_sources)
    score        = m.get("score", "")
    league       = qc_league or m.get("league", "")

    # ── Labels ────────────────────────────────────────────────
    labels    = []
    blv_names = [s["blv"] for s in blv_sources if s["blv"]]

    # 1. Trạng thái — top-left luôn
    status_cfg = {
        "live":     {"text":"● Live",         "color":"#E73131","text_color":"#ffffff"},
        "upcoming": {"text":"🕐 Sắp diễn ra","color":"#d54f1a","text_color":"#ffffff"},
        "finished": {"text":"✅ Kết thúc",    "color":"#444444","text_color":"#ffffff"},
    }.get(m["status"], {"text":"● Live","color":"#E73131","text_color":"#ffffff"})
    labels.append({**status_cfg, "position": "top-left"})

    # 2. BLV — top-right
    if n_blv > 1:
        labels.append({"text":f"🎙 {n_blv} BLV","position":"top-right",
                       "color":"#00601f","text_color":"#ffffff"})
    elif blv_names:
        labels.append({"text":f"🎙 {blv_names[0]}","position":"top-right",
                       "color":"#00601f","text_color":"#ffffff"})

    # 3. Tỉ số — bottom-right
    if score and score != "VS":
        if m["status"] == "live":
            labels.append({"text":f"⚽ {score}","position":"bottom-right",
                           "color":"#E73131","text_color":"#ffffff"})
        elif m["status"] == "finished":
            labels.append({"text":f"KT {score}","position":"bottom-right",
                           "color":"#444444","text_color":"#ffffff"})
    elif league:
        labels.append({"text":league[:30],"position":"bottom-right",
                       "color":"#00000099","text_color":"#ffffff"})

    # 4. Ngày giờ — bottom-left (chỉ upcoming)
    if m["status"] == "upcoming" and (m["time_str"] or m["date_str"]):
        tl = ""
        if m["time_str"] and m["date_str"]: tl = f"{m['time_str']} | {m['date_str']}"
        elif m["time_str"]:                 tl = m["time_str"]
        elif m["date_str"]:                 tl = m["date_str"]
        if tl:
            labels.append({"text":f"🕐 {tl}","position":"bottom-left",
                           "color":"#1a5fa8","text_color":"#ffffff"})

    # ── Stream objects — mỗi BLV = 1 stream riêng ────────────
    stream_objs = []
    blv_groups: dict[str, list] = {}
    for s in all_streams:
        key = s.get("blv") or "__no_blv__"
        blv_groups.setdefault(key, []).append(s)

    for blv_idx, (blv_key, raw_s) in enumerate(blv_groups.items()):
        filtered = filter_streams(raw_s) if raw_s else []
        if not filtered: continue

        stream_label = f"🎙 {blv_key}" if blv_key != "__no_blv__" else f"Nguồn {blv_idx+1}"

        slinks = []
        for lnk_idx, s in enumerate(filtered):
            quality   = s.get("name", "Auto")
            link_name = quality if quality != "Auto" else f"Link {lnk_idx+1}"
            referer   = s.get("referer", blv_sources[0]["detail_url"] if blv_sources else BASE_URL+"/")
            slinks.append({
                "id":      make_id(ch_id, f"b{blv_idx}", f"l{lnk_idx}"),
                "name":    link_name,
                "type":    s["type"],
                "default": lnk_idx == 0,
                "url":     s["url"],
                "request_headers": [
                    {"key":"Referer",    "value":referer},
                    {"key":"User-Agent", "value":CHROME_UA},
                ],
            })

        stream_objs.append({
            "id":           make_id(ch_id, f"st{blv_idx}"),
            "name":         stream_label,
            "stream_links": slinks,
        })

    # Fallback nếu không có stream nào
    if not stream_objs:
        fallback_url = blv_sources[0]["detail_url"] if blv_sources else BASE_URL + "/"
        stream_objs.append({
            "id":   make_id(ch_id, "st0"),
            "name": "Trực tiếp",
            "stream_links": [{
                "id":"lnk0","name":"Link 1","type":"iframe","default":True,
                "url": fallback_url,
                "request_headers": [
                    {"key":"Referer",    "value":fallback_url},
                    {"key":"User-Agent", "value":CHROME_UA},
                ],
            }],
        })

    # ── Thumbnail — nhúng base64 thẳng vào JSON ─────────────
    logo_a_url = m.get("_logo_a", "")
    logo_b_url = m.get("_logo_b", "")

    # Ưu tiên 1: thumbnail CDN quechoa (webp đẹp nhất, URL ngoài)
    if thumb and ("r2.dev/quechoa_thumbs" in thumb or thumb.startswith("http")):
        img_obj = {
            "padding": 1, "background_color": "#ececec",
            "display": "contain", "url": thumb,
            "width": 1600, "height": 1200,
        }
    else:
        # Ưu tiên 2: tạo JPEG base64 từ logo 2 đội (Pillow)
        data_uri = make_match_thumbnail_b64(
            home_team  = m["home_team"],
            away_team  = m["away_team"],
            logo_a_url = logo_a_url,
            logo_b_url = logo_b_url,
            time_str   = m.get("time_str", ""),
            date_str   = m.get("date_str", ""),
            status     = m["status"],
            score      = m.get("score", ""),
            league     = league,
        )
        if data_uri:
            img_obj = {
                "padding": 0, "background_color": "#0d2038",
                "display": "contain", "url": data_uri,
                "width": 800, "height": 450,
            }
        else:
            # Fallback cuối: placeholder
            img_obj = PLACEHOLDER_IMG

    # ── Tên content ───────────────────────────────────────────
    content_name = display_name
    if league: content_name += f" · {league}"

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
                "id":      make_id(ch_id, "ct"),
                "name":    content_name,
                "streams": stream_objs,
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
    ap.add_argument("--all",       action="store_true", help="Tất cả trận (không chỉ tâm điểm)")
    ap.add_argument("--no-stream", action="store_true", help="Không crawl stream (nhanh hơn)")
    ap.add_argument("--output",    default=OUTPUT_FILE)

    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🏟  CRAWLER — quechoa9.live  v10")
    log("  📸  Thumbnail CDN + 🎙 Tách stream theo BLV")
    log("═"*62 + "\n")

    now_vn        = datetime.now(VN_TZ)
    now_str       = now_vn.strftime("%d/%m/%Y %H:%M ICT")
    only_featured = not args.all
    group_name    = "🔥 Tất cả trận đấu" if args.all else "🔥 Trận đấu tâm điểm"

    # Bước 1: Fetch quechoa.json (thumbnail CDN)
    log("📥 Bước 1: Fetch quechoa.json CDN (thumbnail)...")
    qdata  = fetch_quechoa_json()
    lookup = build_quechoa_lookup(qdata)

    # Bước 2: Crawl danh sách trận
    scraper = make_scraper()
    log(f"\n📥 Bước 2: Tải trang chủ quechoa9.live...")
    html = fetch_html(BASE_URL, scraper)
    if not html: log("❌ Không tải được trang chủ."); sys.exit(1)
    if "Just a moment" in html or "cf-browser-verification" in html:
        log("⚠ Cloudflare challenge — thử lại sau."); sys.exit(1)

    log(f"\n🔍 Bước 2b: Phân tích + gộp trận trùng tên...")
    matches = extract_matches(html, only_featured)
    if not matches: log("  ⚠ Không tìm thấy trận nào."); sys.exit(1)

    log(f"\n  ✅ {len(matches)} trận (sau gộp):\n")
    for i, m in enumerate(matches, 1):
        icon  = {"live":"🔴","finished":"✅","upcoming":"🕐"}.get(m["status"],"⚽")
        score = f"  [{m['score']}]" if m.get("score") and m["score"] != "VS" else ""
        dt    = f"  {m['time_str']}" if m.get("time_str") else ""
        dt   += f" {m['date_str']}"  if m.get("date_str") else ""
        n_src = len(m.get("blv_sources", []))
        blv_s = f"  🎙 {n_src} BLV" if n_src > 1 else (
                f"  🎙 {m['blv_sources'][0]['blv']}" if n_src == 1 and m["blv_sources"][0]["blv"] else "")
        log(f"  {icon} [{i:02d}] {m['base_title']}{score}{dt}{blv_s}")

    # Bước 3: Ghép thumbnail CDN + crawl streams
    log(f"\n🖼  Bước 3: Ghép thumbnail CDN + crawl streams từng BLV...")
    channels      = []
    thumb_matched = 0

    for i, m in enumerate(matches, 1):
        # Tìm thumbnail từ quechoa.json (fuzzy match)
        info       = find_in_lookup(lookup, m["home_team"], m["away_team"])
        thumb      = ""
        qc_streams = []
        qc_league  = ""

        if info:
            thumb_matched += 1
            thumb      = info["thumb"]
            qc_streams = info["streams"]
            qc_league  = info["league"]
            m["_logo_a"] = info.get("logo_a","")
            m["_logo_b"] = info.get("logo_b","")
            log(f"\n  [{i:02d}/{len(matches)}] {m['base_title']}")
            log(f"    🖼  CDN: {thumb[thumb.rfind('/')+1:]}" if thumb else "    ⚠  Không có thumb CDN")
        else:
            qc_league  = ""
            m["_logo_a"] = ""; m["_logo_b"] = ""
            log(f"\n  [{i:02d}/{len(matches)}] {m['base_title']}")
            log(f"    ⚠  Không khớp quechoa.json")

        if args.no_stream:
            all_streams = []
        else:
            all_streams = []
            blv_sources = m.get("blv_sources", [])

            for src in blv_sources:
                blv_name   = src["blv"] or ""
                detail_url = src["detail_url"]
                log(f"    🔗 Crawl: {detail_url[-55:]} (🎙 {blv_name or 'không rõ BLV'})")
                streams, page_thumb = crawl_blv_source(detail_url, blv_name, scraper)

                # Lấy thumb từ detail page nếu CDN chưa có
                if not thumb and page_thumb:
                    thumb = page_thumb
                    log(f"    🖼  Detail: {page_thumb[page_thumb.rfind('/')+1:]}")

                seen_urls = {s["url"] for s in all_streams}
                for s in streams:
                    if s["url"] not in seen_urls:
                        seen_urls.add(s["url"])
                        all_streams.append(s)

            log(f"    📡 Tổng {len(all_streams)} stream ({len(blv_sources)} BLV)")
            time.sleep(0.8)

        channels.append(build_channel(m, all_streams, thumb, i, qc_league))

    log(f"\n  📊 Thumbnail CDN: {thumb_matched}/{len(matches)} trận khớp")

    # Bước 4: Ghi file
    result = build_iptv_json(channels, now_str, group_name)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*62}")
    log(f"  ✅ Xong!  📁 {args.output}  ⚽ {len(channels)} trận")
    log(f"  🕐 Cập nhật: {now_str}")
    log("═"*62 + "\n")

if __name__ == "__main__":
    main()
