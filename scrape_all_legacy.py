#!/usr/bin/env python3
"""Scrape ALL reverse-engineered legacy systems in one shot.
1. CYDEEN PAN (Osaka 30+ cities) — GET REST
2. efftis (17 instances) — Cookie + GET
3. e-HARP (Hokkaido) — Session + .do
"""
import sqlite3
import urllib.request
import urllib.parse
import ssl
import http.cookiejar
import re
import time
from datetime import datetime

DB = "/app/data/monitor.db"
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

def classify_cat(t):
    for c, ks in [("construction",["工事","修繕","改修","建設","舗装","解体","補修","土木"]),
                  ("service",["業務委託","委託","役務","保守","清掃","運営","警備","給食","管理"]),
                  ("goods",["購入","物品","リース","賃貸借","車両","機器","印刷"]),
                  ("consulting",["設計","測量","調査","コンサル","策定","監理"]),
                  ("it",["システム","DX","AI","デジタル","ICT","ネットワーク"])]:
        for k in ks:
            if k in t: return c
    return "other"

def make_opener():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=SSL_CTX)
    ), cj

def fetch(opener, url, timeout=15, encoding="utf-8"):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"})
        resp = opener.open(req, timeout=timeout)
        data = resp.read()
        for enc in [encoding, "shift_jis", "euc-jp", "utf-8", "cp932"]:
            try:
                return data.decode(enc)
            except:
                continue
        return data.decode("utf-8", errors="replace")
    except:
        return ""

# ==========================================
# 1. CYDEEN PAN (Osaka)
# ==========================================
def scrape_cydeen_pan(cur):
    print("\n=== CYDEEN PAN (Osaka) ===", flush=True)
    BASE = "https://www.nyusatsu.ebid-osaka.jp"

    # Extended municipality list
    MUNIS = {
        "0203": "271047", "0209": "271233", "0210": "271101",
        "0212": "271128", "0214": "271845", "0220": "271209",
        "0221": "271829", "0223": "271241", "0227": "271276",
    }

    total = 0
    for kikan, muni_code in MUNIS.items():
        try:
            opener, _ = make_opener()
            fetch(opener, f"{BASE}/pan/PAN010.do?KIKAN_NO={kikan}&SCREEN_ID=PAN010")

            params = urllib.parse.urlencode({
                "KIKAN_NO": kikan, "SCREEN_ID": "PAN030",
                "PARAM": "1", "HYOJI_KENSU": "100", "INDEX": "0"
            })
            text = fetch(opener, f"{BASE}/pan/PAN030.do?{params}", encoding="shift_jis")

            lines = [l.strip() for l in re.findall(r'>([^<]{3,200})<', text) if l.strip() and len(l.strip()) > 3]

            i = 0
            count = 0
            while i < len(lines) - 4:
                if any(k in lines[i+1] for k in ["競争入札", "プロポーザル", "随意契約", "見積合わせ"]):
                    title = lines[i]
                    deadline = None
                    for j in range(i+2, min(i+6, len(lines))):
                        dl_m = re.search(r'(\d{4})/(\d{2})/(\d{2})', lines[j])
                        if dl_m:
                            deadline = f"{dl_m.group(1)}-{dl_m.group(2)}-{dl_m.group(3)}"
                            break
                    dept = lines[i+3] if i+3 < len(lines) else ""

                    cur.execute("""INSERT OR IGNORE INTO procurement_items
                        (muni_code,detected_at,title,item_type,deadline,department,url,category)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        (muni_code, datetime.now().strftime("%Y-%m-%d"), title[:200],
                         "general_competitive", deadline, dept[:100],
                         f"{BASE}/pan/PAN010.do?KIKAN_NO={kikan}", classify_cat(title)))
                    total += cur.rowcount
                    count += 1
                    i += 6
                else:
                    i += 1
            if count > 0:
                print(f"  {muni_code}: {count} items", flush=True)
        except:
            pass
        time.sleep(0.5)

    print(f"  CYDEEN total: {total}", flush=True)
    return total

# ==========================================
# 2. efftis (Toshiba)
# ==========================================
def scrape_efftis(cur):
    print("\n=== efftis (Toshiba) ===", flush=True)

    INSTANCES = [
        ("toyama", "/ebid02/PPI/Public", [
            ("162019","富山市"), ("162027","高岡市"), ("162043","魚津市"),
            ("162051","氷見市"), ("162060","滑川市"), ("162078","黒部市"),
            ("162086","砺波市"), ("162094","小矢部市"), ("162108","南砺市"),
        ]),
        ("akita", "/PPI/Public", [("052019","秋田市")]),
        ("nara", "/PPI/Public", [("292010","奈良市")]),
        ("akashi", "/PPI/Public", [("282031","明石市")]),
        ("sakai", "/ebid01/PPI/Public", [("271403","堺市")]),
        ("higashiosaka", "/PPI/Public", [("271276","東大阪市")]),
        ("ise", "/PPI/Public", [("242039","伊勢市")]),
        ("iwaki", "/PPI/Public", [("072041","いわき市")]),
        ("kakogawa", "/PPI/Public", [("282103","加古川市")]),
    ]

    SCREENS = [("PPUBC01200","公告"), ("PPUBC00400","結果")]
    KBNS = [("00","工事"), ("01","コンサル"), ("11","物品")]

    total = 0
    for subdomain, path, munis in INSTANCES:
        base = f"https://{subdomain}.efftis.jp{path}"
        for muni_code, muni_name in munis:
            try:
                opener, _ = make_opener()
                kikanno = muni_code if len(muni_code) == 6 else "0001"
                fetch(opener, f"{base}/PPUBC00100?kikanno={kikanno}")

                for screen, sname in SCREENS:
                    for kbn, kname in KBNS:
                        url = f"{base}/PPUBC00100!link?screenId={screen}&chotatsu_kbn={kbn}&organizationNumber={kikanno}"
                        html = fetch(opener, url)
                        if not html or len(html) < 1000:
                            continue

                        # Extract items from HTML tables
                        texts = re.findall(r'>([^<]{5,150})<', html)
                        for t in texts:
                            t = t.strip()
                            if len(t) > 10 and any(k in t for k in ["工事","業務","委託","購入","設計","調査","プロポーザル"]):
                                itype = "award" if sname == "結果" else "general_competitive"
                                cur.execute("""INSERT OR IGNORE INTO procurement_items
                                    (muni_code,detected_at,title,item_type,url,category)
                                    VALUES (?,?,?,?,?,?)""",
                                    (muni_code, datetime.now().strftime("%Y-%m-%d"),
                                     t[:200], itype, f"{base}/PPUBC00100?kikanno={kikanno}",
                                     classify_cat(t)))
                                total += cur.rowcount
                        time.sleep(0.3)
            except:
                pass
            time.sleep(0.5)
        print(f"  {subdomain}: done", flush=True)

    print(f"  efftis total: {total}", flush=True)
    return total

# ==========================================
# 3. e-HARP (Hokkaido)
# ==========================================
def scrape_eharp(cur):
    print("\n=== e-HARP (Hokkaido) ===", flush=True)
    BASE = "https://www.idc.e-harp.jp"

    try:
        opener, _ = make_opener()
        html = fetch(opener, f"{BASE}/Public/PortalWeb/PublicHomeInit.do")

        if "セッション" in html or len(html) < 500:
            print("  Session init failed", flush=True)
            return 0

        # Try bid notice search
        search_html = fetch(opener, f"{BASE}/Public/PortalWeb/BidNoticeSearchInit.do")
        if search_html and len(search_html) > 1000:
            items = re.findall(r'>([^<]{10,150})<', search_html)
            count = 0
            for t in items:
                t = t.strip()
                if any(k in t for k in ["工事","業務","委託","購入","設計"]):
                    cur.execute("""INSERT OR IGNORE INTO procurement_items
                        (muni_code,detected_at,title,item_type,url,category)
                        VALUES (?,?,?,?,?,?)""",
                        ("010000", datetime.now().strftime("%Y-%m-%d"),
                         t[:200], "general_competitive",
                         f"{BASE}/Public/PortalWeb/BidNoticeSearchInit.do",
                         classify_cat(t)))
                    count += cur.rowcount
            print(f"  e-HARP: {count} items", flush=True)
            return count
    except Exception as e:
        print(f"  e-HARP error: {str(e)[:50]}", flush=True)
    return 0

# ==========================================
# SuperCALS DENTYO (Fujitsu) — Kanagawa, Oita, Fukuoka
# ==========================================
def scrape_supercals_dentyo(cur):
    print("\n=== SuperCALS DENTYO ===", flush=True)

    INSTANCES = [
        ("https://nyusatsu-joho.e-kanagawa.lg.jp", "神奈川", {
            "0001": "140000", "0201": "141003", "0203": "141305",
            "0205": "142018", "0206": "142069", "0207": "142051",
            "0208": "142077", "0209": "142034",
        }),
        ("https://www.t-elis.pref.oita.lg.jp", "大分", {
            "1111": "440000", "2111": "442011", "2112": "442020",
        }),
    ]

    total = 0
    for base, pref_name, munis in INSTANCES:
        for dantai_code, muni_code in munis.items():
            try:
                opener, _ = make_opener()
                # Menu page first
                fetch(opener, f"{base}/DENTYO/GPPI_MENU")

                # Bid list for municipality
                html = fetch(opener, f"{base}/DENTYO/GP5000_10F?hdn_dantai={dantai_code}", encoding="shift_jis")
                if not html or len(html) < 500:
                    continue

                texts = re.findall(r'>([^<]{8,150})<', html)
                count = 0
                for t in texts:
                    t = t.strip()
                    if any(k in t for k in ["工事","業務","委託","購入","設計","調査","プロポーザル"]):
                        cur.execute("""INSERT OR IGNORE INTO procurement_items
                            (muni_code,detected_at,title,item_type,url,category)
                            VALUES (?,?,?,?,?,?)""",
                            (muni_code, datetime.now().strftime("%Y-%m-%d"),
                             t[:200], "general_competitive",
                             f"{base}/DENTYO/GP5000_10F?hdn_dantai={dantai_code}",
                             classify_cat(t)))
                        count += cur.rowcount
                total += count
                if count > 0:
                    print(f"  {pref_name} {dantai_code}: {count}", flush=True)
            except:
                pass
            time.sleep(0.5)

    print(f"  DENTYO total: {total}", flush=True)
    return total

# ==========================================
# MAIN
# ==========================================
def main():
    conn = sqlite3.connect(DB, timeout=30)
    cur = conn.cursor()

    grand_total = 0
    grand_total += scrape_cydeen_pan(cur)
    conn.commit()
    grand_total += scrape_efftis(cur)
    conn.commit()
    grand_total += scrape_eharp(cur)
    conn.commit()
    grand_total += scrape_supercals_dentyo(cur)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM procurement_items")
    print(f"\nGrand total new: {grand_total}", flush=True)
    print(f"DB total: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline >= date('now')")
    print(f"Open bids: {cur.fetchone()[0]}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
