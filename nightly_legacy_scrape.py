#!/usr/bin/env python3
"""Nightly batch: scrape ALL legacy e-bidding systems.
Designed to run as cron job at 02:00 JST daily.
Estimated runtime: 30-60 minutes.
"""
import sqlite3
import urllib.request
import urllib.parse
import ssl
import http.cookiejar
import re
import time
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

DB = "/app/data/monitor.db"
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

STATS = {"systems": {}, "total_new": 0, "errors": []}

def mko():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=SSL_CTX)
    )

def get(opener, url, enc="shift_jis", timeout=15):
    try:
        r = opener.open(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 ToG-Nightly/1.0"}), timeout=timeout)
        d = r.read()
        for e in [enc, "utf-8", "cp932", "euc-jp"]:
            try: return d.decode(e)
            except: pass
        return d.decode("utf-8", errors="replace")
    except Exception as e:
        return ""

def post(opener, url, data, enc="shift_jis", timeout=15):
    try:
        pd = urllib.parse.urlencode(data).encode() if isinstance(data, dict) else data.encode()
        r = opener.open(urllib.request.Request(url, data=pd, headers={
            "User-Agent": "Mozilla/5.0 ToG-Nightly/1.0",
            "Content-Type": "application/x-www-form-urlencoded"
        }), timeout=timeout)
        d = r.read()
        for e in [enc, "utf-8", "cp932"]:
            try: return d.decode(e)
            except: pass
        return d.decode("utf-8", errors="replace")
    except:
        return ""

def cat(t):
    for c, ks in [("construction",["工事","修繕","改修","建設","土木","舗装","解体"]),
                  ("service",["委託","業務","役務","保守","清掃","運営","警備","給食"]),
                  ("goods",["購入","物品","リース","賃貸借","車両","印刷"]),
                  ("consulting",["設計","測量","調査","コンサル","策定","監理"]),
                  ("it",["システム","DX","AI","デジタル","ICT","ネットワーク"])]:
        for k in ks:
            if k in t: return c
    return "other"

def extract_dl(text):
    """HTMLテキスト片からdeadline(YYYY-MM-DD)を抽出"""
    if not text: return None
    m = re.search(r'(\d{4})[/\-.](\d{1,2})[/\-.](\d{1,2})', text)
    if m:
        y,mo,d = int(m.group(1)),int(m.group(2)),int(m.group(3))
        if 2020<=y<=2030 and 1<=mo<=12 and 1<=d<=31:
            return f"{y}-{mo:02d}-{d:02d}"
    m = re.search(r'令和\s*(\d{1,2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日', text)
    if m:
        y=2018+int(m.group(1)); mo=int(m.group(2)); d=int(m.group(3))
        if 1<=mo<=12 and 1<=d<=31: return f"{y}-{mo:02d}-{d:02d}"
    m = re.search(r'[RＲ]\s*(\d{1,2})\s*[./]\s*(\d{1,2})\s*[./]\s*(\d{1,2})', text)
    if m:
        y=2018+int(m.group(1)); mo=int(m.group(2)); d=int(m.group(3))
        if 1<=mo<=12 and 1<=d<=31: return f"{y}-{mo:02d}-{d:02d}"
    return None

def itype(method=""):
    if "一般" in method or "制限付" in method: return "general_competitive"
    if "プロポーザル" in method: return "proposal"
    if "指名" in method: return "designated_competitive"
    if "随意" in method: return "negotiated"
    return "other"

# ============================================================
# 1. CYDEEN PAN (大阪 23市)
# ============================================================
def scrape_cydeen(cur):
    log.info("=== CYDEEN PAN (Osaka) ===")
    BASE = "https://www.nyusatsu.ebid-osaka.jp"
    MUNIS = {
        "0202":"272027","0203":"271047","0204":"271063","0205":"272051",
        "0206":"272060","0207":"272078","0208":"272086","0209":"271233",
        "0210":"271101","0211":"272108","0212":"271128","0214":"271845",
        "0215":"272159","0216":"272167","0218":"272183","0219":"272191",
        "0220":"271209","0221":"271829","0222":"272213","0223":"271241",
        "0227":"271276","0229":"272299","0230":"272302",
    }
    total = 0
    for kk, mc in MUNIS.items():
        try:
            o = mko()
            get(o, f"{BASE}/pan/PAN010.do?KIKAN_NO={kk}&SCREEN_ID=PAN010")
            h = get(o, f"{BASE}/pan/PAN030.do?KIKAN_NO={kk}&SCREEN_ID=PAN030&PARAM=1&HYOJI_KENSU=100&INDEX=0")
            ls = [l.strip() for l in re.findall(r'>([^<]{3,200})<', h) if l.strip() and len(l.strip()) > 3]
            i = 0
            while i < len(ls) - 4:
                if any(k in ls[i+1] for k in ["競争入札","プロポーザル","随意契約","見積"]):
                    dl = None
                    for j in range(i+2, min(i+6, len(ls))):
                        m = re.search(r'(\d{4})/(\d{2})/(\d{2})', ls[j])
                        if m: dl = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"; break
                    dept = ls[i+3] if i+3 < len(ls) else ""
                    cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,deadline,department,url,category) VALUES (?,?,?,?,?,?,?,?)",
                        (mc, datetime.now().strftime("%Y-%m-%d"), ls[i][:200], itype(ls[i+1]), dl, dept[:100],
                         f"{BASE}/pan/PAN010.do?KIKAN_NO={kk}", cat(ls[i])))
                    total += cur.rowcount
                    i += 6
                else:
                    i += 1
        except Exception as e:
            STATS["errors"].append(f"CYDEEN {kk}: {str(e)[:50]}")
        time.sleep(0.5)
    STATS["systems"]["cydeen"] = total
    log.info(f"  CYDEEN: {total}")
    return total

# ============================================================
# 2. SuperCALS SaaS (5県)
# ============================================================
def scrape_supercals(cur):
    log.info("=== SuperCALS SaaS ===")
    INSTANCES = [
        ("https://www.ppi.cals-shiz.jp", "220000", "静岡"),
        ("https://www.chiba-ep-bis.supercals.jp", "120000", "千葉"),
        ("https://www.ep-bis.pref.niigata.jp", "150000", "新潟"),
        ("https://www.ppi.e-nagano.lg.jp", "200000", "長野"),
        ("https://www.ep-bis.supercals.jp", "470000", "沖縄"),
    ]
    total = 0
    for base, muni, name in INSTANCES:
        try:
            o = mko()
            get(o, f"{base}/ebidPPIPublish/EjPPIj")
            post(o, f"{base}/ebidPPIPublish/EjPPIj", {"ejParameterID": "StartPage", "KikanNO": "null"})
            get(o, f"{base}/ebidPPIPublish/EjPPIj?ejParameterID=EjPSJ01&ejShousaiDispFlag=null&ejProcessName=getCondPage")
            h = post(o, f"{base}/ebidPPIPublish/EjPPIj", {
                "ejParameterID": "EjPSJ01", "ejProcessName": "findList",
                "Nendo": str(datetime.now().year), "KikanNO": "", "ChoutatsuCD": "00",
                "selectKokoku": "1", "BidStDate": "", "BidEnDate": "",
                "mojisel1": "", "mojisel2": "", "kkselect": "AND",
                "ejSortSequence": "desc", "ejMaxDisplayRowCount": "100",
                "ejDisplaySort": "030006", "getStpos": "0", "AllhitSize": "0",
                "ejShousaiDispFlag": "", "BukyokuNO": "", "KoujiSyubetu": "",
            })
            count = 0
            chunks = re.findall(r'>([^<]{3,300})<', h)
            for idx, t in enumerate(chunks):
                t = t.strip()
                if any(k in t for k in ["工事","業務","委託","設計","調査","測量","橋梁","舗装"]):
                    if not any(k in t for k in ["システム","トップ","メニュー","フレーム","ブラウザ","Explorer"]):
                        # 後続チャンクからdeadline抽出
                        dl = None
                        for j in range(idx+1, min(idx+8, len(chunks))):
                            dl = extract_dl(chunks[j])
                            if dl: break
                        cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,deadline,url,category) VALUES (?,?,?,?,?,?,?)",
                            (muni, datetime.now().strftime("%Y-%m-%d"), t[:200], "general_competitive",
                             dl, f"{base}/ebidPPIPublish/EjPPIj", cat(t)))
                        count += cur.rowcount
            total += count
            log.info(f"  {name}: {count}")
        except Exception as e:
            STATS["errors"].append(f"SuperCALS {name}: {str(e)[:50]}")
        time.sleep(1)
    STATS["systems"]["supercals"] = total
    return total

# ============================================================
# 3. 神奈川DENTYO (30自治体)
# ============================================================
def scrape_kanagawa(cur):
    log.info("=== Kanagawa DENTYO ===")
    BASE = "https://nyusatsu-joho.e-kanagawa.lg.jp"
    DANTAI = ["0001","0201","0203","0204","0205","0206","0207","0208","0209","0210",
              "0211","0212","0213","0214","0215","0216","0217","0218","0301","0321",
              "0341","0342","0361","0362","0363","0366","0382","0384","0401","0402"]
    total = 0
    o = mko()
    get(o, f"{BASE}/DENTYO/GPPI_MENU")
    for d in DANTAI:
        try:
            h = get(o, f"{BASE}/DENTYO/P5000_INFORMATION?hdn_dantai={d}")
            count = 0
            chunks = re.findall(r'>([^<]{3,300})<', h)
            for idx, t in enumerate(chunks):
                t = t.strip()
                if any(k in t for k in ["工事","業務","委託","設計","調査","プロポーザル","入札"]):
                    if not any(k in t for k in ["トップ","メニュー","サイト","フレーム"]):
                        mc = f"14{d[1:]}00" if len(d) == 4 else "140000"
                        dl = None
                        for j in range(idx+1, min(idx+8, len(chunks))):
                            dl = extract_dl(chunks[j])
                            if dl: break
                        cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,deadline,url,category) VALUES (?,?,?,?,?,?,?)",
                            (mc, datetime.now().strftime("%Y-%m-%d"), t[:200], "general_competitive",
                             dl, f"{BASE}/DENTYO/P5000_INFORMATION?hdn_dantai={d}", cat(t)))
                        count += cur.rowcount
            total += count
        except:
            pass
        time.sleep(0.3)
    STATS["systems"]["kanagawa"] = total
    log.info(f"  Kanagawa: {total}")
    return total

# ============================================================
# 4. e-Tokyo (49自治体 × 工事+物品)
# ============================================================
def scrape_etokyo(cur):
    log.info("=== e-Tokyo ===")
    ET = "https://www.e-tokyo.lg.jp/choutatu_ppij/ppij/pub"
    total = 0

    for search_type in ["1", "2"]:  # 1=工事, 2=物品
        o = mko()
        get(o, ET, enc="cp932")
        get(o, f"{ET}?s=P001&a=1", enc="cp932")
        get(o, f"{ET}?s=P001&a=2", enc="cp932")
        post(o, ET, {"s": "P002", "a": search_type}, enc="cp932")

        for gc in list(range(101, 124)) + list(range(201, 230)):
            try:
                params = f"s=P002&a=3&govCode={gc}&year=&bidWayCode=&maxDispRowCount=100&dispKind1=2&dispOrder1=1&dispKind2=1&dispOrder2=0&pubStDate=&pubEndDate=&kiboStDate=&kiboEndDate=&ankenName=&categoryCode=&categoryNm=&constKbnCd=&selectConst=&selectItem=&itemKbnCd="
                h = post(o, ET, params, enc="cp932")
                trs = re.findall(r'<tr[^>]*>(.*?)</tr>', h, re.DOTALL)
                for tr in trs:
                    tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
                    if len(tds) >= 6:
                        cells = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
                        muni_name = cells[0]
                        title = cells[1]
                        deadline_str = cells[4] if len(cells) > 4 else ""
                        method = cells[6] if len(cells) > 6 else ""
                        if title and len(title) > 5 and any(k in muni_name for k in ["市","区","町","村"]):
                            dl = None
                            dl_m = re.search(r'(\d{4})/(\d+)/(\d+)', deadline_str)
                            if dl_m:
                                dl = f"{dl_m.group(1)}-{int(dl_m.group(2)):02d}-{int(dl_m.group(3)):02d}"
                            cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,deadline,department,url,category) VALUES (?,?,?,?,?,?,?,?)",
                                ("130000", datetime.now().strftime("%Y-%m-%d"), title[:200],
                                 itype(method), dl, muni_name, ET, cat(title)))
                            total += cur.rowcount
            except:
                pass
            time.sleep(0.3)

    STATS["systems"]["e_tokyo"] = total
    log.info(f"  e-Tokyo: {total}")
    return total

# ============================================================
# 5. 群馬g-cals (13自治体)
# ============================================================
def scrape_gunma(cur):
    log.info("=== Gunma g-cals ===")
    BASE = "https://portal.g-cals.e-gunma.lg.jp"
    MUNIS = {"0010":"100000","0201":"102016","0202":"102024","0203":"102032",
             "0204":"102041","0205":"102059","0206":"102067","0207":"102075",
             "0208":"102083","0209":"102091","0210":"102105","0211":"102113","0212":"102121"}
    total = 0
    for kikan, muni in MUNIS.items():
        try:
            o = mko()
            h = get(o, f"{BASE}/ebia/servlet/p?job=AcDantaiZTop&kikan_no={kikan}", enc="utf-8")
            count = 0
            chunks = re.findall(r'>([^<]{3,200})<', h)
            for idx, t in enumerate(chunks):
                t = t.strip()
                if any(k in t for k in ["工事","業務","委託","購入","設計","調査","入札"]):
                    dl = None
                    for j in range(idx+1, min(idx+8, len(chunks))):
                        dl = extract_dl(chunks[j])
                        if dl: break
                    cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,deadline,url,category) VALUES (?,?,?,?,?,?,?)",
                        (muni, datetime.now().strftime("%Y-%m-%d"), t[:200], "general_competitive",
                         dl, f"{BASE}/ebia/servlet/p?job=AcDantaiZTop&kikan_no={kikan}", cat(t)))
                    count += cur.rowcount
            total += count
        except:
            pass
        time.sleep(0.3)
    STATS["systems"]["gunma"] = total
    log.info(f"  Gunma: {total}")
    return total

# ============================================================
# 6. efftis (9インスタンス)
# ============================================================
def scrape_efftis(cur):
    log.info("=== efftis ===")
    INSTANCES = [
        ("toyama", "/ebid02/PPI/Public", [("162019","富山市"),("162027","高岡市"),("162043","魚津市"),
            ("162051","氷見市"),("162060","滑川市"),("162078","黒部市"),("162086","砺波市"),("162094","小矢部市"),("162108","南砺市")]),
        ("akita", "/PPI/Public", [("052019","秋田市")]),
        ("nara", "/PPI/Public", [("292010","奈良市")]),
        ("akashi", "/PPI/Public", [("282031","明石市")]),
        ("sakai", "/ebid01/PPI/Public", [("271403","堺市")]),
        ("higashiosaka", "/PPI/Public", [("271276","東大阪市")]),
        ("ise", "/PPI/Public", [("242039","伊勢市")]),
        ("iwaki", "/PPI/Public", [("072041","いわき市")]),
        ("kakogawa", "/PPI/Public", [("282103","加古川市")]),
    ]
    total = 0
    for sub, path, munis in INSTANCES:
        base = f"https://{sub}.efftis.jp{path}"
        for mc, name in munis:
            try:
                o = mko()
                kno = mc if len(mc) == 6 else "0001"
                get(o, f"{base}/PPUBC00100?kikanno={kno}")
                for screen in ["PPUBC01200", "PPUBC00400"]:
                    for kbn in ["00", "01", "11"]:
                        h = get(o, f"{base}/PPUBC00100!link?screenId={screen}&chotatsu_kbn={kbn}&organizationNumber={kno}")
                        if not h or len(h) < 1000: continue
                        chunks = re.findall(r'>([^<]{3,200})<', h)
                        for idx, t in enumerate(chunks):
                            t = t.strip()
                            if any(k in t for k in ["工事","業務","委託","購入","設計","調査","プロポーザル"]):
                                stype = "award" if screen == "PPUBC00400" else "general_competitive"
                                dl = None
                                for j in range(idx+1, min(idx+8, len(chunks))):
                                    dl = extract_dl(chunks[j])
                                    if dl: break
                                cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,deadline,url,category) VALUES (?,?,?,?,?,?,?)",
                                    (mc, datetime.now().strftime("%Y-%m-%d"), t[:200], stype,
                                     dl, f"{base}/PPUBC00100?kikanno={kno}", cat(t)))
                                total += cur.rowcount
                        time.sleep(0.2)
            except:
                pass
            time.sleep(0.3)
    STATS["systems"]["efftis"] = total
    log.info(f"  efftis: {total}")
    return total

# ============================================================
# 7. KKJ API (daily refresh)
# ============================================================
def scrape_kkj(cur):
    log.info("=== KKJ API (daily) ===")
    # Just run the existing fetch scripts
    import subprocess
    try:
        r1 = subprocess.run(["python3", "/app/fetch_pportal_daily.py"], capture_output=True, text=True, timeout=120)
        log.info(f"  P-Portal: {r1.stdout.strip()[-50:]}")
    except:
        pass
    try:
        r2 = subprocess.run(["python3", "/app/fetch_gov_rss.py"], capture_output=True, text=True, timeout=120)
        log.info(f"  Gov RSS: {r2.stdout.strip()[-50:]}")
    except:
        pass
    STATS["systems"]["kkj_daily"] = "delegated"

# ============================================================
# MAIN
# ============================================================
def main():
    log.info("=" * 60)
    log.info("NIGHTLY LEGACY SCRAPE STARTING")
    log.info("=" * 60)

    conn = sqlite3.connect(DB, timeout=60)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM procurement_items")
    before = cur.fetchone()[0]

    grand = 0
    grand += scrape_cydeen(cur); conn.commit()
    grand += scrape_supercals(cur); conn.commit()
    grand += scrape_kanagawa(cur); conn.commit()
    grand += scrape_etokyo(cur); conn.commit()
    grand += scrape_gunma(cur); conn.commit()
    grand += scrape_efftis(cur); conn.commit()
    scrape_kkj(cur)

    cur.execute("SELECT COUNT(*) FROM procurement_items")
    after = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline >= date('now')")
    open_bids = cur.fetchone()[0]

    STATS["total_new"] = grand
    STATS["db_before"] = before
    STATS["db_after"] = after
    STATS["open_bids"] = open_bids
    STATS["timestamp"] = datetime.now().isoformat()

    # Save stats
    with open("/app/logs/nightly_stats.json", "w") as f:
        json.dump(STATS, f, ensure_ascii=False, indent=2)

    log.info("=" * 60)
    log.info(f"NIGHTLY SCRAPE COMPLETE")
    log.info(f"  New items: {grand}")
    log.info(f"  DB: {before} → {after}")
    log.info(f"  Open bids: {open_bids}")
    log.info(f"  Systems: {json.dumps(STATS['systems'])}")
    if STATS["errors"]:
        log.info(f"  Errors: {len(STATS['errors'])}")
    log.info("=" * 60)

    conn.close()

if __name__ == "__main__":
    main()
