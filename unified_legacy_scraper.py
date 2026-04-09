#!/usr/bin/env python3
"""Unified legacy e-bid scraper.

Targets:
1) e-Tokyo
2) SuperCALS
3) CYDEEN PAN
4) Kanagawa DENTYO

- Uses http.cookiejar for session persistence
- Disables SSL certificate verification
- Sleeps 0.5s between every HTTP request
- Extracts bid-like records from HTML by regular expressions
- Saves JSON result to /tmp/legacy_scrape_results.json
"""

from __future__ import annotations

import datetime as dt
import http.cookiejar
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional

REQUEST_INTERVAL_SEC = 0.5
OUTPUT_JSON = "/tmp/legacy_scrape_results.json"


@dataclass
class FetchResult:
    url: str
    status: str
    http_status: Optional[int]
    encoding: str
    html: str
    error: Optional[str] = None


class LegacyScraper:
    def __init__(self) -> None:
        self.cookie_jar = http.cookiejar.CookieJar()
        self.ssl_context = ssl._create_unverified_context()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=self.ssl_context),
            urllib.request.HTTPCookieProcessor(self.cookie_jar),
        )
        self.default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }

    def request(
        self,
        url: str,
        method: str = "GET",
        data: Optional[Dict[str, str]] = None,
        encoding: str = "utf-8",
        headers: Optional[Dict[str, str]] = None,
    ) -> FetchResult:
        merged_headers = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)

        payload = None
        if data is not None:
            payload = urllib.parse.urlencode(data).encode(encoding, errors="replace")

        req = urllib.request.Request(url=url, data=payload, method=method, headers=merged_headers)

        try:
            with self.opener.open(req, timeout=30) as resp:
                raw = resp.read()
                detected_encoding = encoding
                ctype = resp.headers.get_content_charset()
                if ctype:
                    detected_encoding = ctype

                html = raw.decode(detected_encoding, errors="replace")
                result = FetchResult(
                    url=url,
                    status="ok",
                    http_status=getattr(resp, "status", None),
                    encoding=detected_encoding,
                    html=html,
                )
        except urllib.error.HTTPError as exc:
            try:
                raw = exc.read()
                html = raw.decode(encoding, errors="replace")
            except Exception:
                html = ""
            result = FetchResult(
                url=url,
                status="http_error",
                http_status=getattr(exc, "code", None),
                encoding=encoding,
                html=html,
                error=f"HTTPError: {exc}",
            )
        except Exception as exc:  # pylint: disable=broad-except
            result = FetchResult(
                url=url,
                status="error",
                http_status=None,
                encoding=encoding,
                html="",
                error=f"{type(exc).__name__}: {exc}",
            )

        time.sleep(REQUEST_INTERVAL_SEC)
        return result


def strip_tags(html_fragment: str) -> str:
    text = re.sub(r"<script\\b[^>]*>.*?</script>", " ", html_fragment, flags=re.I | re.S)
    text = re.sub(r"<style\\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text, flags=re.I)
    text = re.sub(r"&amp;", "&", text, flags=re.I)
    text = re.sub(r"\\s+", " ", text)
    return text.strip()


def extract_bid_projects(html: str) -> List[Dict[str, object]]:
    """Heuristic regex extraction from legacy HTML tables/lists."""
    projects: List[Dict[str, object]] = []
    seen = set()

    keyword_pattern = re.compile(r"(入札|案件|調達|工事|業務|委託|物品|公募|公告)")

    # 1) Table rows with bid-like keywords
    for m in re.finditer(r"<tr\\b[^>]*>(.*?)</tr>", html, flags=re.I | re.S):
        row_html = m.group(1)
        row_text = strip_tags(row_html)
        if not row_text:
            continue
        if not keyword_pattern.search(row_text):
            continue

        cells = re.findall(r"<t[dh]\\b[^>]*>(.*?)</t[dh]>", row_html, flags=re.I | re.S)
        cleaned_cells = [strip_tags(c) for c in cells]
        cleaned_cells = [c for c in cleaned_cells if c]
        if not cleaned_cells:
            continue

        links = re.findall(r"href=[\"']([^\"']+)[\"']", row_html, flags=re.I)
        title = cleaned_cells[0]
        key = (title, "|".join(cleaned_cells[:4]))
        if key in seen:
            continue
        seen.add(key)

        projects.append(
            {
                "title": title,
                "cells": cleaned_cells,
                "links": links,
                "snippet": row_text[:300],
            }
        )

    # 2) Label-value fallback patterns often seen in non-tabular pages
    label_patterns = [
        re.compile(
            r"(?:案件名|件名|工事名|業務名|調達件名)\\s*(?:[:：]|</[^>]+>\\s*<[^>]+>)\\s*([^<\\r\\n]{4,200})",
            flags=re.I,
        ),
        re.compile(r"(?:公告番号|入札番号|案件番号)\\s*[:：]\\s*([^<\\r\\n]{1,100})", flags=re.I),
    ]

    fallback_hits = []
    for pat in label_patterns:
        fallback_hits.extend([strip_tags(x) for x in pat.findall(html)])

    for hit in fallback_hits:
        if not hit:
            continue
        key = (hit, "fallback")
        if key in seen:
            continue
        seen.add(key)
        projects.append({"title": hit, "cells": [hit], "links": [], "snippet": hit[:300]})

    return projects


def scrape_e_tokyo(scraper: LegacyScraper) -> Dict[str, object]:
    base = "https://www.e-tokyo.lg.jp/choutatu_ppij/ppij/pub"
    gov_codes = ["101-229"]

    pages = []

    pages.append(scraper.request(base, method="GET", encoding="cp932"))
    pages.append(scraper.request(f"{base}?s=P001&a=1", method="GET", encoding="cp932"))
    pages.append(scraper.request(f"{base}?s=P001&a=2", method="GET", encoding="cp932"))
    pages.append(
        scraper.request(
            base,
            method="POST",
            data={"s": "P002", "a": "1"},
            encoding="cp932",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    )

    search_results = []
    for gov_code in gov_codes:
        search = scraper.request(
            base,
            method="POST",
            data={"s": "P002", "a": "3", "govCode": gov_code, "maxDispRowCount": "100"},
            encoding="cp932",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        search_results.append(
            {
                "govCode": gov_code,
                "fetch": search.__dict__,
                "projects": extract_bid_projects(search.html),
            }
        )

    return {
        "system": "e-Tokyo",
        "base": base,
        "session_pages": [p.__dict__ for p in pages],
        "search_results": search_results,
    }


def scrape_supercals(scraper: LegacyScraper) -> Dict[str, object]:
    instances = [
        {"host": "ppi.cals-shiz.jp"},
        {"host": "chiba-ep-bis.supercals.jp"},
        {"host": "ep-bis.pref.niigata.jp"},
        {"host": "ppi.e-nagano.lg.jp"},
        {"host": "ep-bis.supercals.jp", "extra": {"KikanNO": "4700000"}},
    ]

    out_instances = []
    for inst in instances:
        host = inst["host"]
        base = f"https://{host}/ebidPPIPublish/EjPPIj"
        extra = inst.get("extra", {})

        s1 = scraper.request(base, method="GET", encoding="shift_jis")
        s2_payload = {"ejProcessName": "StartPage"}
        s2_payload.update(extra)
        s2 = scraper.request(
            base,
            method="POST",
            data=s2_payload,
            encoding="shift_jis",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        cond_url = f"{base}?ejProcessName=getCondPage"
        if "KikanNO" in extra:
            cond_url += "&KikanNO=4700000"
        s3 = scraper.request(cond_url, method="GET", encoding="shift_jis")

        find_payload = {
            "ejParameterID": "EjPSJ01",
            "ejProcessName": "findList",
            "Nendo": "2026",
        }
        find_payload.update(extra)

        s4 = scraper.request(
            base,
            method="POST",
            data=find_payload,
            encoding="shift_jis",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        out_instances.append(
            {
                "host": host,
                "extra": extra,
                "session": [s1.__dict__, s2.__dict__, s3.__dict__],
                "search": s4.__dict__,
                "projects": extract_bid_projects(s4.html),
            }
        )

    return {
        "system": "SuperCALS",
        "instances": out_instances,
    }


def scrape_cydeen_pan(scraper: LegacyScraper) -> Dict[str, object]:
    base = "https://www.nyusatsu.ebid-osaka.jp/pan"
    out_orgs = []

    for i in range(202, 231):
        kikan_no = f"{i:04d}"
        s1_url = f"{base}/PAN010.do?KIKAN_NO={kikan_no}"
        s1 = scraper.request(s1_url, method="GET", encoding="shift_jis")

        s2_url = (
            f"{base}/PAN030.do?KIKAN_NO={kikan_no}&SCREEN_ID=PAN030"
            "&PARAM=1&HYOJI_KENSU=100"
        )
        s2 = scraper.request(s2_url, method="GET", encoding="shift_jis")

        out_orgs.append(
            {
                "KIKAN_NO": kikan_no,
                "session": [s1.__dict__],
                "search": s2.__dict__,
                "projects": extract_bid_projects(s2.html),
            }
        )

    return {
        "system": "CYDEEN PAN",
        "base": base,
        "organizations": out_orgs,
    }


def scrape_kanagawa_dentyo(scraper: LegacyScraper) -> Dict[str, object]:
    menu_url = "https://nyusatsu-joho.e-kanagawa.lg.jp/DENTYO/GPPI_MENU"
    info_base = "https://nyusatsu-joho.e-kanagawa.lg.jp/DENTYO/P5000_INFORMATION"

    # Prompt specifies 0001-0402 (30 municipalities). If your target set is known,
    # replace this list with the exact 30 codes.
    dantai_codes = [f"{i:04d}" for i in range(1, 403)]

    menu = scraper.request(menu_url, method="GET", encoding="shift_jis")
    out_orgs = []

    for code in dantai_codes:
        url = f"{info_base}?hdn_dantai={code}"
        s = scraper.request(url, method="GET", encoding="shift_jis")
        out_orgs.append(
            {
                "hdn_dantai": code,
                "search": s.__dict__,
                "projects": extract_bid_projects(s.html),
            }
        )

    return {
        "system": "神奈川DENTYO",
        "menu": menu.__dict__,
        "organizations": out_orgs,
    }


def summarize_counts(results: Dict[str, object]) -> Dict[str, int]:
    counts: Dict[str, int] = {}

    e_tokyo_total = sum(
        len(x.get("projects", []))
        for x in results.get("e_tokyo", {}).get("search_results", [])
    )
    counts["e_tokyo_projects"] = e_tokyo_total

    super_total = sum(
        len(x.get("projects", []))
        for x in results.get("supercals", {}).get("instances", [])
    )
    counts["supercals_projects"] = super_total

    pan_total = sum(
        len(x.get("projects", []))
        for x in results.get("cydeen_pan", {}).get("organizations", [])
    )
    counts["cydeen_pan_projects"] = pan_total

    kanagawa_total = sum(
        len(x.get("projects", []))
        for x in results.get("kanagawa_dentyo", {}).get("organizations", [])
    )
    counts["kanagawa_dentyo_projects"] = kanagawa_total

    return counts


def main() -> None:
    scraper = LegacyScraper()

    results: Dict[str, object] = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "request_interval_sec": REQUEST_INTERVAL_SEC,
        "ssl_verification": False,
    }

    results["e_tokyo"] = scrape_e_tokyo(scraper)
    results["supercals"] = scrape_supercals(scraper)
    results["cydeen_pan"] = scrape_cydeen_pan(scraper)
    results["kanagawa_dentyo"] = scrape_kanagawa_dentyo(scraper)
    results["summary"] = summarize_counts(results)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Saved: {OUTPUT_JSON}")
    print(json.dumps(results["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
