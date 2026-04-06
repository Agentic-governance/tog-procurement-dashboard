#!/usr/bin/env python3
"""Re-classify procurement items with expanded keyword rules."""
import sqlite3

DB_PATH = "/app/data/monitor.db"

CATEGORY_RULES = [
    ("construction", [
        "工事", "修繕", "改修", "建設", "舗装", "設置工", "撤去工", "解体",
        "建築", "塗装", "防水", "耐震", "配管", "建替", "増築", "改築",
        "造成", "掘削", "基礎", "仮設", "鉄骨", "外壁", "屋根", "内装",
        "タイル", "左官", "電気工事", "空調工事", "給排水", "下水道工事",
        "道路", "橋梁", "トンネル", "堤防", "護岸", "砂防", "河川",
    ]),
    ("it", [
        "システム", "ソフトウェア", "ネットワーク", "サーバ", "データ",
        "ICT", "DX", "AI", "クラウド", "セキュリティ", "LAN",
        "IP伝送", "電子計算", "情報処理", "プログラム", "アプリ",
        "デジタル", "ウェブ", "ホームページ", "サイバー", "Wi-Fi",
    ]),
    ("consulting", [
        "設計", "測量", "調査", "コンサル", "計画策定", "検討",
        "鑑定", "評価", "審査", "分析", "診断", "策定",
        "アセスメント", "モニタリング", "アドバイザリー",
    ]),
    ("service", [
        "業務委託", "委託", "役務", "保守", "点検", "清掃",
        "管理業務", "運営", "警備", "派遣", "人材",
        "除草", "草刈", "除排雪", "除雪", "排雪",
        "廃棄物", "処分", "処理", "汚泥", "収集",
        "運搬", "輸送", "運送", "配達", "配送",
        "印刷", "封入", "製本", "刊行",
        "検査", "試験", "健診", "検診",
        "支援", "補助業務", "事務", "相談",
        "広報", "周知", "啓発",
        "整備事業", "保育間伐", "造林事業", "植付", "下刈",
        "地拵", "間伐", "除伐", "森林整備", "森林環境",
        "借上", "賃借", "使用許可",
        "講習", "研修", "セミナー", "教育",
        "単価契約", "請負", "業務",
    ]),
    ("goods", [
        "購入", "調達", "納入", "物品", "備品", "機器",
        "車両", "薬品", "リース", "賃貸借",
        "買入", "買入れ", "売払", "交換契約",
        "燃料", "重油", "ガス", "灯油", "軽油", "電気",
        "食品", "食料", "食材", "給食",
        "紙", "トナー", "文具", "消耗品",
        "医薬品", "医療", "ワクチン",
        "被服", "制服", "靴",
        "自動車", "巡視艇", "船舶",
        "砕石", "資材", "材料",
        "供給", "供給契約",
    ]),
]

def classify(title):
    if not title:
        return "other"
    for key, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in title:
                return key
    return "other"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    
    rows = conn.execute("SELECT rowid, title FROM procurement_items WHERE category = 'other'").fetchall()
    print(f"Items to reclassify: {len(rows)}", flush=True)
    
    updates = {}
    batch = []
    for rowid, title in rows:
        new_cat = classify(title)
        if new_cat != "other":
            batch.append((new_cat, rowid))
            updates[new_cat] = updates.get(new_cat, 0) + 1
    
    print(f"Will reclassify: {len(batch)}", flush=True)
    for cat, count in sorted(updates.items(), key=lambda x: -x[1]):
        print(f"  {cat}: +{count}", flush=True)
    
    conn.executemany("UPDATE procurement_items SET category = ? WHERE rowid = ?", batch)
    conn.commit()
    
    rows = conn.execute("SELECT category, COUNT(*) FROM procurement_items GROUP BY category ORDER BY COUNT(*) DESC").fetchall()
    total = sum(r[1] for r in rows)
    print(f"\nFinal distribution:", flush=True)
    for cat, count in rows:
        pct = count/total*100
        print(f"  {cat or chr(45):20s} {count:>8,} ({pct:.1f}%)", flush=True)
    print(f"  TOTAL{chr(32)*14} {total:>8,}", flush=True)
    
    remaining_other = next((r[1] for r in rows if r[0] == "other"), 0)
    print(f"\nOther reduced: 44,090 -> {remaining_other} ({remaining_other/total*100:.1f}%)", flush=True)
    
    conn.close()

if __name__ == "__main__":
    main()
