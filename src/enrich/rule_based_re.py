import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

ARTICLES_IN = DATA_DIR / "articles_raw.filtered.jsonl"
NODES_IN = DATA_DIR / "network_nodes_full.enriched.json"
RELS_IN = DATA_DIR / "network_relationships_full.enriched.json"

NODES_OUT = DATA_DIR / "network_nodes_full.enriched.json"  # giữ nguyên node
RELS_OUT = DATA_DIR / "network_relationships_full.enriched.json"  # ghi đè có thêm RE mới
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.enriched.csv"

# Keyword -> relation type (source = bài viết, target = thực thể được nhắc)
PATTERNS = {
    "kế vị": "KẾ_NHIỆM_CỦA",
    "kế nhiệm": "KẾ_NHIỆM_CỦA",
    "tiền nhiệm": "TIỀN_NHIỆM_CỦA",
    "tiếp nối": "KẾ_NHIỆM_CỦA",
    "kế tiếp": "KẾ_NHIỆM_CỦA",
    "chỉ huy": "CHỈ_HUY",
    "lãnh đạo": "CHỈ_HUY",
    "dẫn dắt": "CHỈ_HUY",
    "tham gia": "THAM_GIA_SỰ_KIỆN",
    "tham chiến": "THAM_GIA_SỰ_KIỆN",
    "phục vụ": "PHỤC_VỤ",
    "bổ nhiệm": "ĐƯỢC_BỔ_NHIỆM_BỞI",
    "ký hiệp ước": "KÝ_HIỆP_ƯỚC_VỚI",
    "ký hòa ước": "KÝ_HIỆP_ƯỚC_VỚI",
}


def load_articles(path: Path) -> List[Dict]:
    articles = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                articles.append(json.loads(line))
            except Exception:
                continue
    return articles


def load_json(path: Path):
    return json.load(open(path, "r", encoding="utf-8"))


def build_rel_set(rels: List[Dict]) -> Set[Tuple[str, str, str]]:
    s = set()
    for r in rels:
        s.add((r.get("source", ""), r.get("target", ""), r.get("type", "")))
    return s


def save_rels_json(path: Path, rels: List[Dict]):
    json.dump(rels, open(path, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


def save_rels_csv(path: Path, rels: List[Dict]):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([":START_ID", ":END_ID", ":TYPE"])
        for r in rels:
            w.writerow([r.get("source", ""), r.get("target", ""), r.get("type", "")])


def main():
    if not (ARTICLES_IN.exists() and NODES_IN.exists() and RELS_IN.exists()):
        print("❌ Thiếu file input (articles/nodes/rels).")
        return

    print("Đang tải dữ liệu...")
    articles = load_articles(ARTICLES_IN)
    nodes = load_json(NODES_IN)
    rels = load_json(RELS_IN)

    title_lookup: Dict[str, str] = {}
    for n in nodes:
        t = n.get("title")
        if t:
            title_lookup[t.lower()] = t

    rel_set = build_rel_set(rels)

    added = 0
    for art in articles:
        src_title = art.get("title", "")
        text = art.get("text", "")
        if not src_title or not text:
            continue
        text_low = text.lower()
        # Với mỗi pattern, nếu keyword và target title cùng xuất hiện, tạo quan hệ
        for kw, rel_type in PATTERNS.items():
            if kw not in text_low:
                continue
            for tgt_low, tgt_title in title_lookup.items():
                if tgt_title == src_title:
                    continue
                if tgt_low in text_low:
                    key = (src_title, tgt_title, rel_type)
                    if key in rel_set:
                        continue
                    rel_set.add(key)
                    rels.append({"source": src_title, "target": tgt_title, "type": rel_type})
                    added += 1

    print(f"Đã thêm {added} cạnh ngữ nghĩa (rule-based).")
    save_rels_json(RELS_OUT, rels)
    save_rels_csv(RELS_CSV_OUT, rels)
    print("Đã lưu:")
    print(f"- {RELS_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

