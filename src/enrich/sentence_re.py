import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

import mwparserfromhell

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

ARTICLES_IN = DATA_DIR / "articles_raw_wikitext.filtered.jsonl"
NODES_IN = DATA_DIR / "network_nodes_full.enriched.json"
RELS_IN = DATA_DIR / "network_relationships_full.enriched.json"

NODES_OUT = DATA_DIR / "network_nodes_full.enriched.json"  # giữ nguyên node
RELS_OUT = DATA_DIR / "network_relationships_full.enriched.json"  # ghi đè bổ sung
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.enriched.csv"


# Rule-based phân loại quan hệ từ câu
def classify_relation(sentence: str) -> str:
    s = sentence.lower()
    if any(k in s for k in ["vợ", "phối ngẫu", "chính thất", "hoàng hậu", "phu nhân"]):
        return "PHỐI_NGẪU_VỚI"
    if any(k in s for k in ["con", "hậu duệ", "con trai", "con gái", "hoàng tử", "công chúa"]):
        return "LÀ_CHA_MẸ_CỦA"
    if any(k in s for k in ["kế vị", "kế nhiệm", "tiền nhiệm", "tiếp nối"]):
        return "KẾ_NHIỆM_LIÊN_QUAN"
    if any(k in s for k in ["đánh bại", "đối đầu", "giao chiến", "chiến đấu", "thắng", "thua", "đánh trận"]):
        return "ĐỐI_ĐẦU"
    if any(k in s for k in ["chỉ huy", "lãnh đạo", "dẫn dắt"]):
        return "CHỈ_HUY"
    if any(k in s for k in ["tham gia", "tham chiến", "tham dự"]):
        return "THAM_GIA_SỰ_KIỆN"
    if any(k in s for k in ["phục vụ", "dưới quyền", "trung thành với"]):
        return "PHỤC_VỤ"
    if any(k in s for k in ["bổ nhiệm", "phong", "cử làm"]):
        return "ĐƯỢC_BỔ_NHIỆM_BỞI"
    if any(k in s for k in ["sinh tại", "sinh ở", "quê tại", "quê ở"]):
        return "SINH_TẠI"
    if any(k in s for k in ["mất tại", "qua đời", "từ trần"]):
        return "MẤT_TẠI"
    return "NHẮC_ĐẾN_CÂU"


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


def split_sentences(text: str) -> List[str]:
    # đơn giản, đủ dùng cho rule
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]


def main():
    if not (ARTICLES_IN.exists() and NODES_IN.exists() and RELS_IN.exists()):
        print("❌ Thiếu file input (articles wikitext / nodes / rels).")
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
        wikitext = art.get("wikitext", "")
        if not src_title or not wikitext:
            continue

        # Parse wikitext -> plain text and wikilinks
        wikicode = mwparserfromhell.parse(wikitext)
        plain_text = wikicode.strip_code()
        sentences = split_sentences(plain_text)

        # Collect candidate targets by simple string match per sentence
        for sent in sentences:
            sent_low = sent.lower()
            matched_targets = [
                tgt_title for tgt_low, tgt_title in title_lookup.items()
                if tgt_title != src_title and tgt_low in sent_low and len(tgt_low) >= 4
            ]
            if not matched_targets:
                continue
            rel_type = classify_relation(sent)
            for tgt_title in matched_targets:
                key = (src_title, tgt_title, rel_type)
                if key in rel_set:
                    continue
                rel_set.add(key)
                rels.append({"source": src_title, "target": tgt_title, "type": rel_type})
                added += 1

    print(f"Đã thêm {added} cạnh ngữ nghĩa ở mức câu (rule-based).")
    save_rels_json(RELS_OUT, rels)
    save_rels_csv(RELS_CSV_OUT, rels)
    print("Đã lưu:")
    print(f"- {RELS_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

