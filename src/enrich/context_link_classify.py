import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Scan-Link-Filter-Classify (rule-based, không LLM):
# 1) Quét wikitext, tách câu
# 2) Tìm [[link]] trong câu
# 3) Lọc: chỉ giữ link tới node có nhãn quan tâm
# 4) Phân loại quan hệ dựa trên từ khóa trong câu (evidence)

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

ARTICLES_IN = DATA_DIR / "articles_raw_wikitext.filtered.jsonl"
NODES_IN = DATA_DIR / "network_nodes_full.enriched.json"
RELS_IN = DATA_DIR / "network_relationships_full.enriched.json"

RELS_OUT = DATA_DIR / "network_relationships_full.context.json"
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.context.csv"

# Nhãn node cần giữ
LABEL_WHITELIST = {
    "Nhân vật Lịch sử",
    "Vua Nhà Nguyễn",
    "Sự kiện",
    "Địa danh",
}

SENT_MIN_LEN = 20
SENT_MAX_LEN = 220  # cắt evidence quá dài
MAX_TARGETS_PER_SENT = 3
MAX_SENT_PER_ARTICLE = 200  # tránh bùng nổ câu

# Từ khóa -> type (mở rộng)
RULES = [
    (["vợ", "phối ngẫu", "chính thất", "hoàng hậu", "phu nhân", "chồng"], "PHỐI_NGẪU_VỚI"),
    (["cha", "thân phụ", "phụ thân", "mẹ", "thân mẫu", "mẫu thân"], "LÀ_CHA_MẸ_CỦA"),
    (["con", "hậu duệ", "con trai", "con gái", "hoàng tử", "công chúa"], "LÀ_CON_CỦA"),
    (["kế vị", "kế nhiệm", "tiền nhiệm", "tiếp nối", "lên ngôi", "thoái vị", "nhường ngôi"], "KẾ_NHIỆM_LIÊN_QUAN"),
    (["đánh bại", "đối đầu", "giao chiến", "trận", "chiến đấu", "thắng", "thua", "đánh trận", "khởi nghĩa", "nổi dậy"], "ĐỐI_ĐẦU"),
    (["chỉ huy", "lãnh đạo", "dẫn dắt", "tổng chỉ huy"], "CHỈ_HUY"),
    (["tham gia", "tham chiến", "tham dự", "tham gia trận", "tham gia khởi nghĩa"], "THAM_GIA_SỰ_KIỆN"),
    (["phục vụ", "dưới quyền", "trung thành với", "phò tá", "phụ tá", "thuộc hạ"], "PHỤC_VỤ"),
    (["bổ nhiệm", "phong", "cử làm", "thăng chức"], "ĐƯỢC_BỔ_NHIỆM_BỞI"),
    (["ký hiệp ước", "ký hòa ước", "ký hiệp định"], "KÝ_HIỆP_ƯỚC_VỚI"),
    (["sinh tại", "sinh ở", "quê tại", "quê ở"], "SINH_TẠI"),
    (["mất tại", "qua đời", "từ trần"], "MẤT_TẠI"),
]

DEFAULT_TYPE = "MENTIONED_IN"


def load_articles(path: Path) -> List[Dict]:
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                continue
    return items


def load_json(path: Path):
    return json.load(open(path, "r", encoding="utf-8"))


def save_rels_json(path: Path, rels: List[Dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    json.dump(rels, open(path, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


def save_rels_csv(path: Path, rels: List[Dict]):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([":START_ID", ":END_ID", ":TYPE", "evidence"])
        for r in rels:
            w.writerow([r.get("source", ""), r.get("target", ""), r.get("type", ""), r.get("evidence", "")])


def split_sentences_wikitext(wikitext: str) -> List[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", wikitext) if s.strip()]


def extract_links_in_sentence(sentence: str) -> List[str]:
    matches = re.findall(r"\[\[(.*?)(?:\|(.*?))?\]\]", sentence)
    targets = []
    for m in matches:
        target = m[0].strip()
        if not target:
            continue
        if ":" in target.lower():
            continue
        targets.append(target)
    return targets


def classify_sentence(sent: str) -> str:
    low = sent.lower()
    for kws, rel in RULES:
        if any(k in low for k in kws):
            return rel
    return DEFAULT_TYPE


def main():
    if not (ARTICLES_IN.exists() and NODES_IN.exists() and RELS_IN.exists()):
        print("❌ Thiếu file input (articles/nodes/rels).")
        return

    print("Đang tải dữ liệu...")
    articles = load_articles(ARTICLES_IN)
    nodes = load_json(NODES_IN)
    rels = load_json(RELS_IN)

    title_lookup: Dict[str, Tuple[str, str]] = {}
    for n in nodes:
        t = n.get("title")
        if t:
            title_lookup[t.lower()] = (t, n.get("label", ""))

    rel_set: Set[Tuple[str, str, str]] = {(r.get("source", ""), r.get("target", ""), r.get("type", "")) for r in rels}

    added = 0
    for art in articles:
        src = art.get("title", "")
        wikitext = art.get("wikitext", "")
        if not src or not wikitext:
            continue
        sentences = split_sentences_wikitext(wikitext)[:MAX_SENT_PER_ARTICLE]
        for sent in sentences:
            if len(sent) < SENT_MIN_LEN:
                continue
            if len(sent) > SENT_MAX_LEN:
                sent = sent[:SENT_MAX_LEN] + "..."
            targets = extract_links_in_sentence(sent)
            if not targets:
                continue
            targets = targets[:MAX_TARGETS_PER_SENT]
            rel_type = classify_sentence(sent)
            for tgt in targets:
                tgt_norm = tgt.lower()
                tgt_title = None
                tgt_label = None

                # ranh giới từ
                for norm, (t_full, lbl) in title_lookup.items():
                    if norm in tgt_norm or tgt_norm in norm:
                        # ưu tiên match exact lower
                        if tgt_norm == norm or re.search(rf"\\b{re.escape(norm)}\\b", sent.lower()):
                            tgt_title, tgt_label = t_full, lbl
                            break

                if not tgt_title:
                    continue
                # Lọc nhãn quan tâm
                if tgt_label not in LABEL_WHITELIST:
                    continue
                if tgt_title == src:
                    continue
                key = (src, tgt_title, rel_type)
                if key in rel_set:
                    continue
                rel_set.add(key)
                rels.append({
                    "source": src,
                    "target": tgt_title,
                    "type": rel_type,
                    "evidence": sent.strip()
                })
                added += 1

    print(f"Đã thêm {added} cạnh (scan-link-filter-classify, rule-based).")
    save_rels_json(RELS_OUT, rels)
    save_rels_csv(RELS_CSV_OUT, rels)
    print("Đã lưu:")
    print(f"- {RELS_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

