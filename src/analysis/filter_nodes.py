import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Set

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

NODES_JSON_IN = DATA_DIR / "network_nodes_full.json"
RELS_JSON_IN = DATA_DIR / "network_relationships_full.json"

NODES_JSON_OUT = DATA_DIR / "network_nodes_full.filtered.json"
RELS_JSON_OUT = DATA_DIR / "network_relationships_full.filtered.json"

NODES_CSV_OUT = DATA_DIR / "nodes_for_neo4j.filtered.csv"
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.filtered.csv"


COMMON_TERMS = {
    'việt nam', 'pháp', 'nhà nguyễn', 'lịch sử việt nam', 'đại nam',
    'tiếng việt', 'hán tự', 'quốc ngữ', 'việt sử thông giám cương mục',
    'trung quốc', 'nhật bản', 'đông dương', 'châu âu', 'châu á',
    'thế chiến', 'chiến tranh thế giới', 'cách mạng', 'độc lập',
    'văn hóa', 'xã hội', 'kinh tế', 'chính trị', 'tôn giáo',
    'phật giáo', 'nho giáo', 'đạo giáo', 'công giáo', 'hồi giáo',
    'địa lý', 'lịch sử', 'nhân vật', 'sự kiện', 'triều đại',
    'hoàng đế', 'vua', 'quan lại', 'tướng lĩnh', 'binh lính',
    'học giả', 'nhà thơ', 'nhà văn', 'nhà sử học', 'nhà khoa học',
    'địa danh', 'sông', 'núi', 'biển', 'đảo', 'thành phố', 'làng',
    'huyện', 'tỉnh', 'quốc gia', 'đế quốc', 'cộng hòa', 'vương quốc',
    'chính phủ', 'quân đội', 'đảng', 'tổ chức', 'hội đồng', 'ủy ban'
}

PREFIXES = (
    "tập tin:", "thể loại:", "bản mẫu:", "wikipedia:", "chủ đề:"
)


def is_noise(title: str) -> bool:
    """Heuristics to drop non-useful nodes for Nguyễn dynasty network."""
    if not title:
        return True
    t = title.strip().lower()

    # Years or simple dates
    if re.fullmatch(r"\d{3,4}", t):
        return True
    if re.fullmatch(r"\d{1,2}\s*tháng\s*\d{1,2}", t):
        return True
    if re.fullmatch(r"tháng\s*\d{1,2}", t):
        return True

    # Namespace / system pages
    if t.startswith(PREFIXES):
        return True

    # Broad/common terms
    if t in COMMON_TERMS:
        return True

    return False


def load_data():
    nodes = json.load(open(NODES_JSON_IN, "r", encoding="utf-8"))
    rels = json.load(open(RELS_JSON_IN, "r", encoding="utf-8"))
    return nodes, rels


def filter_nodes_and_rels(nodes: List[Dict], rels: List[Dict]):
    # Initial keep-set by title
    valid_nodes: Dict[str, Dict] = {}
    for node in nodes:
        title = node.get("title")
        if title and not is_noise(title):
            valid_nodes[title] = node

    # Filter relationships to current valid nodes
    filtered_rels = [
        r for r in rels
        if r.get("source") in valid_nodes and r.get("target") in valid_nodes
    ]

    # Compute degree to drop isolated nodes
    degree: Dict[str, int] = {}
    for r in filtered_rels:
        degree[r["source"]] = degree.get(r["source"], 0) + 1
        degree[r["target"]] = degree.get(r["target"], 0) + 1

    final_nodes = [
        n for title, n in valid_nodes.items()
        if degree.get(title, 0) > 0
    ]

    # Final rels filtered again (in case some nodes became isolated)
    final_titles: Set[str] = {n["title"] for n in final_nodes}
    final_rels = [
        r for r in filtered_rels
        if r["source"] in final_titles and r["target"] in final_titles
    ]

    return final_nodes, final_rels


def save_json(nodes: List[Dict], rels: List[Dict]):
    NODES_JSON_OUT.parent.mkdir(parents=True, exist_ok=True)
    json.dump(nodes, open(NODES_JSON_OUT, "w", encoding="utf-8"), indent=4, ensure_ascii=False)
    json.dump(rels, open(RELS_JSON_OUT, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


def save_csv(nodes: List[Dict], rels: List[Dict]):
    NODES_CSV_OUT.parent.mkdir(parents=True, exist_ok=True)

    # Nodes CSV
    with open(NODES_CSV_OUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["title:ID", ":LABEL", "page_id", "infobox"])
        for n in nodes:
            title = n.get("title", "")
            label = n.get("label", "")
            page_id = int(n.get("page_id", 0) or 0)
            infobox = n.get("infobox", {})
            if isinstance(infobox, dict):
                infobox_str = json.dumps(infobox, ensure_ascii=False)
            else:
                infobox_str = "{}"
            writer.writerow([title, label, page_id, infobox_str])

    # Relationships CSV
    with open(RELS_CSV_OUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([":START_ID", ":END_ID", ":TYPE"])
        for r in rels:
            writer.writerow([r.get("source", ""), r.get("target", ""), r.get("type", "")])


def main():
    print("Đang đọc dữ liệu gốc...")
    nodes, rels = load_data()
    print(f"- Nodes gốc: {len(nodes)}")
    print(f"- Rels gốc:  {len(rels)}")

    print("Đang lọc nhiễu...")
    final_nodes, final_rels = filter_nodes_and_rels(nodes, rels)
    print(f"- Nodes sau lọc: {len(final_nodes)}")
    print(f"- Rels sau lọc:  {len(final_rels)}")

    print("Đang lưu JSON filtered...")
    save_json(final_nodes, final_rels)

    print("Đang lưu CSV filtered...")
    save_csv(final_nodes, final_rels)

    print("Hoàn tất. File đầu ra:")
    print(f"- {NODES_JSON_OUT}")
    print(f"- {RELS_JSON_OUT}")
    print(f"- {NODES_CSV_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

