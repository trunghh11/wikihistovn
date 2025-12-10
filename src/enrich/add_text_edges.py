import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Thêm cạnh "NHẮC_ĐẾN" từ văn bản bài viết tới các node đã có, dựa trên so khớp tiêu đề.
# Yêu cầu: đã có các file filtered và articles_raw.filtered.jsonl.

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

NODES_IN = DATA_DIR / "network_nodes_full.filtered.json"
RELS_IN = DATA_DIR / "network_relationships_full.filtered.json"
ARTICLES_IN = DATA_DIR / "articles_raw.filtered.jsonl"

NODES_OUT = DATA_DIR / "network_nodes_full.enriched.json"
RELS_OUT = DATA_DIR / "network_relationships_full.enriched.json"

NODES_CSV_OUT = DATA_DIR / "nodes_for_neo4j.enriched.csv"
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.enriched.csv"


def load_json(path: Path):
    return json.load(open(path, "r", encoding="utf-8"))


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


def build_rel_set(rels: List[Dict]) -> Set[Tuple[str, str, str]]:
    s = set()
    for r in rels:
        s.add((r.get("source", ""), r.get("target", ""), r.get("type", "")))
    return s


def add_relationship(source: str, target: str, rel_type: str, rels: List[Dict], rel_set: Set[Tuple[str, str, str]]):
    key = (source, target, rel_type)
    if key in rel_set:
        return
    rel_set.add(key)
    rels.append({"source": source, "target": target, "type": rel_type})


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(path, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


def save_csv_nodes(nodes: List[Dict], path: Path):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
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


def save_csv_rels(rels: List[Dict], path: Path):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([":START_ID", ":END_ID", ":TYPE"])
        for r in rels:
            writer.writerow([r.get("source", ""), r.get("target", ""), r.get("type", "")])


def main():
    if not (NODES_IN.exists() and RELS_IN.exists() and ARTICLES_IN.exists()):
        print("❌ Thiếu file input. Yêu cầu các file filtered và articles_raw.filtered.jsonl")
        return

    print("Đang tải nodes, relationships, articles...")
    nodes = load_json(NODES_IN)
    rels = load_json(RELS_IN)
    articles = load_articles(ARTICLES_IN)

    title_to_title = {}
    for n in nodes:
        t = n.get("title")
        if t:
            title_to_title[t.lower()] = t

    rel_set = build_rel_set(rels)

    added = 0
    for art in articles:
        src_title = art.get("title", "")
        text = (art.get("text") or "").lower()
        if not src_title or not text:
            continue
        src_title_lower = src_title.lower()

        for tgt_lower, tgt_title in title_to_title.items():
            if tgt_lower == src_title_lower:
                continue
            # bỏ qua target quá ngắn/dễ nhiễu
            if len(tgt_lower) < 4:
                continue
            if tgt_lower in text:
                add_relationship(src_title, tgt_title, "NHẮC_ĐẾN", rels, rel_set)
                added += 1
        # giảm tải: text chỉ 1 lần per article, vòng lặp hoàn tất

    print(f"Đã thêm {added} cạnh NHẮC_ĐẾN từ văn bản.")

    # Lưu JSON enriched
    save_json(NODES_OUT, nodes)
    save_json(RELS_OUT, rels)

    # Lưu CSV enriched
    save_csv_nodes(nodes, NODES_CSV_OUT)
    save_csv_rels(rels, RELS_CSV_OUT)

    print("Hoàn tất. Output:")
    print(f"- {NODES_OUT}")
    print(f"- {RELS_OUT}")
    print(f"- {NODES_CSV_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

