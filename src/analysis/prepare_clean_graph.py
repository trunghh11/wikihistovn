import csv
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Mục tiêu: lọc nhiễu trước khi nạp Neo4j/RAG
# - Giữ các node có nhãn hữu ích (Nhân vật Lịch sử, Vua Nhà Nguyễn, Sự kiện) hoặc có infobox.
# - Lọc cạnh theo whitelist quan hệ ngữ nghĩa, bỏ NHẮC_ĐẾN_CÂU / LIÊN_KẾT_TỚI.
# - Loại node cô lập sau khi lọc cạnh.

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

NODES_IN = DATA_DIR / "network_nodes_full.enriched.json"
RELS_IN = DATA_DIR / "network_relationships_full.enriched.json"

NODES_OUT = DATA_DIR / "network_nodes_full.clean.json"
RELS_OUT = DATA_DIR / "network_relationships_full.clean.json"
NODES_CSV_OUT = DATA_DIR / "nodes_for_neo4j.clean.csv"
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.clean.csv"

KEEP_LABELS = {"Nhân vật Lịch sử", "Vua Nhà Nguyễn", "Sự kiện"}
KEEP_REL_TYPES = {
    "LÀ_CHA_MẸ_CỦA", "LÀ_CON_CỦA", "LÀ_CHA_CỦA",
    "TIỀN_NHIỆM_CỦA", "KẾ_NHIỆM_CỦA",
    "PHỐI_NGẪU_VỚI",
    "CHỈ_HUY", "ĐƯỢC_CHỈ_HUY_BỞI",
    "PHỤC_VỤ", "ĐƯỢC_BỔ_NHIỆM_BỞI",
    "THAM_GIA_SỰ_KIỆN", "THAM_GIA",
    "SINH_TẠI", "MẤT_TẠI",
    "ĐỐI_ĐẦU", "KÝ_HIỆP_ƯỚC_VỚI",
}
# Các type nhiễu sẽ bị loại: NHẮC_ĐẾN_CÂU, LIÊN_KẾT_TỚI, NHẮC_ĐẾN, v.v.


def load_json(path: Path):
    return json.load(open(path, "r", encoding="utf-8"))


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(path, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


def save_csv_nodes(nodes: List[Dict], path: Path):
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
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([":START_ID", ":END_ID", ":TYPE", "evidence"])
        for r in rels:
            writer.writerow([r.get("source", ""), r.get("target", ""), r.get("type", ""), r.get("evidence", "")])


def filter_nodes(nodes: List[Dict]) -> Dict[str, Dict]:
    kept = {}
    for n in nodes:
        title = n.get("title")
        if not title:
            continue
        label = n.get("label", "")
        infobox = n.get("infobox", {})
        if label in KEEP_LABELS or (isinstance(infobox, dict) and infobox):
            kept[title] = n
    return kept


def filter_rels(rels: List[Dict], valid_nodes: Set[str]) -> List[Dict]:
    out = []
    rel_set: Set[Tuple[str, str, str]] = set()
    for r in rels:
        src, tgt, typ = r.get("source"), r.get("target"), r.get("type")
        if not src or not tgt or not typ:
            continue
        if src not in valid_nodes or tgt not in valid_nodes:
            continue
        if typ not in KEEP_REL_TYPES:
            continue
        key = (src, tgt, typ)
        if key in rel_set:
            continue
        rel_set.add(key)
        out.append(r)
    return out


def drop_isolates(nodes: Dict[str, Dict], rels: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    degree: Dict[str, int] = {}
    for r in rels:
        degree[r["source"]] = degree.get(r["source"], 0) + 1
        degree[r["target"]] = degree.get(r["target"], 0) + 1
    kept_titles = {t for t in nodes if degree.get(t, 0) > 0}
    kept_nodes = [nodes[t] for t in kept_titles]
    kept_rels = [r for r in rels if r["source"] in kept_titles and r["target"] in kept_titles]
    return kept_nodes, kept_rels


def main():
    if not (NODES_IN.exists() and RELS_IN.exists()):
        print("❌ Thiếu file input enriched.")
        return

    print("Đang tải dữ liệu...")
    nodes = load_json(NODES_IN)
    rels = load_json(RELS_IN)
    print(f"- Nodes gốc: {len(nodes)}, Rels gốc: {len(rels)}")

    valid_nodes = filter_nodes(nodes)
    print(f"- Nodes sau lọc label/infobox: {len(valid_nodes)}")

    filtered_rels = filter_rels(rels, set(valid_nodes.keys()))
    print(f"- Rels sau lọc type: {len(filtered_rels)}")

    final_nodes, final_rels = drop_isolates(valid_nodes, filtered_rels)
    print(f"- Nodes sau bỏ cô lập: {len(final_nodes)}")
    print(f"- Rels cuối: {len(final_rels)}")

    save_json(NODES_OUT, final_nodes)
    save_json(RELS_OUT, final_rels)
    save_csv_nodes(final_nodes, NODES_CSV_OUT)
    save_csv_rels(final_rels, RELS_CSV_OUT)

    print("Đã lưu:")
    print(f"- {NODES_OUT}")
    print(f"- {RELS_OUT}")
    print(f"- {NODES_CSV_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

