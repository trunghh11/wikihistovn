import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Refine context relationships:
# - Drop generic/unclear types (MENTIONED_IN, NHẮC_ĐẾN*, LIÊN_KẾT_TỚI)
# - Resolve conflicts per pair by priority
# - Deduplicate; handle undirected PHỐI_NGẪU_VỚI
# - Keep only nodes with meaningful labels (optional)

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

NODES_IN = DATA_DIR / "network_nodes_full.enriched.json"
RELS_IN = DATA_DIR / "network_relationships_full.context.cleaned.json"

RELS_OUT = DATA_DIR / "network_relationships_full.context.refined.json"
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.context.refined.csv"

DROP_TYPES = {
    "MENTIONED_IN",
    "NHẮC_ĐẾN",
    "NHẮC_ĐẾN_CÂU",
    "LIÊN_KẾT_TỚI",
}

# Priority: lower index = higher priority
TYPE_PRIORITY = [
    "PHỐI_NGẪU_VỚI",
    "KẾ_NHIỆM_CỦA",
    "TIỀN_NHIỆM_CỦA",
    "KẾ_NHIỆM_LIÊN_QUAN",
    "LÀ_CHA_MẸ_CỦA",
    "LÀ_CON_CỦA",
    "ĐỐI_ĐẦU",
    "CHỈ_HUY",
    "ĐƯỢC_BỔ_NHIỆM_BỞI",
    "PHỤC_VỤ",
    "THAM_GIA_SỰ_KIỆN",
    "SINH_TẠI",
    "MẤT_TẠI",
]
PRIORITY_MAP = {t: i for i, t in enumerate(TYPE_PRIORITY)}

LABEL_WHITELIST = {
    "Nhân vật Lịch sử",
    "Vua Nhà Nguyễn",
    "Sự kiện",
}


def load_json(path: Path):
    return json.load(open(path, "r", encoding="utf-8"))


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(path, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


def save_csv(path: Path, rels: List[Dict]):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([":START_ID", ":END_ID", ":TYPE", "evidence"])
        for r in rels:
            w.writerow([r.get("source", ""), r.get("target", ""), r.get("type", ""), r.get("evidence", "")])


def main():
    if not (NODES_IN.exists() and RELS_IN.exists()):
        print("❌ Thiếu file input.")
        return

    nodes = load_json(NODES_IN)
    rels = load_json(RELS_IN)

    # label lookup
    label_map: Dict[str, str] = {}
    for n in nodes:
        t = n.get("title")
        if t:
            label_map[t] = n.get("label", "")

    kept_nodes: Set[str] = set()
    best_rel: Dict[Tuple[str, str], Dict] = {}

    for r in rels:
        src = r.get("source", "")
        tgt = r.get("target", "")
        typ = r.get("type", "")
        if not src or not tgt:
            continue
        if src == tgt:
            continue
        if typ in DROP_TYPES:
            continue

        # label filter
        if label_map.get(src, "") not in LABEL_WHITELIST or label_map.get(tgt, "") not in LABEL_WHITELIST:
            continue

        # undirected key for PHỐI_NGẪU_VỚI
        if typ == "PHỐI_NGẪU_VỚI":
            key_pair = tuple(sorted([src, tgt]))
            key = (key_pair[0], key_pair[1])
        else:
            key = (src, tgt)

        prio = PRIORITY_MAP.get(typ, len(TYPE_PRIORITY))
        existing = best_rel.get(key)

        # keep better priority
        if existing:
            old_prio = PRIORITY_MAP.get(existing["type"], len(TYPE_PRIORITY))
            if prio < old_prio:
                best_rel[key] = {"source": src, "target": tgt, "type": typ, "evidence": r.get("evidence", "")}
        else:
            best_rel[key] = {"source": src, "target": tgt, "type": typ, "evidence": r.get("evidence", "")}

        kept_nodes.add(src)
        kept_nodes.add(tgt)

    cleaned = list(best_rel.values())
    print(f"Input rels: {len(rels)}")
    print(f"Output rels: {len(cleaned)}")

    save_json(RELS_OUT, cleaned)
    save_csv(RELS_CSV_OUT, cleaned)
    print("Đã lưu:")
    print(f"- {RELS_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

