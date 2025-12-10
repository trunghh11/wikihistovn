import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

"""
Làm sạch file context:
- Bỏ self-loop.
- Ưu tiên quan hệ ngữ nghĩa: nếu (source,target) đã có type khác MENTIONED_IN,
  thì bỏ MENTIONED_IN trùng cặp đó.
- Khử trùng lặp theo (source,target,type); giữ 1 evidence đầu tiên.
- Hạn chế số cạnh MENTIONED_IN lặp lại trên cùng cặp xuống 1.
"""

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

RELS_IN = DATA_DIR / "network_relationships_full.context.json"
RELS_OUT = DATA_DIR / "network_relationships_full.context.cleaned.json"
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.context.cleaned.csv"


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
    if not RELS_IN.exists():
        print(f"❌ Không tìm thấy file {RELS_IN}")
        return

    rels = load_json(RELS_IN)
    print(f"Input relationships: {len(rels)}")

    # Thu thập cặp có quan hệ mạnh (khác MENTIONED_IN)
    strong_pairs: Set[Tuple[str, str]] = set()
    for r in rels:
        src = r.get("source", "")
        tgt = r.get("target", "")
        typ = r.get("type", "")
        if not src or not tgt or src == tgt:
            continue
        if typ != "MENTIONED_IN":
            strong_pairs.add((src, tgt))

    seen: Set[Tuple[str, str, str]] = set()
    mentioned_seen: Set[Tuple[str, str]] = set()
    cleaned: List[Dict] = []

    for r in rels:
        src = r.get("source", "")
        tgt = r.get("target", "")
        typ = r.get("type", "")
        if not src or not tgt:
            continue
        if src == tgt:
            continue

        # Nếu đã có quan hệ mạnh, bỏ MENTIONED_IN cho cặp đó
        if typ == "MENTIONED_IN" and (src, tgt) in strong_pairs:
            continue

        key = (src, tgt, typ)
        if key in seen:
            continue
        seen.add(key)

        # Giới hạn 1 cạnh MENTIONED_IN trên mỗi cặp
        if typ == "MENTIONED_IN":
            mkey = (src, tgt)
            if mkey in mentioned_seen:
                continue
            mentioned_seen.add(mkey)

        cleaned.append(r)

    print(f"Output relationships: {len(cleaned)}")
    save_json(RELS_OUT, cleaned)
    save_csv(RELS_CSV_OUT, cleaned)
    print("Saved:")
    print(f"- {RELS_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

