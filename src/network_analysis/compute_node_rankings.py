import json
import os
from collections import Counter
from typing import Dict, List, Tuple

from src.common.config_paths import DATA_PROCESSED


# === CẤU HÌNH ĐƯỜNG DẪN ===

NODES_IN = os.path.join(DATA_PROCESSED, "network_nodes_enriched.json")
RELS_BASE_IN = os.path.join(DATA_PROCESSED, "network_relationships_full.json")
RELS_TEXT_IN = os.path.join(DATA_PROCESSED, "network_relationships_text_based.json")
NODES_OUT = os.path.join(DATA_PROCESSED, "network_nodes_ranked.json")


def load_nodes(path: str) -> Dict[str, Dict]:
    """Đọc nodes và trả về map title -> node."""
    print(f"Đang đọc nodes từ: {path}")
    with open(path, "r", encoding="utf-8") as f:
        nodes = json.load(f)
    if not isinstance(nodes, list):
        raise ValueError("File nodes phải chứa một list.")
    title_to_node: Dict[str, Dict] = {}
    for n in nodes:
        title = n.get("title")
        if title:
            title_to_node[title] = n
    print(f"  > Số node có title hợp lệ: {len(title_to_node)}")
    return title_to_node


def load_relationships(path: str) -> List[Dict]:
    """Đọc danh sách cạnh (source, target, type)."""
    print(f"Đang đọc relationships từ: {path}")
    with open(path, "r", encoding="utf-8") as f:
        rels = json.load(f)
    if not isinstance(rels, list):
        raise ValueError("File relationships phải chứa một list.")
    print(f"  > Số cạnh: {len(rels)}")
    return rels


def compute_degrees(
    rels: List[Dict],
) -> Tuple[Counter, Counter, Counter]:
    """
    Tính degree đơn giản:
      - out_degree[title]
      - in_degree[title]
      - total_degree[title]
    """
    out_deg: Counter = Counter()
    in_deg: Counter = Counter()

    for r in rels:
        s = r.get("source")
        t = r.get("target")
        if s:
            out_deg[s] += 1
        if t:
            in_deg[t] += 1

    total_deg: Counter = Counter()
    for k, v in out_deg.items():
        total_deg[k] += v
    for k, v in in_deg.items():
        total_deg[k] += v

    return out_deg, in_deg, total_deg


def attach_rankings() -> None:
    nodes = load_nodes(NODES_IN)
    base_rels = load_relationships(RELS_BASE_IN)
    text_rels = load_relationships(RELS_TEXT_IN)

    # Degrees cho mạng gốc
    base_out, base_in, base_total = compute_degrees(base_rels)
    # Degrees cho mạng từ văn bản
    text_out, text_in, text_total = compute_degrees(text_rels)

    # Kết hợp
    combined_total: Counter = base_total.copy()
    for k, v in text_total.items():
        combined_total[k] += v

    # Tính ranking: sort theo degree giảm dần
    def build_rank(counter: Counter) -> Dict[str, int]:
        sorted_titles = [t for t, _ in counter.most_common()]
        return {title: rank + 1 for rank, title in enumerate(sorted_titles)}

    base_rank = build_rank(base_total)
    text_rank = build_rank(text_total)
    combined_rank = build_rank(combined_total)

    # Gắn vào node
    updated_nodes: List[Dict] = []
    for title, node in nodes.items():
        node["base_out_degree"] = int(base_out.get(title, 0))
        node["base_in_degree"] = int(base_in.get(title, 0))
        node["base_total_degree"] = int(base_total.get(title, 0))
        node["text_out_degree"] = int(text_out.get(title, 0))
        node["text_in_degree"] = int(text_in.get(title, 0))
        node["text_total_degree"] = int(text_total.get(title, 0))
        node["combined_total_degree"] = int(combined_total.get(title, 0))

        node["base_rank"] = int(base_rank.get(title, 0))
        node["text_rank"] = int(text_rank.get(title, 0))
        node["combined_rank"] = int(combined_rank.get(title, 0))

        updated_nodes.append(node)

    # Ghi ra file mới
    os.makedirs(os.path.dirname(NODES_OUT), exist_ok=True)
    with open(NODES_OUT, "w", encoding="utf-8") as f:
        json.dump(updated_nodes, f, ensure_ascii=False, indent=4)

    print(f"✅ Đã gắn degree & rank cho {len(updated_nodes)} node.")
    print(f"   File output: {NODES_OUT}")

    # In thử top 10 node theo combined_rank
    print("\nTop 10 node theo combined_total_degree:")
    top10 = sorted(updated_nodes, key=lambda n: n.get("combined_rank", 0) or 10**9)[:10]
    for n in top10:
        print(
            f"  - {n.get('title')} | degree={n.get('combined_total_degree')} | rank={n.get('combined_rank')}"
        )


if __name__ == "__main__":
    print("--- 🚀 Tính degree & xếp hạng node dựa trên mạng quan hệ ---")
    try:
        attach_rankings()
        print("\n--- Hoàn tất compute_node_rankings ---")
    except FileNotFoundError as e:
        print(f"❌ Thiếu file đầu vào: {e}")
        print("   Cần có network_nodes_enriched.json, network_relationships_full.json, network_relationships_text_based.json.")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi: {e}")


