import json
import os
import random
from collections import Counter, defaultdict
from typing import Dict, List, Set

from src.common.config_paths import DATA_PROCESSED


# === CẤU HÌNH ĐƯỜNG DẪN ===

NODES_RANKED_IN = os.path.join(DATA_PROCESSED, "network_nodes_ranked.json")
NODES_FALLBACK_IN = os.path.join(DATA_PROCESSED, "network_nodes_enriched.json")

RELS_BASE_IN = os.path.join(DATA_PROCESSED, "network_relationships_full.json")
RELS_TEXT_IN = os.path.join(DATA_PROCESSED, "network_relationships_text_based.json")

NODES_OUT = os.path.join(DATA_PROCESSED, "network_nodes_ranked.json")


def load_nodes() -> Dict[str, Dict]:
    """
    Đọc nodes, ưu tiên file ranked (đã có degree, pagerank), fallback sang enriched.
    Trả về map title -> node.
    """
    path = NODES_RANKED_IN if os.path.exists(NODES_RANKED_IN) else NODES_FALLBACK_IN
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


def load_relationships(*paths: str) -> List[Dict]:
    """Đọc và gộp nhiều file relationships."""
    all_rels: List[Dict] = []
    for p in paths:
        print(f"Đang đọc relationships từ: {p}")
        with open(p, "r", encoding="utf-8") as f:
            rels = json.load(f)
        if not isinstance(rels, list):
            raise ValueError(f"File {p} phải chứa một list.")
        print(f"  > Số cạnh trong file: {len(rels)}")
        all_rels.extend(rels)
    print(f"  > Tổng số cạnh sau khi gộp: {len(all_rels)}")
    return all_rels


def build_undirected_graph(titles: Set[str], rels: List[Dict]) -> Dict[str, Set[str]]:
    """
    Xây đồ thị vô hướng: adjacency list node -> set(neighbors).
    Chỉ giữ cạnh giữa các node có title hợp lệ.
    """
    adj: Dict[str, Set[str]] = defaultdict(set)
    kept = 0
    for r in rels:
        s = r.get("source")
        t = r.get("target")
        if not s or not t:
            continue
        if s not in titles or t not in titles:
            continue
        if s == t:
            continue
        adj[s].add(t)
        adj[t].add(s)
        kept += 1

    print(f"  > Số cạnh vô hướng sau khi lọc: {kept}")
    print(f"  > Số node thực sự có ít nhất một cạnh: {len(adj)}")
    return adj


def label_propagation_communities(
    adj: Dict[str, Set[str]],
    max_iters: int = 50,
) -> Dict[str, str]:
    """
    Thuật toán Label Propagation đơn giản để phát hiện cộng đồng.
    Trả về: label_per_node (node_title -> community_label).
    """
    nodes = list(adj.keys())
    if not nodes:
        return {}

    # Khởi tạo: nhãn ban đầu = chính node
    labels: Dict[str, str] = {node: node for node in nodes}

    for it in range(max_iters):
        changes = 0
        # Duyệt node theo thứ tự random để tránh bias
        random.shuffle(nodes)

        for node in nodes:
            neighbors = adj[node]
            if not neighbors:
                continue

            # Lấy nhãn phổ biến nhất trong hàng xóm
            neighbor_labels = [labels[n] for n in neighbors]
            if not neighbor_labels:
                continue

            counter = Counter(neighbor_labels)
            # Nếu hòa, chọn nhãn nhỏ nhất (ổn định)
            max_count = max(counter.values())
            candidate_labels = [lab for lab, c in counter.items() if c == max_count]
            new_label = min(candidate_labels)

            if labels[node] != new_label:
                labels[node] = new_label
                changes += 1

        print(f"  > Iter {it+1}: số node đổi nhãn = {changes}")
        if changes == 0:
            print("  > Hội tụ sớm (không còn node nào đổi nhãn).")
            break

    return labels


def detect_communities() -> None:
    title_to_node = load_nodes()
    titles = set(title_to_node.keys())

    rels = load_relationships(RELS_BASE_IN, RELS_TEXT_IN)
    adj = build_undirected_graph(titles, rels)

    if not adj:
        print("❌ Đồ thị rỗng sau khi xây adjacency.")
        return

    print("\n--- Bắt đầu Label Propagation để phát hiện cộng đồng ---")
    labels = label_propagation_communities(adj)
    if not labels:
        print("❌ Không có nhãn cộng đồng nào được gán.")
        return

    # Gom node theo nhãn
    communities: Dict[str, List[str]] = defaultdict(list)
    for node, lab in labels.items():
        communities[lab].append(node)

    # Sắp xếp cộng đồng theo kích thước giảm dần, gán community_id = 1..K
    sorted_comms = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)
    community_id_map: Dict[str, int] = {}
    community_size_map: Dict[str, int] = {}

    print("\n--- Thống kê cộng đồng ---")
    for cid, (lab, members) in enumerate(sorted_comms, start=1):
        size = len(members)
        community_id_map[lab] = cid
        community_size_map[lab] = size
        print(f"  Cộng đồng {cid}: size={size} (nhãn nội bộ: {lab})")

    # Gắn thông tin cộng đồng vào node
    updated_nodes: List[Dict] = []
    for title, node in title_to_node.items():
        lab = labels.get(title)
        if lab is not None:
            node["community_label"] = lab
            node["community_id"] = int(community_id_map.get(lab, 0))
            node["community_size"] = int(community_size_map.get(lab, 0))
        else:
            # Node cô lập (không có trong adj), cho mỗi node là 1 cộng đồng riêng
            node["community_label"] = title
            node["community_id"] = 0
            node["community_size"] = 1
        updated_nodes.append(node)

    os.makedirs(os.path.dirname(NODES_OUT), exist_ok=True)
    with open(NODES_OUT, "w", encoding="utf-8") as f:
        json.dump(updated_nodes, f, ensure_ascii=False, indent=4)

    print(f"\n✅ Đã gán community_id / community_size cho {len(updated_nodes)} node.")
    print(f"   File output: {NODES_OUT}")

    # In thử vài cộng đồng lớn nhất
    print("\nTop 3 cộng đồng lớn nhất (liệt kê tối đa 5 node đầu tiên mỗi cộng đồng):")
    for cid, (lab, members) in enumerate(sorted_comms[:3], start=1):
        print(f"  Cộng đồng {cid} (size={len(members)}): {', '.join(sorted(members)[:5])} ...")


if __name__ == "__main__":
    print("--- 🚀 Phát hiện cộng đồng trong mạng tri thức triều Nguyễn ---")
    try:
        detect_communities()
        print("\n--- Hoàn tất detect_communities ---")
    except FileNotFoundError as e:
        print(f"❌ Thiếu file đầu vào: {e}")
        print("   Cần có network_nodes_ranked.json (hoặc network_nodes_enriched.json), network_relationships_full.json, network_relationships_text_based.json.")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi: {e}")


