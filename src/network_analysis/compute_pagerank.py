import json
import os
from typing import Dict, List, Set

from src.common.config_paths import DATA_PROCESSED


# === CẤU HÌNH ĐƯỜNG DẪN ===

# Ưu tiên hợp nhất vào file đã có degree/rank.
NODES_RANKED_IN = os.path.join(DATA_PROCESSED, "network_nodes_ranked.json")
NODES_FALLBACK_IN = os.path.join(DATA_PROCESSED, "network_nodes_enriched.json")

RELS_BASE_IN = os.path.join(DATA_PROCESSED, "network_relationships_full.json")
RELS_TEXT_IN = os.path.join(DATA_PROCESSED, "network_relationships_text_based.json")

# Ghi đè lại file ranked (hợp nhất thêm cột PageRank)
NODES_OUT = os.path.join(DATA_PROCESSED, "network_nodes_ranked.json")


def load_nodes() -> Dict[str, Dict]:
    """
    Đọc nodes, ưu tiên file ranked (đã có degree), fallback sang enriched.
    Trả về map title -> node.
    """
    path = NODES_RANKED_IN if os.path.exists(NODES_RANKED_IN) else NODES_FALLBACK_IN
    print(f"Đang đọc nodes từ: {path}")
    with open(path, "r", encoding="utf-8") as f:
        nodes = json.load(f)
    if not isinstance(nodes, List):
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


def build_directed_graph(
    titles: Set[str], rels: List[Dict]
) -> Dict[str, Set[str]]:
    """
    Xây đồ thị có hướng: adjacency list source -> {targets}.
    Chỉ giữ cạnh giữa các node có title hợp lệ.
    """
    adj: Dict[str, Set[str]] = {t: set() for t in titles}
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
        kept += 1

    print(f"  > Số cạnh có hướng sau khi lọc: {kept}")
    return adj


def compute_pagerank(
    adj: Dict[str, Set[str]],
    damping: float = 0.85,
    max_iters: int = 50,
    tol: float = 1e-6,
) -> Dict[str, float]:
    """
    PageRank cơ bản bằng power iteration.
    Trả về dict title -> pagerank (chuẩn hóa tổng = 1).
    """
    nodes = list(adj.keys())
    n = len(nodes)
    if n == 0:
        return {}

    print(f"--- Bắt đầu tính PageRank cho {n} node ---")
    # Map node -> index
    index = {node: i for i, node in enumerate(nodes)}

    # Out-degree
    out_degree = [len(adj[node]) for node in nodes]

    # Khởi tạo rank đều nhau
    rank = [1.0 / n] * n

    for it in range(max_iters):
        new_rank = [0.0] * n
        # Tổng rank của các node "dangling" (không có outgoing edge)
        dangling_sum = 0.0

        for i, node in enumerate(nodes):
            if out_degree[i] == 0:
                dangling_sum += rank[i]
                continue
            share = rank[i] / out_degree[i]
            for target in adj[node]:
                j = index[target]
                new_rank[j] += damping * share

        # Phân phối lại mass của dangling nodes + teleport
        teleport = (1.0 - damping) / n
        dangling_share = damping * dangling_sum / n

        diff = 0.0
        for i in range(n):
            new_rank[i] += teleport + dangling_share
            diff += abs(new_rank[i] - rank[i])
        rank = new_rank

        print(f"  > Iter {it+1}: diff={diff:.6f}")
        if diff < tol:
            print("  > Hội tụ sớm.")
            break

    # Chuẩn hóa tổng = 1 (phòng trường hợp sai số số học)
    total = sum(rank)
    if total > 0:
        rank = [r / total for r in rank]

    return {node: rank[index[node]] for node in nodes}


def attach_pagerank() -> None:
    title_to_node = load_nodes()
    rels = load_relationships(RELS_BASE_IN, RELS_TEXT_IN)

    titles = set(title_to_node.keys())
    adj = build_directed_graph(titles, rels)

    pr = compute_pagerank(adj)
    if not pr:
        print("❌ Không tính được PageRank (đồ thị rỗng?).")
        return

    # Xếp hạng theo PageRank
    sorted_nodes = sorted(pr.items(), key=lambda x: x[1], reverse=True)
    pr_rank: Dict[str, int] = {title: i + 1 for i, (title, _) in enumerate(sorted_nodes)}

    updated_nodes: List[Dict] = []
    for title, node in title_to_node.items():
        node["pagerank"] = float(pr.get(title, 0.0))
        node["pagerank_rank"] = int(pr_rank.get(title, 0))
        updated_nodes.append(node)

    os.makedirs(os.path.dirname(NODES_OUT), exist_ok=True)
    with open(NODES_OUT, "w", encoding="utf-8") as f:
        json.dump(updated_nodes, f, ensure_ascii=False, indent=4)

    print(f"✅ Đã gắn PageRank cho {len(updated_nodes)} node.")
    print(f"   File output: {NODES_OUT}")

    print("\nTop 10 node theo PageRank:")
    for title, score in sorted_nodes[:10]:
        print(f"  - {title}: {score:.6f}")


if __name__ == "__main__":
    print("--- 🚀 Tính PageRank cho mạng tri thức triều Nguyễn ---")
    try:
        attach_pagerank()
        print("\n--- Hoàn tất compute_pagerank ---")
    except FileNotFoundError as e:
        print(f"❌ Thiếu file đầu vào: {e}")
        print("   Cần có network_nodes_enriched.json, network_relationships_full.json, network_relationships_text_based.json.")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi: {e}")


