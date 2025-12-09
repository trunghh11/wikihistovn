import json
import os
import math
import random
from collections import deque, defaultdict
from typing import Dict, List, Set, Tuple

from config_paths import DATA_PROCESSED


# === CẤU HÌNH ĐƯỜNG DẪN ===

NODES_IN = os.path.join(DATA_PROCESSED, "network_nodes_enriched.json")
RELS_BASE_IN = os.path.join(DATA_PROCESSED, "network_relationships_full.json")
RELS_TEXT_IN = os.path.join(DATA_PROCESSED, "network_relationships_text_based.json")


def load_nodes(path: str) -> Set[str]:
    """Đọc nodes, trả về tập title hợp lệ."""
    print(f"Đang đọc nodes từ: {path}")
    with open(path, "r", encoding="utf-8") as f:
        nodes = json.load(f)
    titles: Set[str] = set()
    for n in nodes:
        title = n.get("title")
        if title:
            titles.add(title)
    print(f"  > Số node có title: {len(titles)}")
    return titles


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
    Xây đồ thị vô hướng (adjacency list) từ danh sách cạnh.
    Chỉ giữ cạnh giữa các node có title nằm trong tập titles.
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


def connected_components(adj: Dict[str, Set[str]]) -> List[Set[str]]:
    """Tìm tất cả thành phần liên thông trong đồ thị vô hướng."""
    visited: Set[str] = set()
    components: List[Set[str]] = []

    for node in adj.keys():
        if node in visited:
            continue
        comp: Set[str] = set()
        queue = deque([node])
        visited.add(node)
        while queue:
            u = queue.popleft()
            comp.add(u)
            for v in adj[u]:
                if v not in visited:
                    visited.add(v)
                    queue.append(v)
        components.append(comp)

    components.sort(key=len, reverse=True)
    return components


def bfs_distances(adj: Dict[str, Set[str]], source: str, max_nodes: int = None) -> Dict[str, int]:
    """
    BFS từ một nguồn, trả về khoảng cách ngắn nhất tới các node khác.
    Nếu max_nodes != None, chỉ duyệt tối đa max_nodes node (cắt sớm để tiết kiệm thời gian).
    """
    dist: Dict[str, int] = {source: 0}
    queue = deque([source])
    visited_count = 1

    while queue:
        u = queue.popleft()
        d = dist[u]
        for v in adj[u]:
            if v not in dist:
                dist[v] = d + 1
                queue.append(v)
                visited_count += 1
                if max_nodes is not None and visited_count >= max_nodes:
                    return dist
    return dist


def analyze_small_world() -> None:
    titles = load_nodes(NODES_IN)
    rels = load_relationships(RELS_BASE_IN, RELS_TEXT_IN)
    adj = build_undirected_graph(titles, rels)

    if not adj:
        print("❌ Đồ thị rỗng sau khi xây adjacency.")
        return

    # Thống kê degree cơ bản
    degrees = [len(neigh) for neigh in adj.values()]
    num_nodes = len(adj)
    num_edges = sum(degrees) // 2
    avg_deg = sum(degrees) / num_nodes if num_nodes > 0 else 0.0

    print("\n=== THỐNG KÊ CƠ BẢN CỦA ĐỒ THỊ (VÔ HƯỚNG, GỘP CẠNH GỐC + VĂN BẢN) ===")
    print(f"Số node (có cạnh): {num_nodes}")
    print(f"Số cạnh vô hướng: {num_edges}")
    print(f"Độ trung bình <k>: {avg_deg:.2f}")

    # Thành phần liên thông
    comps = connected_components(adj)
    giant = comps[0]
    print("\n=== THÀNH PHẦN LIÊN THÔNG LỚN NHẤT (GIANT COMPONENT) ===")
    print(f"Số thành phần liên thông: {len(comps)}")
    print(f"Size thành phần lớn nhất: {len(giant)} (~{len(giant) / num_nodes * 100:.1f}% số node có cạnh)")

    # Ước lượng khoảng cách ngắn nhất trung bình bằng sampling
    sample_size = min(100, len(giant))
    sampled_nodes = random.sample(list(giant), sample_size)

    total_dist = 0
    total_pairs = 0
    max_dist = 0

    # Thống kê phân phối khoảng cách (1,2,3,4,5+)
    dist_buckets = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

    print("\n=== ƯỚC LƯỢNG KHOẢNG CÁCH NGẮN NHẤT TRUNG BÌNH (SMALL-WORLD) ===")
    print(f"Đang lấy mẫu BFS từ {sample_size} node trong giant component...")

    for idx, src in enumerate(sampled_nodes, start=1):
        if idx % 10 == 0 or idx == 1:
            print(f"  > BFS {idx}/{sample_size} từ node: {src}")

        dists = bfs_distances(adj, src)
        for tgt, d in dists.items():
            if tgt == src:
                continue
            total_dist += d
            total_pairs += 1
            if d > max_dist:
                max_dist = d
            if d <= 4:
                dist_buckets[d] += 1
            else:
                dist_buckets[5] += 1

    if total_pairs == 0:
        print("Không có cặp node nào để tính khoảng cách.")
        return

    avg_shortest_path = total_dist / total_pairs
    print(f"\nKhoảng cách ngắn nhất trung bình (ước lượng trên mẫu): {avg_shortest_path:.2f}")
    print(f"Đường kính ước lượng (max distance thấy trong mẫu): {max_dist}")

    total_bucket_pairs = sum(dist_buckets.values())
    print("\nPhân phối xấp xỉ khoảng cách (trên các cặp được tính):")
    for k in [1, 2, 3, 4, 5]:
        label = f"d={k}" if k < 5 else "d>=5"
        count = dist_buckets[k]
        pct = (count / total_bucket_pairs * 100) if total_bucket_pairs > 0 else 0.0
        print(f"  {label}: {count} cặp (~{pct:.1f}%)")

    # So sánh nhanh với log(N) / log(<k>) – đặc trưng small-world
    if avg_deg > 1:
        theo_small_world = math.log(len(giant)) / math.log(avg_deg)
        print(
            f"\nGiá trị tham chiếu small-world log(N)/log(<k>) với N={len(giant)}, <k>={avg_deg:.2f}: "
            f"{theo_small_world:.2f}"
        )
        print(
            "So sánh: nếu khoảng cách trung bình ~ cùng bậc với giá trị này (vài bước), "
            "ta có bằng chứng mạng mang tính 'thế giới nhỏ'."
        )


if __name__ == "__main__":
    print("--- 🚀 Phân tích 'thế giới nhỏ' cho mạng tri thức triều Nguyễn ---")
    try:
        analyze_small_world()
        print("\n--- Hoàn tất analyze_small_world ---")
    except FileNotFoundError as e:
        print(f"❌ Thiếu file đầu vào: {e}")
        print("   Cần có network_nodes_enriched.json, network_relationships_full.json, network_relationships_text_based.json.")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi: {e}")


