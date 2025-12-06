import argparse
from typing import List
import warnings

# -----------------------------------------------
# Cấu hình để tắt các cảnh báo (warning) từ NumPy
# -----------------------------------------------
# 1) Cảnh báo "Numpy built with MINGW-W64 on Windows 64 bits is experimental..."
warnings.filterwarnings(
    "ignore",
    message=r"Numpy built with MINGW-W64 on Windows 64 bits is experimental.*",
)

# 2) Các RuntimeWarning kiểu "invalid value encountered in exp2/log10/nextafter"
warnings.filterwarnings("ignore", category=RuntimeWarning, module=r"numpy\..*")

# LƯU Ý: Các dòng trên phải đặt TRƯỚC khi import pandas/networkx
import networkx as nx
import pandas as pd


def load_graph_from_edges_csv(
    edges_path: str,
    directed: bool = False,
) -> nx.Graph:
    """
    Load a graph from an edges CSV file with at least three columns:
    sourceId, targetId, type.

    Parameters
    ----------
    edges_path : str
        Path to the edges.csv file.
    directed : bool
        If True, build a directed graph. Otherwise, an undirected graph.
    """
    graph_cls = nx.DiGraph if directed else nx.Graph
    g = graph_cls()

    df = pd.read_csv(edges_path)

    # Expect the columns as in data/processed/edges.csv
    required_cols = {"sourceId", "targetId"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in CSV: {missing_cols}")

    # For now we treat edges as unweighted (weight = 1 for all)
    # but we attach the 'type' column as an edge attribute for future use.
    for _, row in df.iterrows():
        source = row["sourceId"]
        target = row["targetId"]
        edge_type = row.get("type", None)

        if edge_type is not None:
            g.add_edge(source, target, type=edge_type, weight=1)
        else:
            g.add_edge(source, target, weight=1)

    return g


def shortest_path(
    g: nx.Graph,
    source: str,
    target: str,
) -> List[str]:
    """
    Compute the unweighted shortest path between two nodes using NetworkX.

    Raises:
        nx.NetworkXNoPath: if there is no path between source and target.
        nx.NodeNotFound: if either node is not in the graph.
    """
    return nx.shortest_path(g, source=source, target=target, weight=None)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Demo: tìm đường đi ngắn nhất giữa 2 node "
            "trên đồ thị được xây từ data/processed/edges.csv bằng NetworkX."
        )
    )
    parser.add_argument(
        "--edges-path",
        default="data/processed/edges.csv",
        help="Đường dẫn tới file edges.csv (mặc định: data/processed/edges.csv)",
    )
    parser.add_argument(
        "--source",
        required=False,
        help="Tên node bắt đầu (ví dụ: 'Minh Mạng'). Nếu bỏ trống sẽ dùng mặc định.",
    )
    parser.add_argument(
        "--target",
        required=False,
        help="Tên node kết thúc (ví dụ: 'Gia Long'). Nếu bỏ trống sẽ dùng mặc định.",
    )
    parser.add_argument(
        "--directed",
        action="store_true",
        help="Xây đồ thị có hướng (DiGraph). Mặc định là vô hướng.",
    )

    args = parser.parse_args()

    # Nếu người dùng không truyền tham số, dùng cặp mặc định để luôn có kết quả demo
    source = args.source or "Minh Mạng"
    target = args.target or "Gia Long"

    print(f"Đang tìm đường đi ngắn nhất từ '{source}' tới '{target}'...")

    g = load_graph_from_edges_csv(args.edges_path, directed=bool(args.directed))

    try:
        path = shortest_path(g, source=source, target=target)
    except nx.NodeNotFound as e:
        print(f"Lỗi: {e}")
        return
    except nx.NetworkXNoPath:
        print(
            f"Không tồn tại đường đi giữa '{source}' và '{target}' "
            "trên đồ thị."
        )
        return

    print("Đường đi ngắn nhất (tính theo số cạnh):")
    print(" -> ".join(path))
    print(f"Số bước: {len(path) - 1}")


if __name__ == "__main__":
    main()


