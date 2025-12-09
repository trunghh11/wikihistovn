import json
import os
import re
from typing import Dict, List, Set, Tuple

from src.common.config_paths import DATA_PROCESSED


# === CẤU HÌNH ĐƯỜNG DẪN ===

NODES_IN = os.path.join(DATA_PROCESSED, "network_nodes_enriched.json")
TEXTS_IN = os.path.join(DATA_PROCESSED, "network_nodes_texts.jsonl")
RELS_OUT = os.path.join(DATA_PROCESSED, "network_relationships_text_based.json")


def load_nodes(path: str) -> Tuple[Dict[int, Dict], Dict[str, int]]:
    """
    Đọc danh sách node và tạo:
      - map page_id -> node
      - map title_lower -> page_id
    """
    print(f"Đang đọc nodes từ: {path}")
    with open(path, "r", encoding="utf-8") as f:
        nodes = json.load(f)

    if not isinstance(nodes, list):
        raise ValueError("File nodes phải chứa một list các node.")

    id_to_node: Dict[int, Dict] = {}
    title_to_id: Dict[str, int] = {}

    for n in nodes:
        pid = n.get("page_id")
        title = n.get("title")
        if isinstance(pid, int) and title:
            id_to_node[pid] = n
            title_to_id[title.lower()] = pid

    print(f"  > Số node có page_id hợp lệ: {len(id_to_node)}")
    return id_to_node, title_to_id


def load_texts(path: str) -> Dict[int, str]:
    """
    Đọc corpus (JSONL) và trả về:
      {page_id: plain_text_lower}
    """
    print(f"Đang đọc corpus văn bản từ: {path}")
    id_to_text: Dict[int, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            pid = rec.get("page_id")
            if isinstance(pid, int):
                text = (rec.get("plain_text") or "").lower()
                if text:
                    id_to_text[pid] = text
    print(f"  > Đọc được plain_text cho {len(id_to_text)} page_id")
    return id_to_text


def build_title_patterns(
    id_to_node: Dict[int, Dict],
) -> List[Tuple[int, str, re.Pattern]]:
    """
    Tạo danh sách (page_id, title, regex_pattern) cho các title đủ dài, không quá chung chung
    VÀ có nhãn mục tiêu phù hợp.
    """
    stop_titles = {
        "việt nam",
        "đại việt",
        "nhà nguyễn",
        "triều nguyễn",
        "lịch sử việt nam",
    }

    # Chỉ giữ các nhãn mà ta quan tâm làm node đích
    allowed_target_labels = {
        "Vua Nhà Nguyễn",
        "Nhân vật Lịch sử",
        "Địa danh",
        "Sự kiện",
        "Tổ chức",
    }

    patterns: List[Tuple[int, str, re.Pattern]] = []
    for pid, node in id_to_node.items():
        title = node.get("title")
        if not title:
            continue

        label = node.get("label", "")
        if label not in allowed_target_labels:
            continue

        t_lower = title.lower()
        # Bỏ các tiêu đề quá ngắn hoặc quá chung
        if len(t_lower) < 4 or t_lower in stop_titles:
            continue

        # Regex: khớp nguyên từ (word boundary), không phân biệt hoa thường
        escaped = re.escape(t_lower)
        pattern = re.compile(r"\b" + escaped + r"\b", flags=re.IGNORECASE)
        patterns.append((pid, title, pattern))

    # Sắp xếp theo độ dài tiêu đề giảm dần để ưu tiên cụm dài
    patterns.sort(key=lambda x: len(x[1]), reverse=True)
    print(f"  > Tạo regex cho {len(patterns)} tiêu đề node (đã lọc theo nhãn)")
    return patterns


def build_text_cooccurrence_relations() -> None:
    """
    Sinh các cạnh dựa trên việc đồng xuất hiện tên node trong văn bản Wikipedia.

    Logic đơn giản:
      - Với mỗi node A (page_id, title) có plain_text:
          * Dùng regex để tìm xem văn bản có nhắc tới tiêu đề của node B khác hay không.
          * Nếu có, tạo cạnh A --(ĐỒNG_XUẤT_HIỆN_VĂN_BẢN)--> B.
      - Dùng set để loại trùng lặp.
    """
    id_to_node, _ = load_nodes(NODES_IN)
    id_to_text = load_texts(TEXTS_IN)

    # Lọc sớm các node đích đủ điều kiện để giảm kích thước vòng lặp trong mỗi văn bản
    title_patterns = build_title_patterns(id_to_node)

    rels: List[Dict] = []
    rel_keys: Set[Tuple[str, str, str]] = set()

    total_docs = len(id_to_text)
    print(f"--- Bắt đầu trích xuất quan hệ đồng xuất hiện cho {total_docs} văn bản ---")

    for idx, (pid, text) in enumerate(id_to_text.items(), start=1):
        source_node = id_to_node.get(pid)
        if not source_node:
            continue

        source_title = source_node.get("title")
        if not source_title:
            continue

        if idx % 100 == 0 or idx == 1:
            print(f"  > Đang xử lý văn bản {idx}/{total_docs}: {source_title}")

        for target_pid, target_title, pattern in title_patterns:
            if target_pid == pid:
                continue

            target_node = id_to_node.get(target_pid)
            if not target_node:
                continue

            if not pattern.search(text):
                continue

            key = (source_title, target_title, "ĐỒNG_XUẤT_HIỆN_VĂN_BẢN")
            if key in rel_keys:
                continue

            rel_keys.add(key)
            rels.append(
                {
                    "source": source_title,
                    "target": target_title,
                    "type": "ĐỒNG_XUẤT_HIỆN_VĂN_BẢN",
                }
            )

    os.makedirs(os.path.dirname(RELS_OUT), exist_ok=True)
    with open(RELS_OUT, "w", encoding="utf-8") as f:
        json.dump(rels, f, ensure_ascii=False, indent=4)

    print(f"✅ Đã sinh {len(rels)} quan hệ đồng xuất hiện từ văn bản.")
    print(f"   File output: {RELS_OUT}")


if __name__ == "__main__":
    print("--- 🚀 Xây dựng quan hệ đồng xuất hiện từ corpus Wikipedia ---")
    try:
        build_text_cooccurrence_relations()
        print("\n--- Hoàn tất build_text_cooccurrence_relations ---")
    except FileNotFoundError as e:
        print(f"❌ Thiếu file đầu vào: {e}")
        print("   Hãy đảm bảo đã có network_nodes_enriched.json và network_nodes_texts.jsonl.")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi: {e}")


