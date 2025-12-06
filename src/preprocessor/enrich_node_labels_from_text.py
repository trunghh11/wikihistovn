import json
import os
import re
from typing import Dict, List

from config_paths import DATA_PROCESSED


# === CẤU HÌNH ĐƯỜNG DẪN ===

NODES_IN = os.path.join(DATA_PROCESSED, "network_nodes_full.json")
TEXTS_IN = os.path.join(DATA_PROCESSED, "network_nodes_texts.jsonl")
NODES_OUT = os.path.join(DATA_PROCESSED, "network_nodes_enriched.json")


def load_nodes(path: str) -> List[Dict]:
    print(f"Đang đọc nodes từ: {path}")
    with open(path, "r", encoding="utf-8") as f:
        nodes = json.load(f)
    if not isinstance(nodes, list):
        raise ValueError("File nodes phải chứa một list các node.")
    print(f"  > Số node: {len(nodes)}")
    return nodes


def load_texts(path: str) -> Dict[int, Dict]:
    """
    Đọc corpus Wikipedia (JSONL) và trả về dict:
      {page_id: {"intro_text": str, "plain_text": str}}
    """
    print(f"Đang đọc corpus văn bản từ: {path}")
    texts: Dict[int, Dict] = {}
    count = 0
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
                texts[pid] = {
                    "intro_text": rec.get("intro_text", "") or "",
                    "plain_text": rec.get("plain_text", "") or "",
                }
                count += 1
    print(f"  > Đọc được text cho {count} page_id")
    return texts


def classify_label_from_text(intro_text: str, current_label: str) -> str:
    """
    Phân loại đơn giản dựa trên intro_text.
    Chỉ "sửa" lại nếu current_label đang là nhãn rất chung như 'Thực thể'.
    """
    if current_label and current_label not in ("Thực thể", "Entity", "Other"):
        # Đã có nhãn khá rõ (Vua Nhà Nguyễn, Nhân vật Lịch sử, Sự kiện...)
        return current_label

    if not intro_text:
        return current_label or "Thực thể"

    text = intro_text.lower()

    # Heuristic cho ĐỊA DANH
    place_keywords = [
        "là một tỉnh",
        "là một thành phố",
        "là một huyện",
        "là một quận",
        "là một xã",
        "là một phường",
        "là một thị trấn",
        "là một quốc gia",
        "là một tiểu bang",
        "là một vùng",
        "là một đảo",
        "là một bán đảo",
        "là một ngọn núi",
        "là một con sông",
        "là một hồ",
        "là một vịnh",
    ]
    if any(k in text for k in place_keywords):
        return "Địa danh"

    # Heuristic cho SỰ KIỆN
    event_keywords = [
        "là một trận",
        "là trận",
        "là một cuộc chiến",
        "là một cuộc chiến tranh",
        "là một cuộc khởi nghĩa",
        "là một khởi nghĩa",
        "là một chiến dịch",
        "là một phong trào",
        "là một sự kiện",
        "là một vụ",
    ]
    if any(k in text for k in event_keywords):
        return "Sự kiện"

    # Heuristic cho TỔ CHỨC / THIẾT CHẾ
    org_keywords = [
        "là một đảng chính trị",
        "là một tổ chức",
        "là một cơ quan",
        "là một trường đại học",
        "là một trường cao đẳng",
        "là một câu lạc bộ",
        "là một doanh nghiệp",
        "là một công ty",
        "là một tập đoàn",
        "là một tờ báo",
        "là một nhật báo",
        "là một tạp chí",
    ]
    if any(k in text for k in org_keywords):
        return "Tổ chức"

    # Heuristic cho NHÂN VẬT LỊCH SỬ
    person_patterns = [
        r"là một .*nhà [a-zàáạãảăắằặẵẳâấầậẫẩêếềệễểôốồộỗổơớờợỡởưứừựữử]+",  # nhà văn, nhà thơ...
        r"là một .*hoàng đế",
        r"là một .*vua",
        r"là một .*hoàng hậu",
        r"là một .*tướng",
        r"là một .*quan lại",
        r"sinh năm \d{3,4}",
    ]
    if any(re.search(pat, text) for pat in person_patterns):
        return "Nhân vật Lịch sử"

    # Nếu không nhận dạng được thì giữ nguyên / gán Thực thể
    return current_label or "Thực thể"


def enrich_labels() -> None:
    nodes = load_nodes(NODES_IN)
    texts = load_texts(TEXTS_IN)

    updated = 0
    for node in nodes:
        pid = node.get("page_id")
        current_label = node.get("label", "") or ""
        intro = ""
        if isinstance(pid, int) and pid in texts:
            intro = texts[pid]["intro_text"]

        new_label = classify_label_from_text(intro, current_label)
        if new_label != current_label:
            node["label"] = new_label
            updated += 1

    os.makedirs(os.path.dirname(NODES_OUT), exist_ok=True)
    with open(NODES_OUT, "w", encoding="utf-8") as f:
        json.dump(nodes, f, ensure_ascii=False, indent=4)

    print(f"✅ Đã cập nhật nhãn cho {updated} node.")
    print(f"   File output: {NODES_OUT}")


if __name__ == "__main__":
    print("--- 🚀 Làm giàu nhãn node từ văn bản Wikipedia (intro_text) ---")
    try:
        enrich_labels()
        print("\n--- Hoàn tất enrich_node_labels_from_text ---")
    except FileNotFoundError as e:
        print(f"❌ Thiếu file đầu vào: {e}")
        print("   Hãy đảm bảo đã có network_nodes_full.json và network_nodes_texts.jsonl.")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi: {e}")


