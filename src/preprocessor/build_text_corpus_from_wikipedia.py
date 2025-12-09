import json
import os
import time
from typing import List, Dict

import mwparserfromhell
import requests

from config_paths import DATA_PROCESSED


# === CẤU HÌNH CƠ BẢN ===

API_URL = "https://vi.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Project for university; contact: 22024527@vnu.edu.vn)"
}

# Đường dẫn dữ liệu đầu vào / đầu ra (tương thích cấu trúc hiện tại)
NODES_JSON_IN = os.path.join(DATA_PROCESSED, "network_nodes_full.json")
TEXT_CORPUS_OUT = os.path.join(DATA_PROCESSED, "network_nodes_texts.jsonl")


def load_nodes(path: str) -> List[Dict]:
    """Đọc danh sách node từ file JSON đã build_graph."""
    print(f"Đang đọc nodes từ: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("File network_nodes_full.json phải chứa một list các node.")
    print(f"  > Số node: {len(data)}")
    return data


def extract_plain_and_intro_text(wikitext: str) -> Dict[str, str]:
    """
    Chuyển wikitext → plain text, tách phần intro.

    - plain_text: toàn bộ nội dung bài (đã bỏ markup wiki).
    - intro_text: đoạn mở đầu (trước section đầu tiên hoặc trước khoảng trắng lớn).
    """
    if not wikitext:
        return {"plain_text": "", "intro_text": ""}

    wikicode = mwparserfromhell.parse(wikitext)
    plain = wikicode.strip_code().strip()

    # Tách intro: đơn giản là lấy đoạn trước heading '=='
    intro = plain
    # Thử cắt ở heading dạng "== Tiêu đề =="
    for sep in ["\n==", "\n== ", "\n=== "]:
        idx = intro.find(sep)
        if idx != -1:
            intro = intro[:idx].strip()
            break

    # Nếu intro quá ngắn, giữ nguyên plain
    if len(intro) < 50:
        intro = plain

    return {"plain_text": plain, "intro_text": intro}


def fetch_pages_by_ids(page_ids: List[int]) -> Dict[int, Dict]:
    """
    Gọi API theo batch pageids để lấy wikitext cho nhiều trang cùng lúc.
    Trả về dict {page_id: {"title": ..., "wikitext": ...}}.
    """
    result: Dict[int, Dict] = {}
    if not page_ids:
        return result

    batch_size = 50
    total = len(page_ids)
    total_batches = (total // batch_size) + (1 if total % batch_size else 0)

    print(f"--- Bắt đầu gọi API Wikipedia cho {total} trang (chia {total_batches} batch) ---")

    for i in range(0, total, batch_size):
        batch = page_ids[i : i + batch_size]
        batch_num = (i // batch_size) + 1

        params = {
            "action": "query",
            "format": "json",
            "pageids": "|".join(str(pid) for pid in batch),
            "prop": "revisions",
            "rvprop": "content",
            "formatversion": "2",
        }

        print(f"  > Batch {batch_num}/{total_batches} ({len(batch)} trang)...")
        try:
            time.sleep(1)  # lịch sự với API
            resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            pages = resp.json().get("query", {}).get("pages", [])

            for page in pages:
                if "missing" in page:
                    continue
                pid = page.get("pageid")
                title = page.get("title")
                wikitext = page.get("revisions", [{}])[0].get("content", "")
                if not wikitext:
                    continue
                result[pid] = {
                    "title": title,
                    "wikitext": wikitext,
                }
        except Exception as e:
            print(f"    ! Lỗi khi gọi batch {batch_num}: {e}")

    print(f"--- Đã lấy được nội dung cho {len(result)}/{total} page_id ---")
    return result


def build_text_corpus(nodes_path: str, out_path: str) -> None:
    """
    Xây corpus văn bản từ Wikipedia cho tất cả node trong network_nodes_full.json.

    Output: file JSON Lines, mỗi dòng:
      {
        "page_id": int,
        "title": str,
        "label": str,
        "plain_text": str,
        "intro_text": str
      }
    """
    nodes = load_nodes(nodes_path)

    # Lấy danh sách page_id hợp lệ
    page_ids: List[int] = []
    page_id_to_node: Dict[int, Dict] = {}

    for node in nodes:
        pid = node.get("page_id")
        # Loại các giá trị None/0/không phải số
        if isinstance(pid, int) and pid > 0:
            page_ids.append(pid)
            page_id_to_node[pid] = node

    page_ids = sorted(set(page_ids))
    print(f"  > Số page_id hợp lệ: {len(page_ids)}")

    # Gọi API lấy nội dung
    pages_content = fetch_pages_by_ids(page_ids)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    written = 0

    print(f"\n--- Ghi corpus ra file: {out_path} ---")
    with open(out_path, "w", encoding="utf-8") as fout:
        for pid, page_info in pages_content.items():
            node = page_id_to_node.get(pid, {})
            label = node.get("label", "Thực thể")
            title = page_info.get("title", node.get("title", ""))

            texts = extract_plain_and_intro_text(page_info.get("wikitext", ""))

            record = {
                "page_id": pid,
                "title": title,
                "label": label,
                "plain_text": texts["plain_text"],
                "intro_text": texts["intro_text"],
            }
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1

    print(f"✅ Đã ghi {written} dòng vào corpus văn bản.")
    print("   (mỗi dòng là một JSON, có thể load dần để huấn luyện / phân tích)")


if __name__ == "__main__":
    print("--- 🚀 Xây dựng corpus văn bản từ Wikipedia cho các node trong network ---")
    try:
        build_text_corpus(NODES_JSON_IN, TEXT_CORPUS_OUT)
        print("\n--- Hoàn tất build_text_corpus_from_wikipedia ---")
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file nodes tại: {NODES_JSON_IN}")
        print("   Hãy chạy script build_full_network.py trước.")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi: {e}")


