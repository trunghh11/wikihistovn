import json
import time
from pathlib import Path
from typing import Dict, List

import requests

HEADERS = {
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Project for university; contact: 22024527@vnu.edu.vn)"
}
API_URL = "https://vi.wikipedia.org/w/api.php"

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"
DEFAULT_INPUT = DATA_DIR / "network_nodes_full.filtered.json"
DEFAULT_OUTPUT = DATA_DIR / "articles_raw.filtered.jsonl"


def chunked(items: List[str], size: int) -> List[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_batch(titles: List[str]) -> List[Dict]:
    """
    Fetch plaintext extracts for a batch of titles.
    Returns list of dicts with title, page_id, text.
    """
    if not titles:
        return []

    params = {
        "action": "query",
        "format": "json",
        "titles": "|".join(titles),
        "prop": "extracts",
        "explaintext": 1,
        "formatversion": "2",
    }
    try:
        resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ! Lỗi gọi API cho batch {len(titles)} mục: {e}")
        return []

    pages = resp.json().get("query", {}).get("pages", [])
    results = []
    for page in pages:
        if "missing" in page:
            continue
        text = page.get("extract") or ""
        if not text.strip():
            continue
        results.append({
            "title": page.get("title", ""),
            "page_id": page.get("pageid", 0),
            "text": text,
            "source": "wikipedia",
        })
    return results


def main(input_path: Path = DEFAULT_INPUT, output_path: Path = DEFAULT_OUTPUT):
    if not input_path.exists():
        print(f"❌ Không tìm thấy file input: {input_path}")
        return

    print(f"Đang đọc danh sách node từ: {input_path}")
    nodes = json.load(open(input_path, "r", encoding="utf-8"))
    titles = [n.get("title") for n in nodes if n.get("title")]
    titles = list(dict.fromkeys(titles))  # remove duplicates, preserve order
    total = len(titles)
    print(f"Tổng số title cần fetch: {total}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_f = open(output_path, "w", encoding="utf-8")

    processed = 0
    kept = 0
    for batch in chunked(titles, 50):
        processed += len(batch)
        items = fetch_batch(batch)
        kept += len(items)
        for obj in items:
            out_f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        # lịch sự với API
        time.sleep(0.5)
        if processed % 500 == 0:
            print(f"  > Đã xử lý {processed}/{total} title, giữ {kept}")

    out_f.close()
    print("Hoàn tất thu thập văn bản.")
    print(f"- Tổng title yêu cầu: {total}")
    print(f"- Số bài ghi được  : {kept}")
    print(f"- File output      : {output_path}")


if __name__ == "__main__":
    main()

