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
DEFAULT_OUTPUT = DATA_DIR / "articles_raw_wikitext.filtered.jsonl"


def chunked(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_batch(titles: List[str]) -> List[Dict]:
    params = {
        "action": "query",
        "format": "json",
        "titles": "|".join(titles),
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "formatversion": "2",
    }
    try:
        resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ! Lỗi gọi API: {e}")
        return []

    pages = resp.json().get("query", {}).get("pages", [])
    results = []
    for page in pages:
        if "missing" in page:
            continue
        revs = page.get("revisions", [])
        if not revs:
            continue
        # formatversion=2 + rvslots=main => content nằm trong slots.main.content
        slots = revs[0].get("slots") or {}
        main_slot = slots.get("main") or {}
        content = main_slot.get("content") or ""
        if not content.strip():
            continue
        results.append({
            "title": page.get("title", ""),
            "page_id": page.get("pageid", 0),
            "wikitext": content,
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
    titles = list(dict.fromkeys(titles))
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
        time.sleep(0.5)
        if processed % 500 == 0:
            print(f"  > Đã xử lý {processed}/{total}, giữ {kept}")

    out_f.close()
    print("Hoàn tất thu thập wikitext.")
    print(f"- Tổng title yêu cầu: {total}")
    print(f"- Số bài ghi được  : {kept}")
    print(f"- File output      : {output_path}")


if __name__ == "__main__":
    main()

