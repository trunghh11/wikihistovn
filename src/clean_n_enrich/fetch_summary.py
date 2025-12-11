import requests
import json
import time
from tqdm import tqdm
import os

# --- CẤU HÌNH ---
INPUT_NODES_FILE = "data/processed/nodes_metadata.json"  # File đầu vào từ Step 1
OUTPUT_NODES_FILE = "data/processed/nodes_metadata_enriched.json" # File đầu ra có thêm summary

HEADERS = {
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Summary fetcher; contact: student@vnu.edu.vn)"
}

def fetch_wiki_summary(title):
    """
    Lấy đoạn mở đầu (summary/extract) của bài viết Wikipedia tiếng Việt theo tiêu đề.
    """
    url = "https://vi.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "titles": title,
        "prop": "extracts",
        "exintro": True,       # Chỉ lấy phần mở đầu (introduction)
        "explaintext": True,   # Lấy văn bản thuần (plaintext)
        "redirects": True      # Theo dõi các trang đổi hướng
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        
        # Nếu gặp lỗi server (5xx) hoặc Client (4xx), trả về chuỗi rỗng để không chết chương trình
        if response.status_code != 200:
            return ""

        data = response.json()
        
        # Xử lý kết quả trả về
        if 'query' not in data or 'pages' not in data['query']:
            return ""

        page = next(iter(data['query']['pages'].values()))
        
        if 'missing' in page:
            return ""
        
        # Lấy extract, nếu không có thì trả về rỗng
        summary = page.get('extract', "")
        return summary

    except Exception as e:
        print(f"\n[Lỗi] {title}: {e}")
        return ""

def main():
    # 1. Kiểm tra file đầu vào
    if not os.path.exists(INPUT_NODES_FILE):
        print(f"❌ Không tìm thấy file: {INPUT_NODES_FILE}. Hãy chạy Step 1 trước.")
        return

    print(f"--- ĐANG ĐỌC DỮ LIỆU TỪ {INPUT_NODES_FILE} ---")
    with open(INPUT_NODES_FILE, 'r', encoding='utf-8') as f:
        nodes = json.load(f)

    print(f"🔹 Tổng số node cần xử lý: {len(nodes)}")
    
    # 2. Duyệt qua từng node và lấy summary
    # Sử dụng tqdm để hiện thanh tiến trình
    updated_nodes = []
    
    for node in tqdm(nodes, desc="Fetching Summaries"):
        title = node.get('title', '')
        
        # Nếu chưa có summary hoặc summary rỗng thì mới fetch
        if 'summary' not in node or not node['summary']:
            summary = fetch_wiki_summary(title)
            # Làm sạch summary (xóa xuống dòng thừa)
            node['summary'] = summary.replace('\n', ' ').strip()
        
        updated_nodes.append(node)
        
        # Delay nhẹ 0.1s để tôn trọng server Wikipedia (tránh lỗi 429 Too Many Requests)
        time.sleep(0.1)

    # 3. Lưu kết quả
    print(f"\n--- ĐANG LƯU KẾT QUẢ RA {OUTPUT_NODES_FILE} ---")
    with open(OUTPUT_NODES_FILE, 'w', encoding='utf-8') as f:
        json.dump(updated_nodes, f, ensure_ascii=False, indent=4)
    
    print(f"✅ Hoàn tất! Đã thêm summary cho {len(updated_nodes)} nodes.")

if __name__ == "__main__":
    main()