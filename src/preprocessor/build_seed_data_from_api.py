import requests
import time
import json
import mwparserfromhell
import os

# Danh sách tên các vị vua để bắt đầu truy vấn
NGUYEN_DYNASTY_KINGS_NAMES = [
    "Gia Long", "Minh Mạng", "Thiệu Trị", "Tự Đức", "Dục Đức",
    "Hiệp Hòa", "Kiến Phúc", "Hàm Nghi", "Đồng Khánh", "Thành Thái",
    "Duy Tân", "Khải Định", "Bảo Đại"
]

API_URL = "https://vi.wikipedia.org/w/api.php"
HEADERS = {
    # Cung cấp User-Agent là một thông lệ tốt khi dùng API
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Project for university; contact: 22024527@vnu.edu.vn)"
}

def extract_infobox_data(wikicode, title):
    """Trích xuất dữ liệu từ template Infobox đầu tiên tìm thấy."""
    # In danh sách template để kiểm tra
    templates = wikicode.filter_templates()
    print(f"Templates found in {title}: {[t.name for t in templates]}")

    # Tìm Infobox (mở rộng điều kiện để bao gồm các tên template khác)
    infoboxes = wikicode.filter_templates(matches=lambda t: 'infobox' in t.name.lower() or 'thông tin nhân vật hoàng gia' in t.name.lower())
    if not infoboxes:
        print(f"⚠️ Không tìm thấy Infobox trong trang '{title}'.")
        return {}  # Trả về dictionary rỗng nếu không tìm thấy Infobox
    
    infobox = infoboxes[0]
    data = {}
    for param in infobox.params:
        key = param.name.strip()
        value = param.value.strip_code().strip()
        if value:
            data[key] = value
    return data

def build_seed_data_from_api(king_names, output_path):
    """
    Quy trình đơn giản hóa: Dùng tên để lấy nội dung trực tiếp,
    sau đó phân tích và lưu file.
    """
    print("--- Bước 1: Dùng tên các vị vua để lấy nội dung chi tiết từ API ---")
    
    params_get_content = {
        "action": "query",
        "format": "json",
        "titles": "|".join(king_names),
        "prop": "revisions|info",  # Lấy cả nội dung (revisions) và thông tin cơ bản (info) để có pageid
        "rvprop": "content",
        "formatversion": "2" # Dùng format version 2 để có cấu trúc JSON dễ xử lý hơn
    }
    
    try:
        response = requests.get(API_URL, params=params_get_content, headers=HEADERS)
        response.raise_for_status() # Báo lỗi nếu HTTP status không phải 2xx
        pages_content = response.json().get("query", {}).get("pages", [])
        
        # Lọc ra các trang không tìm thấy (trang "missing")
        valid_pages = [p for p in pages_content if "missing" not in p]
        
        print(f"✅ Lấy thành công nội dung cho {len(valid_pages)}/{len(king_names)} trang.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Lỗi mạng: {e}")
        return

    print("\n--- Bước 2: Phân tích nội dung và tạo file JSON ---")
    seed_data = []
    
    for page in valid_pages:
        # Lấy nội dung wikitext từ response
        wikitext = page.get("revisions", [{}])[0].get("content", "")
        if not wikitext:
            print(f"⚠️ Trang '{page.get('title')}' không có nội dung.")
            continue
            
        page_id = page.get("pageid")
        title = page.get("title")
        
        # Phân tích wikitext bằng mwparserfromhell
        wikicode = mwparserfromhell.parse(wikitext)
        
        # Trích xuất infobox và links
        infobox = extract_infobox_data(wikicode, title)
        links = [link.title.strip() for link in wikicode.filter_wikilinks()]
        
        seed_data.append({
            'page_id': page_id,
            'title': title,
            'infobox': infobox if infobox else {},  # Đảm bảo infobox luôn là dictionary
            'links': links
        })
        print(f"  -> Đã xử lý: {title} (ID: {page_id})")

    # Lưu kết quả vào file JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(seed_data, f, indent=4, ensure_ascii=False)
        
    print(f"\n🎉 Hoàn thành! Đã tìm và xử lý {len(seed_data)} nhân vật.")
    print(f"Danh sách hạt giống ban đầu đã được lưu tại: '{output_path}'")


if __name__ == "__main__":
    # Đường dẫn file output
    OUTPUT_SEED_PATH = '../../data/processed/seed_data_nguyen_kings_from_api.json'
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(os.path.dirname(OUTPUT_SEED_PATH), exist_ok=True)
    
    build_seed_data_from_api(NGUYEN_DYNASTY_KINGS_NAMES, OUTPUT_SEED_PATH)