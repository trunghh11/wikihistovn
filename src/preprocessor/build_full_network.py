import requests
import json
import os
import time
import mwparserfromhell
import re
from typing import List, Dict, Set, Any, Tuple

# --- CẤU HÌNH ---
API_URL = "https://vi.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Project for university; contact: 22024527@vnu.edu.vn)"
}
# File input (từ script trước)
SEED_FILE = '../../data/processed/seed_data_nguyen_kings_from_api.json'
# File output
OUTPUT_NODES = '../../data/processed/network_nodes_full.json'
OUTPUT_RELS = '../../data/processed/network_relationships_full.json'

# --- ĐỊNH NGHĨA CÁC LOẠI CẠNH TỪ INFOBOX ---

# 1. Cạnh từ Infobox của Vua (Nội tộc)
KING_REL_MAP = {
    # Key Infobox : (Loại Cạnh, Cạnh Đảo Ngược)
    'kế nhiệm': ('TIỀN_NHIỆM_CỦA', 'KẾ_NHIỆM_CỦA'),
    'tiền nhiệm': ('KẾ_NHIỆM_CỦA', 'TIỀN_NHIỆM_CỦA'),
    'thân phụ': ('LÀ_CON_CỦA', 'LÀ_CHA_CỦA'),
    'cha': ('LÀ_CON_CỦA', 'LÀ_CHA_CỦA'),
    'thân mẫu': ('LÀ_CON_CỦA', 'LÀ_MẸ_CỦA'),
    'mẹ': ('LÀ_CON_CỦA', 'LÀ_MẸ_CỦA'),
    'phối ngẫu': ('PHỐI_NGẪU_VỚI', 'PHỐI_NGẪU_VỚI'),
    'vợ': ('PHỐI_NGẪU_VỚI', 'PHỐI_NGẪU_VỚI'),
    'chồng': ('PHỐI_NGẪU_VỚI', 'PHỐI_NGẪU_VỚI'),
    'chính thất': ('PHỐI_NGẪU_VỚI', 'PHỐI_NGẪU_VỚI'),
    'phối mẫu': ('PHỐI_NGẪU_VỚI', 'PHỐI_NGẪU_VỚI'),
    'con cái': ('LÀ_CHA_CỦA', 'LÀ_CON_CỦA'),
    'hậu duệ': ('LÀ_CHA_CỦA', 'LÀ_CON_CỦA'),
    'anh em': ('LÀ_ANH_EM_CỦA', 'LÀ_ANH_EM_CỦA'),
    'anh': ('LÀ_ANH_EM_CỦA', 'LÀ_ANH_EM_CỦA'),
    'em': ('LÀ_ANH_EM_CỦA', 'LÀ_ANH_EM_CỦA'),
    'chị': ('LÀ_ANH_EM_CỦA', 'LÀ_ANH_EM_CỦA'),
    'em gái': ('LÀ_ANH_EM_CỦA', 'LÀ_ANH_EM_CỦA'),
    'em trai': ('LÀ_ANH_EM_CỦA', 'LÀ_ANH_EM_CỦA'),
}

# 2. Cạnh từ Infobox của "Hàng xóm" (Ngoại tộc)
NEIGHBOR_REL_MAP = {
    'phục vụ': ('PHỤC_VỤ', 'ĐƯỢC_PHỤC_VỤ_BỞI'),
    'bổ nhiệm': ('ĐƯỢC_BỔ_NHIỆM_BỞI', 'BỔ_NHIỆM'),
    'thầy giáo': ('LÀ_THẦY_CỦA', 'LÀ_TRÒ_CỦA'),
    'giáo viên': ('LÀ_THẦY_CỦA', 'LÀ_TRÒ_CỦA'),
    'học trò': ('LÀ_TRÒ_CỦA', 'LÀ_THẦY_CỦA'),
    'chống': ('CHỐNG_ĐỐI', 'BỊ_CHỐNG_ĐỐI_BỞI'),
    'đối thủ': ('ĐỐI_THỦ_CỦA', 'ĐỐI_THỦ_CỦA'),
    'tham chiến': ('THAM_GIA_SỰ_KIỆN', 'CÓ_THAM_GIA_BỞI'),
    'tham gia': ('THAM_GIA_SỰ_KIỆN', 'CÓ_THAM_GIA_BỞI'),
    'lãnh đạo': ('LÃNH_ĐẠO', 'ĐƯỢC_LÃNH_ĐẠO_BỞI'),
    'chỉ huy': ('CHỈ_HUY', 'ĐƯỢC_CHỈ_HUY_BỞI'),
    'chỉ huy bởi': ('ĐƯỢC_CHỈ_HUY_BỞI', 'CHỈ_HUY'),
    'chỉ huy 1': ('CHỈ_HUY', 'ĐƯỢC_CHỈ_HUY_BỞI'),
    'chỉ huy 2': ('CHỈ_HUY', 'ĐƯỢC_CHỈ_HUY_BỞI'),
    'đồng minh': ('ĐỒNG_MINH_VỚI', 'ĐỒNG_MINH_VỚI'),
    'kẻ thù': ('CHỐNG_ĐỐI', 'BỊ_CHỐNG_ĐỐI_BỞI'),
    'đồng đội': ('ĐỒNG_ĐỘI_VỚI', 'ĐỒNG_ĐỘI_VỚI'),
}

# --- CÁC HÀM TIỆN ÍCH ---

def is_valid_link(title: str) -> bool:
    """Lọc các liên kết chung chung, trang hệ thống, trang năm."""
    if not title: return False
    # Lọc các trang có số
    if any(char.isdigit() for char in title): return False
    # Lọc địa danh phổ biến
    if is_likely_location(title): return False
    # Lọc các trang hệ thống
    if title.lower().startswith(('tập tin:', 'thể loại:', 'bản mẫu:', 'danh sách', 'wikipedia:', 'chủ đề:')):
        return False
    # Lọc các thuật ngữ chung
    common_terms = [
        'việt nam', 'pháp', 'nhà nguyễn', 'lịch sử việt nam', 'đại nam', 
        'tiếng việt', 'hán tự', 'quốc ngữ', 'việt sử thông giám cương mục',
        'trung quốc', 'nhật bản', 'đông dương', 'châu âu', 'châu á',
        'thế chiến', 'chiến tranh thế giới', 'cách mạng', 'độc lập',
        'văn hóa', 'xã hội', 'kinh tế', 'chính trị', 'tôn giáo',
        'phật giáo', 'nho giáo', 'đạo giáo', 'công giáo', 'hồi giáo',
        'địa lý', 'lịch sử', 'nhân vật', 'sự kiện', 'triều đại',
        'hoàng đế', 'vua', 'quan lại', 'tướng lĩnh', 'binh lính',
        'học giả', 'nhà thơ', 'nhà văn', 'nhà sử học', 'nhà khoa học',
        'địa danh', 'sông', 'núi', 'biển', 'đảo', 'thành phố', 'làng',
        'huyện', 'tỉnh', 'quốc gia', 'đế quốc', 'cộng hòa', 'vương quốc',
        'chính phủ', 'quân đội', 'đảng', 'tổ chức', 'hội đồng', 'ủy ban'
    ]
    if title.lower() in common_terms: return False
    return True

def is_likely_location(title: str) -> bool:
    """
    Heuristic phát hiện địa danh để loại bỏ sớm.
    Tránh tạo node người nhầm với địa điểm.
    """
    t = title.lower()
    # Khớp theo từ khóa xuất hiện trong chuỗi
    location_keywords = [
        'tỉnh', 'thành phố', 'quận', 'huyện', 'xã', 'phường',
        'thị xã', 'thị trấn', 'vịnh', 'đảo', 'bán đảo', 'cảng',
        'cầu', 'đèo', 'đồi', 'núi', 'sông', 'suối', 'hồ', 'biển',
        'vườn quốc gia', 'khu bảo tồn', 'cố đô', 'kinh thành', 'thành cổ',
        'lăng', 'đền', 'chùa', 'miếu', 'thánh thất', 'nhà thờ', 'tu viện'
    ]
    # Khớp theo hậu tố phổ biến
    location_suffixes = [
        ' province', ' city', ' district', ' county', ' river', ' lake',
        ' bay', ' gulf', ' cape', ' strait', ' island', ' islands',
        ' archipelago', ' mountain', ' hill', ' range', ' bridge', ' port'
    ]
    # Bao quanh bởi khoảng trắng để khớp từ khóa nguyên vẹn
    wrapped = f" {t} "
    if any(f" {kw} " in wrapped for kw in location_keywords):
        return True
    if any(t.endswith(suf) for suf in location_suffixes):
        return True
    return False

def clean_infobox_value(value_wikitext: str) -> List[str]:
    """
    Tách các giá trị trong một trường Infobox (ví dụ: "A<br>B") thành list [A, B].
    Đây là hàm quan trọng để làm sạch tên thực thể.
    """
    if not value_wikitext:
        return []
    
    # 1. Tách các giá trị bằng <br> hoặc xuống dòng
    items = re.split(r'<br\s*/?>|\n', str(value_wikitext))
    cleaned_items = []
    
    for item in items:
        # 2. Dùng mwparserfromhell để làm sạch sâu (loại bỏ [[ ]], {{ }})
        parsed_item = mwparserfromhell.parse(item)
        
        # 3. Lấy văn bản thuần túy
        clean_text = parsed_item.strip_code().strip()
        
        # 4. Chỉ lấy phần trước dấu [ (chú thích) hoặc ( (ghi chú)
        clean_text = re.split(r'\[|\(', clean_text, 1)[0].strip()
        
        if clean_text:
            cleaned_items.append(clean_text)
    return cleaned_items

def extract_infobox_data_and_label(wikicode: mwparserfromhell.wikicode) -> Tuple[Dict, str]:
    """
    Trích xuất dữ liệu Infobox VÀ xác định Nhãn (Label) cho Node.
    """
    infoboxes = wikicode.filter_templates(matches=lambda t: 'infobox' in t.name.lower() or 'thông tin' in t.name.lower())
    if not infoboxes: 
        return {}, 'Thực thể' # Nhãn mặc định
    
    infobox = infoboxes[0]
    data = {}
    label = 'Thực thể' # Nhãn mặc định
    
    # Xác định Nhãn (Label)
    infobox_name = infobox.name.lower().strip()

    # Từ khóa liên quan đến 'Sự kiện'
    event_keywords = [
        'chiến tranh', 'trận đánh', 'khởi nghĩa', 'sự kiện', 'cuộc chiến',
        'cách mạng', 'phong trào', 'xung đột', 'nổi dậy', 'thảm sát',
        'hòa ước', 'hiệp định', 'tuyên ngôn', 'hội nghị', 'chiến dịch',
        'thời kỳ', 'thời đại', 'cuộc nổi dậy', 'cuộc khởi nghĩa', 'cuộc chiến tranh',
        'trận chiến', 'trận đánh', 'thảm họa', 'cuộc di cư', 'cuộc diệt chủng'
    ]
    
    # Từ khóa liên quan đến 'Nhân vật Lịch sử'
    person_keywords = [
        'nhân vật', 'quan lại', 'tướng', 'lãnh đạo',
        'nhà thơ', 'nhà văn', 'nhà sử học', 'nhà khoa học', 'nhà giáo',
        'chính trị gia', 'nhà hoạt động', 'nhà cách mạng', 'hoàng hậu',
        'công chúa', 'hoàng tử', 'nhà ngoại giao', 'nhà quân sự',
        'nhà tư tưởng', 'nhà triết học', 'nhà cải cách', 'nhà truyền giáo',
        'nhà sáng lập', 'nhà cai trị', 'nhà lãnh đạo', 'nhà thám hiểm'
    ]
    
    # Kiểm tra từ khóa để xác định nhãn
    if any(keyword in infobox_name for keyword in event_keywords):
        label = 'Sự kiện'
    elif any(keyword in infobox_name for keyword in person_keywords):
        label = 'Nhân vật Lịch sử'
    
    # Trích xuất dữ liệu từ Infobox
    for param in infobox.params:
        key = param.name.strip()
        value = param.value  # Giữ nguyên wikitext để hàm clean_infobox_value xử lý
        if value:
            data[key] = str(value)
    return data, label

def fetch_data_by_titles(titles_list: List[str]) -> Dict[str, Dict]:
    """
    Gọi API để "làm giàu" dữ liệu, xử lý theo batch 50
    VÀ BÁO CÁO TIẾN TRÌNH. Nếu có node bị rỗng infobox, fetch lại bằng page_id.
    """
    total_items = len(titles_list)
    total_batches = (total_items // 50) + (1 if total_items % 50 > 0 else 0)
    
    print(f"  > Đang gọi API để làm giàu {total_items} hàng xóm (chia thành {total_batches} lượt gọi)...")
    enriched_data = {}
    missing_pages = []  # Danh sách các page_id cần fetch lại

    # --- FETCH BẰNG TITLES ---
    for i in range(0, total_items, 50):
        current_batch_num = (i // 50) + 1
        batch_titles = titles_list[i:i+50]
        
        params = {
            "action": "query", "format": "json", "titles": "|".join(batch_titles),
            "prop": "revisions|info", "rvprop": "content", "formatversion": "2"
        }
        
        print(f"    > Đang xử lý lượt {current_batch_num}/{total_batches} ({len(batch_titles)} mục)...")
        
        try:
            time.sleep(1)  # Lịch sự với API
            response = requests.get(API_URL, params=params, headers=HEADERS)
            response.raise_for_status()
            pages = response.json().get("query", {}).get("pages", [])
            
            for page in pages:
                if "missing" in page:
                    continue
                
                title = page['title']
                wikitext = page.get("revisions", [{}])[0].get("content", "")
                if not wikitext:
                    print(f"    ⚠️ Trang '{title}' không có nội dung, thêm vào danh sách fetch lại.")
                    missing_pages.append(page['pageid'])
                    continue
                
                wikicode = mwparserfromhell.parse(wikitext)
                infobox, label = extract_infobox_data_and_label(wikicode)
                links = [link.title.strip() for link in wikicode.filter_wikilinks()]
                
                enriched_data[title] = {
                    'page_id': page['pageid'], 'title': title,
                    'infobox': infobox, 'links': links, 'label': label
                }
        except Exception as e:
            print(f"    ! Lỗi khi fetch lượt {current_batch_num}: {e}")
    
    # --- FETCH LẠI BẰNG PAGE ID ---
    if missing_pages:
        print(f"\n  > Đang fetch lại {len(missing_pages)} trang bị thiếu bằng page_id...")
        for i in range(0, len(missing_pages), 50):
            current_batch_num = (i // 50) + 1
            batch_ids = missing_pages[i:i+50]
            
            params = {
                "action": "query", "format": "json", "pageids": "|".join(map(str, batch_ids)),
                "prop": "revisions|info", "rvprop": "content", "formatversion": "2"
            }
            
            print(f"    > Đang xử lý lượt {current_batch_num}/{len(missing_pages)//50 + 1} ({len(batch_ids)} mục)...")
            
            try:
                time.sleep(1)  # Lịch sự với API
                response = requests.get(API_URL, params=params, headers=HEADERS)
                response.raise_for_status()
                pages = response.json().get("query", {}).get("pages", [])
                
                for page in pages:
                    if "missing" in page:
                        continue
                    
                    title = page['title']
                    wikitext = page.get("revisions", [{}])[0].get("content", "")
                    if not wikitext:
                        print(f"    ⚠️ Trang '{title}' vẫn không có nội dung sau khi fetch lại.")
                        continue
                    
                    wikicode = mwparserfromhell.parse(wikitext)
                    infobox, label = extract_infobox_data_and_label(wikicode)
                    links = [link.title.strip() for link in wikicode.filter_wikilinks()]
                    
                    enriched_data[title] = {
                        'page_id': page['pageid'], 'title': title,
                        'infobox': infobox, 'links': links, 'label': label
                    }
            except Exception as e:
                print(f"    ! Lỗi khi fetch lại bằng page_id: {e}")
    
    print(f"  > Làm giàu thành công {len(enriched_data)}/{total_items} hàng xóm.")
    return enriched_data

def add_relationship(source: str, target: str, type: str, all_rels: List, rel_set: Set):
    if type in ('PHỐI_NGẪU_VỚI', 'ĐỐI_THỦ_CỦA'):
        key = tuple(sorted((source, target))) + (type,)
    else:
        key = (source, target, type)
        
    if key not in rel_set:
        rel_set.add(key)
        all_rels.append({'source': source, 'target': target, 'type': type})

# --- HÀM CHÍNH ĐIỀU PHỐI QUÁ TRÌNH ---

def build_full_network():
    print("--- 🚀 Bắt đầu xây dựng mạng lưới (2 Lớp) ---")
    
    # --- KHỞI TẠO ---
    all_nodes: Dict[str, Dict] = {}        # Dict chứa tất cả các node, key là title
    all_relationships: List[Dict] = []   # List chứa tất cả các cạnh
    rel_set: Set[tuple] = set()          # Set để kiểm tra trùng lặp cạnh
    neighbors_to_fetch: Set[str] = set() # Set chứa các hàng xóm cần "làm giàu"
    
    try:
        with open(SEED_FILE, 'r', encoding='utf-8') as f:
            seed_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Lỗi: Không tìm thấy file hạt giống '{SEED_FILE}'. Dừng lại.")
        return

    # --- LỚP 1 (PASS 1): XỬ LÝ HẠT GIỐNG (13 VUA) ---
    print(f"--- Lớp 1: Đang xử lý {len(seed_data)} Node Hạt Giống (Vua) ---")
    for king in seed_data:
        title = king['title']
        
        # 1. Thêm Node Vua
        all_nodes[title] = {
            'page_id': king['page_id'], 'title': title,
            'label': 'Vua Nhà Nguyễn', 'infobox': king.get('infobox', {})
        }
        
        # 2. Tạo Cạnh Nội Tộc (Gia đình, Kế vị)
        infobox = king.get('infobox', {})
        for key, (rel_type, rev_type) in KING_REL_MAP.items():
            if key in infobox:
                targets = clean_infobox_value(infobox[key])
                for target_title in targets:
                    add_relationship(title, target_title, rel_type, all_relationships, rel_set)

        # 3. Thu thập "Hàng xóm" để làm giàu ở Lớp 2
        for link_title in king.get('links', []):
            if is_valid_link(link_title):
                neighbors_to_fetch.add(link_title)

    print(f"  > Xử lý xong {len(all_nodes)} vua.")
    print(f"  > Tìm thấy {len(neighbors_to_fetch)} hàng xóm tiềm năng để làm giàu.")

    # --- LỚP 2 (PASS 2): LÀM GIÀU "HÀNG XÓM" ---
    print("\n--- Lớp 2: Đang làm giàu Hàng xóm (Quan lại, Kẻ thù, Sự kiện...) ---")
    
    # Loại bỏ các vua ra khỏi danh sách fetch (vì đã có thông tin)
    neighbors_to_fetch.difference_update(all_nodes.keys())
    enriched_neighbors = fetch_data_by_titles(list(neighbors_to_fetch))
    
    for neighbor_title, neighbor_data in enriched_neighbors.items():
        # 1. Thêm Node Hàng xóm (Quan lại, Kẻ thù,...)
        if neighbor_title not in all_nodes:
            all_nodes[neighbor_title] = {
                'page_id': neighbor_data['page_id'], 'title': neighbor_title,
                'label': neighbor_data['label'], # Sử dụng nhãn đã được phân loại
                'infobox': neighbor_data.get('infobox', {})
            }
        
        # 2. Tạo Cạnh Ngoại Tộc (Phục vụ, Chống đối, ...)
        infobox = neighbor_data.get('infobox', {})
        for key, (rel_type, rev_type) in NEIGHBOR_REL_MAP.items():
            if key in infobox:
                targets = clean_infobox_value(infobox[key])
                for target_title in targets:
                    # Chỉ tạo cạnh nếu nó nối với một node ta đã biết (ví dụ: Vua)
                    if target_title in all_nodes:
                        add_relationship(neighbor_title, target_title, rel_type, all_relationships, rel_set)
    
    print(f"  > Đã thêm {len(enriched_neighbors)} node hàng xóm vào mạng lưới.")

    # --- LỚP 3 (PASS 3): TẠO CẠNH "FALLBACK" LIÊN_KẾT_TỚI ---
    print("\n--- Lớp 3: Đang tạo các Cạnh 'LIÊN_KẾT_TỚI' (Fallback) ---")
    
    # Chỉ tạo fallback từ các Vua (Hạt giống)
    for king in seed_data:
        source_title = king['title']
        for target_title in king.get('links', []):
            if not is_valid_link(target_title):
                continue
            
            # Chỉ tạo nếu node đích đã nằm trong mạng lưới của chúng ta
            if target_title not in all_nodes:
                continue
            
            # Kiểm tra xem đã có cạnh nào (bất kỳ chiều) giữa 2 node này chưa
            rel_exists = any(
                (r['source'] == source_title and r['target'] == target_title) or \
                (r['source'] == target_title and r['target'] == source_title) \
                for r in all_relationships
            )
            
            if not rel_exists:
                add_relationship(source_title, target_title, 'LIÊN_KẾT_TỚI', all_relationships, rel_set)
    
    print(f"  > Đã tạo xong các cạnh fallback.")

    # --- LƯU KẾT QUẢ ---
    print("\n--- HOÀN TẤT ---")
    
    final_nodes = list(all_nodes.values())
    
    with open(OUTPUT_NODES, 'w', encoding='utf-8') as f:
        json.dump(final_nodes, f, indent=4, ensure_ascii=False)
        
    with open(OUTPUT_RELS, 'w', encoding='utf-8') as f:
        json.dump(all_relationships, f, indent=4, ensure_ascii=False)

    print(f"📊 Tổng số Nodes: {len(final_nodes)}")
    print(f"↔️ Tổng số Relationships (Cạnh): {len(all_relationships)}")
    print(f"✅ Đã lưu file vào '{OUTPUT_NODES}' và '{OUTPUT_RELS}'")

# --- CHẠY SCRIPT ---
if __name__ == "__main__":
    # Đảm bảo thư mục processed tồn tại
    os.makedirs(os.path.dirname(OUTPUT_NODES), exist_ok=True)
    
    build_full_network()