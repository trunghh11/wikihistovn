import json
import csv
import re
import os
# Đảm bảo bạn đã import hàm từ file parse_infobox.py
from parse_infobox import get_infobox_data

# === CẤU HÌNH ===

# Tự động tìm đường dẫn thư mục gốc (WIKIHISTOVN)
# SCRIPT_DIR sẽ là: .../WIKIHISTOVN/src/preprocessor
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT sẽ là: .../WIKIHISTOVN
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

# === ĐỊNH NGHĨA ĐƯỜNG DẪN TỪ THƯ MỤC GỐC ===

# 1. Đường dẫn INPUT (Nguồn dữ liệu)
# Trỏ đến: WIKIHISTOVN/data/processed/seed_data_nguyen_kings_from_api.json
JSON_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "seed_data_nguyen_kings_from_api.json")

# 2. Đường dẫn OUTPUT (Nơi lưu 2 tệp CSV)
# Trỏ đến: WIKIHISTOVN/data/processed/nodes.csv
NODES_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "nodes.csv")
# Trỏ đến: WIKIHISTOVN/data/processed/edges.csv
EDGES_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "edges.csv")
RELATIONSHIP_MAP = {
    # (Tên quan hệ, Nhãn node đích)
    "Tiền nhiệm": ("TIEN_NHIEM", "NhanVat"),
    "Kế nhiệm": ("KE_NHIEM", "NhanVat"),
    "Thân phụ": ("CHA_CUA", "NhanVat"),
    "Thân mẫu": ("ME_CUA", "NhanVat"),
    "Phối ngẫu": ("PHOI_NGAU_VOI", "NhanVat"),
    "An táng": ("AN_TANG_TAI", "DiaDiem"),
    "Sinh": ("SINH_TAI", "DiaDiem"), # Giả sử nơi sinh là địa điểm
    "Hoàng tộc": ("THUOC_HOANG_TOC", "ToChuc"),
    "Tôn giáo": ("THEO_TON_GIAO", "TonGiao"),
}

# === 2. THÊM HÀM PHÂN LOẠI (CLASSIFY) MỚI ===
def classify_node(node_title):
    """
    Phân loại node dựa trên Regex và từ khóa.
    """
    node_lower = node_title.lower()
    
    # 1. Lọc ngày tháng, năm
    if re.fullmatch(r'\d{1,2}\s+tháng\s+\d{1,2}', node_lower):
        return "NgayThang"
    if re.fullmatch(r'tháng\s+\d{1,2}', node_lower):
        return "NgayThang"
    if re.fullmatch(r'\d{4}', node_lower):
        return "Nam"
        
    # 2. Lọc địa điểm (thêm các tỉnh thành, quốc gia bạn biết vào đây)
    places = ['huế', 'việt nam', 'pháp quốc', 'hưng hóa', 'đại nam', 'hà nội', 'sài gòn']
    if node_lower in places:
        return "DiaDiem"
        
    # 3. Lọc tổ chức (thêm từ khóa)
    orgs = ['sứ học', 'nhà nguyễn', 'hội', 'quân đội']
    for org in orgs:
        if org in node_lower:
            return "ToChuc"

    # 4. Mặc định là Nhân Vật
    return "NhanVat"


# === 3. THAY ĐỔI CÁCH LƯU NODE ===
# Dùng dict để lưu {nodeId: label}, tránh trùng lặp và lưu được nhãn
nodes_dict = {} 
edges_list = []

print("Bắt đầu xử lý tệp JSON và thu thập dữ liệu Infobox...")

try:
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy tệp {JSON_FILE}")
    exit()
# ... (các except khác giữ nguyên) ...

if not isinstance(data, list):
    print(f"Lỗi: Tệp {JSON_FILE} không chứa một danh sách (list).")
    exit()

# === 4. SỬA LẠI VÒNG LẶP CHÍNH ===
for item in data:
    source_title = item.get('title')
    if not source_title:
        continue
        
    print(f"Đang xử lý: {source_title}")
    
    # Gán nhãn cho node nguồn (Giả sử các trang seed đều là Nhân Vật)
    if source_title not in nodes_dict:
        nodes_dict[source_title] = "NhanVat"
    
    # --- A. Xử lý Infobox (Nguồn chất lượng cao) ---
    infobox = get_infobox_data(source_title)
    if infobox:
        for key, value in infobox.items():
            if key in RELATIONSHIP_MAP:
                # Lấy cả 2 giá trị từ map
                relationship_type, target_label = RELATIONSHIP_MAP[key]
                target_title = re.sub(r'\[.*?\]', '', value).strip()
                
                if not target_title:
                    continue
                
                # Thêm node đích với nhãn ĐÚNG
                if target_title not in nodes_dict:
                    nodes_dict[target_title] = target_label
                
                # Tạo cạnh (edge)
                edges_list.append({
                    "sourceId": source_title,
                    "targetId": target_title,
                    "type": relationship_type
                })

    # --- B. Xử lý Links (Nguồn chất lượng thấp, dùng hàm classify) ---
    links = item.get('links', [])
    if links:
        for link_title in links:
            # Lọc nhiễu cơ bản
            if not link_title or link_title == source_title:
                continue
                
            # Phân loại node link này
            target_label = classify_node(link_title)
            
            # Thêm node đích với nhãn VỪA ĐOÁN ĐƯỢC
            if link_title not in nodes_dict:
                nodes_dict[link_title] = target_label
            
            # Tạo cạnh LIEN_QUAN_TOI
            edges_list.append({
                "sourceId": source_title,
                "targetId": link_title,
                "type": "LIEN_QUAN_TOI"
            })

print("\nXử lý hoàn tất. Bắt đầu ghi ra tệp CSV...")

# === 5. SỬA LẠI CÁCH GHI TỆP NODES.CSV ===
try:
    with open(NODES_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["nodeId", "label"]) # Viết header
        
        # Ghi từ dict (thay vì set)
        for node_id, label in nodes_dict.items():
            writer.writerow([node_id, label]) 
            
    print(f"Đã ghi {len(nodes_dict)} nodes vào {NODES_CSV}")
except IOError as e:
    print(f"Lỗi khi ghi tệp {NODES_CSV}: {e}")

# (Phần ghi EDGES.CSV giữ nguyên)
try:
    if edges_list:
        with open(EDGES_CSV, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=edges_list[0].keys())
            writer.writeheader()
            writer.writerows(edges_list)
        print(f"Đã ghi {len(edges_list)} edges vào {EDGES_CSV}")
    else:
        print("Không có edges nào để ghi.")
except IOError as e:
    print(f"Lỗi khi ghi tệp {EDGES_CSV}: {e}")

print("\nHoàn thành! Bạn đã có 2 tệp 'nodes.csv' và 'edges.csv' với nhãn đúng.")