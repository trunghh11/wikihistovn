import json
import csv
import re
from parse_infobox import get_infobox_data # Import hàm bạn vừa tạo

# === CẤU HÌNH ===

# Tệp JSON gốc chứa danh sách các vua
JSON_FILE = "seed_data_nguyen_kings_from_api.json" 
# Tệp CSV đầu ra
NODES_CSV = "nodes.csv"
EDGES_CSV = "edges.csv"

# 1. ĐỊNH NGHĨA ÁNH XẠ (Rất quan trọng)
# Ánh xạ từ key trong Infobox sang loại quan hệ trong Graph
# Bạn cần thêm các key khác mà bạn tìm thấy (ví dụ: "Phối ngẫu", "An táng"...)
RELATIONSHIP_MAP = {
    # Mối quan hệ giữa các nhân vật
    "Tiền nhiệm": "TIEN_NHIEM",
    "Kế nhiệm": "KE_NHIEM",
    "Thân phụ": "CHA_CUA",
    "Thân mẫu": "ME_CUA",
    "Phối ngẫu": "PHOI_NGAU_VOI", # Sẽ tạo cạnh (Vua) -> (Hoàng hậu)
    
    # Mối quan hệ với Địa điểm
    "An táng": "AN_TANG_TAI",     # Sẽ tạo cạnh (Vua) -> (Lăng)
    "Sinh": "SINH_TAI",          # Sẽ tạo cạnh (Vua) -> (Nơi sinh)
    
    # Mối quan hệ với Tổ chức / Khái niệm
    "Hoàng tộc": "THUOC_HOANG_TOC", # Sẽ tạo cạnh (Vua) -> (Nhà Nguyễn)
    "Tôn giáo": "THEO_TON_GIAO",   # Sẽ tạo cạnh (Vua) -> (Nho giáo)
    "Niên hiệu": "CO_NIEN_HIEU",   # Sẽ tạo cạnh (Vua) -> (Minh Mạng)
}

# === CHUẨN BỊ DỮ LIỆU ===
nodes_set = set()  # Dùng 'set' để tự động loại bỏ các node trùng lặp
edges_list = [] # Dùng 'list' để chứa tất cả các cạnh

print("Bắt đầu xử lý tệp JSON và thu thập dữ liệu Infobox...")

# === ĐỌC TỆP JSON GỐC ===
try:
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy tệp {JSON_FILE}")
    exit()
except json.JSONDecodeError:
    print(f"Lỗi: Tệp {JSON_FILE} không phải là JSON hợp lệ.")
    exit()

if not isinstance(data, list):
    print(f"Lỗi: Tệp {JSON_FILE} không chứa một danh sách (list).")
    exit()

# === 2. LẶP QUA TỪNG MỤC TRONG JSON ===
for item in data:
    source_title = item.get('title')
    
    if not source_title:
        continue
        
    print(f"Đang xử lý: {source_title}")
    
    # Thêm node nguồn (ví dụ: "Minh Mạng") vào danh sách
    nodes_set.add(source_title)
    
    # Gọi API để lấy Infobox
    infobox = get_infobox_data(source_title)
    
    if not infobox:
        print(f" -> Không có Infobox cho {source_title}, bỏ qua.")
        continue
# ... (Thêm code này vào SAU khối "if not infobox:") ...

    # === KHAI THÁC MẢNG "LINKS" ===
    # Lấy các liên kết từ tệp JSON gốc (file seed)
    links = item.get('links', [])
    if links:
        print(f" -> Đang xử lý {len(links)} liên kết chung cho {source_title}...")
        
        # Danh sách lọc bỏ (bạn cần bổ sung thêm)
        STOP_WORDS = {
            "Vua Việt Nam", "Nhà Nguyễn", "Đại Nam", "Việt Nam", "Huế",
            "1783", "1868", "1820", "1841", "14 tháng 2", "20 tháng 1",
            "chữ Hán", "Nho giáo", "Phật giáo"
        }

        for link_title in links:
            # Bỏ qua nếu là link yếu
            if link_title in STOP_WORDS or link_title.isnumeric():
                continue
            
            # 1. Thêm node mới (node được liên kết)
            nodes_set.add(link_title)
            
            # 2. Tạo cạnh (edge) yếu
            edge = {
                "sourceId": source_title,
                "targetId": link_title,
                "type": "LIEN_QUAN_TOI" # Cạnh này có ý nghĩa thấp
            }
            edges_list.append(edge)
            
# ... (Phần còn lại của file giữ nguyên) ...
    # === 3. XỬ LÝ INFOBOX ĐÃ THU THẬP ===
    for key, value in infobox.items():
        # Kiểm tra xem key này có trong bản đồ quan hệ của chúng ta không
        if key in RELATIONSHIP_MAP:
            relationship_type = RELATIONSHIP_MAP[key]
            target_title = value # Tên của node đích (ví dụ: "Gia Long")
            
            # Làm sạch tên node đích (loại bỏ chú thích, v.v.)
            target_title = re.sub(r'\[.*?\]', '', target_title).strip()
            
            if not target_title:
                continue

            # Thêm node đích vào danh sách
            nodes_set.add(target_title)
            
            # Tạo một cạnh (edge)
            edge = {
                "sourceId": source_title,
                "targetId": target_title,
                "type": relationship_type
            }
            edges_list.append(edge)

print("\nXử lý hoàn tất. Bắt đầu ghi ra tệp CSV...")

# === 4. GHI RA TỆP NODES.CSV ===
try:
    with open(NODES_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["nodeId", "label"]) # Viết header
        for node_id in nodes_set:
            # Tạm thời gán tất cả là nhãn "NhanVat" (Nhân Vật)
            # Sau này bạn có thể làm phức tạp hơn để phân loại
            writer.writerow([node_id, "NhanVat"]) 
    print(f"Đã ghi {len(nodes_set)} nodes vào {NODES_CSV}")

except IOError as e:
    print(f"Lỗi khi ghi tệp {NODES_CSV}: {e}")


# === 5. GHI RA TỆP EDGES.CSV ===
try:
    if edges_list:
        with open(EDGES_CSV, 'w', encoding='utf-8', newline='') as f:
            # Lấy keys từ phần tử đầu tiên để làm header
            writer = csv.DictWriter(f, fieldnames=edges_list[0].keys())
            writer.writeheader()
            writer.writerows(edges_list)
        print(f"Đã ghi {len(edges_list)} edges vào {EDGES_CSV}")
    else:
        print("Không có edges nào để ghi.")
        
except IOError as e:
    print(f"Lỗi khi ghi tệp {EDGES_CSV}: {e}")

print("\nHoàn thành! Bạn đã có 2 tệp 'nodes.csv' và 'edges.csv'.")