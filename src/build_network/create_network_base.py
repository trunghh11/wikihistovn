import requests
import json
import mwparserfromhell
from collections import deque
import time
import csv

# --- 1. CẤU HÌNH ---
SEED_NODES = [
    "Gia Long", "Minh Mạng", "Thiệu Trị", "Tự Đức", "Dục Đức", 
    "Hiệp Hòa", "Kiến Phúc", "Hàm Nghi", "Đồng Khánh", "Thành Thái", 
    "Duy Tân", "Khải Định", "Bảo Đại"
]

MAX_DEPTH = 2

HEADERS = {
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Project for university; contact: student@vnu.edu.vn)"
}

OUTPUT_NODES_FILE = "data/processed/nodes_metadata.json"
OUTPUT_EDGES_FILE = "data/processed/initial_edges.csv"

# --- 2. TỪ KHÓA LỌC ---
VALID_STARTS = {
    # Họ phổ biến
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", 
    "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Trịnh", "Đinh", "Lâm",
    "Mai", "Đào", "Cao", "Tôn", "Tống", "Quang", "Trương", "Ngô", "Lương",
    # Niên hiệu / Tước hiệu
    "Gia", "Minh", "Thiệu", "Tự", "Kiến", "Hàm", "Đồng", "Thành", "Duy", "Khải", "Bảo", "Dục", "Hiệp",
    "Hoàng", "Thái", "Nam", "Bắc"
}

PERSON_TEMPLATE_KEYWORDS = [
    "nhân vật", "person", "người", "tiểu sử",
    "vua", "hoàng gia", "hoàng đế", "hoàng hậu", "phi tần",
    "lãnh đạo", "quân sự", "tướng", "quan", "thần", "chức vụ"
]

class DirectGraphCrawler:
    def __init__(self):
        self.visited = set()         
        self.valid_nodes = {}        
        self.potential_edges = []    
        self.queue = deque()         

    def is_capitalized(self, title):
        if not title: return False
        return title[0].isupper()

    def has_valid_start(self, title):
        if not title: return False
        first_word = title.split()[0]
        return first_word in VALID_STARTS

    def fetch_wikitext(self, title):
        url = "https://vi.wikipedia.org/w/api.php"
        params = {
            "action": "query", "prop": "revisions", "titles": title,
            "rvprop": "content|ids", "rvslots": "main", "format": "json", "formatversion": 2
        }
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10).json()
            if 'query' in resp and 'pages' in resp['query']:
                page = resp['query']['pages'][0]
                if 'missing' in page: return None
                if 'revisions' not in page: return None
                
                return {
                    "page_id": page['pageid'],
                    "title": page['title'],
                    "wikitext": page['revisions'][0]['slots']['main']['content']
                }
        except Exception as e:
            print(f"❌ Lỗi fetch '{title}': {e}")
            return None

    def validate_and_parse_node(self, raw_data):
        title = raw_data['title']
        wikitext = raw_data['wikitext']
        wikicode = mwparserfromhell.parse(wikitext)
        
        # 1. Trích xuất Infobox (nếu có)
        # Lấy bất kỳ template nào có chữ 'infobox', 'thông tin', 'hộp'
        infoboxes = wikicode.filter_templates(
            matches=lambda t: 'infobox' in t.name.lower() or 'thông tin' in t.name.lower() or 'hộp' in t.name.lower()
        )
        
        extracted_infobox = {}
        is_person_template = False
        
        if infoboxes:
            target_template = infoboxes[0]
            print(f"   > Tìm thấy Infobox trong '{title}': {target_template.name}")
            template_name = target_template.name.strip().lower()
            
            # Kiểm tra xem template này có phải chuyên cho Người không
            if any(kw in template_name for kw in PERSON_TEMPLATE_KEYWORDS):
                is_person_template = True
            
            # Parse params
            for param in target_template.params:
                try:
                    key = str(param.name).strip()
                    val = str(param.value).strip()
                    extracted_infobox[key] = val
                except: pass

        # --- ĐIỀU KIỆN CHẤP THUẬN (CẬP NHẬT) ---
        is_seed = title in SEED_NODES
        has_surname = self.has_valid_start(title)
        
        # Logic mới: (Đúng Họ) HOẶC (Template là Person)
        # Lưu ý: Nếu đúng họ mà không có infobox (extracted_infobox rỗng) ta vẫn lấy, 
        # vì có thể bài viết chưa chuẩn hóa template nhưng vẫn là người.
        is_valid = is_seed or (has_surname or is_person_template)

        if is_valid:
            raw_links = [str(l.title) for l in wikicode.filter_wikilinks()]
            return {
                "page_id": raw_data['page_id'],
                "title": title,
                "infobox": extracted_infobox,
                "raw_links": raw_links
            }
        else:
            return None

    def run(self):
        # 1. Khởi tạo
        for seed in SEED_NODES:
            self.queue.append((seed, 0))

        print(f"--- BẮT ĐẦU CRAWL TỪ {len(SEED_NODES)} NODE HẠT GIỐNG ---")

        while self.queue:
            current_title, depth = self.queue.popleft()
            
            if depth > MAX_DEPTH: continue
            if current_title in self.visited: continue
            
            print(f"Depth {depth}: Đang xử lý '{current_title}'...")
            self.visited.add(current_title)

            # A. Fetch
            raw_data = self.fetch_wikitext(current_title)
            if not raw_data: continue

            # B. Validate & Parse
            parsed_node = self.validate_and_parse_node(raw_data)
            
            if not parsed_node: 
                continue

            # C. Lưu Node
            self.valid_nodes[current_title] = {
                "page_id": parsed_node['page_id'],
                "title": parsed_node['title'],
                "infobox": parsed_node['infobox']
            }

            if depth == MAX_DEPTH: continue

            # D. Xử lý Links
            raw_links = parsed_node['raw_links']
            potential_links = set([l for l in raw_links if self.is_capitalized(l)])
            
            for link in potential_links:
                if link == current_title: continue

                # Chỉ đi vào link nếu nó có tiềm năng (Đúng họ/niên hiệu)
                if self.has_valid_start(link):
                    self.potential_edges.append({
                        "source": current_title,
                        "target": link,
                        "type": "LIÊN_KẾT_TỚI"
                    })

                    if link not in self.visited:
                        self.queue.append((link, depth + 1))
            
            time.sleep(0.1)

        self.save_data()

    def save_data(self):
        print("\n--- ĐANG HOÀN TẤT DỮ LIỆU ---")
        
        # Lưu Nodes
        final_nodes_list = list(self.valid_nodes.values())
        with open(OUTPUT_NODES_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_nodes_list, f, ensure_ascii=False, indent=4)
        
        # Lưu Edges (Lọc lần cuối)
        final_edges = []
        rejected_count = 0
        valid_titles = set(self.valid_nodes.keys())
        seen_edges = set()

        for edge in self.potential_edges:
            src = edge['source']
            tgt = edge['target']
            
            # Chỉ giữ cạnh nếu Target thực sự tồn tại (đã được fetch và validate)
            if src in valid_titles and tgt in valid_titles:
                edge_tuple = (src, tgt)
                if edge_tuple not in seen_edges:
                    final_edges.append(edge)
                    seen_edges.add(edge_tuple)
            else:
                rejected_count += 1
        
        with open(OUTPUT_EDGES_FILE, 'w', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["source", "target", "type"])
            writer.writeheader()
            writer.writerows(final_edges)
            
        print(f"✅ Đã lưu {len(final_nodes_list)} Nodes vào '{OUTPUT_NODES_FILE}'")
        print(f"✅ Đã lưu {len(final_edges)} Edges vào '{OUTPUT_EDGES_FILE}'")
        print(f"   (Đã loại bỏ {rejected_count} cạnh rác)")

if __name__ == "__main__":
    crawler = DirectGraphCrawler()
    crawler.run()