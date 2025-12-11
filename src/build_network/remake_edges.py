import requests
import json
import csv
import re
import time
from collections import defaultdict
from tqdm import tqdm

# --- CẤU HÌNH ---
INPUT_EDGES_FILE = "data/processed/initial_edges.csv"
OUTPUT_FINAL_FILE = "data/processed/final_relations.csv"

HEADERS = {
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Relation Refiner; contact: 22024527@vnu.edu.vn)"
}

# --- 1. QUY TẮC TỪ KHÓA ---
RELATION_RULES = {
    # =========================================================================
    # NHÓM 1: HUYẾT THỐNG & GIA ĐÌNH (Family & Lineage)
    # =========================================================================
    
    "LÀ_CHA_CỦA": [
        "cha là", "cha ruột", "thân phụ", "phụ hoàng", "phụ thân", "người cha",
        "sinh hạ", "sinh ra", "sinh được", "có con là", 
        "hoàng tử là", "trưởng nam là", "con trai là", "đích tử là", "con là",
        "dưỡng phụ", "cha nuôi" # Mở rộng cha nuôi
    ],
    
    "LÀ_MẸ_CỦA": [
        "mẹ là", "mẹ ruột", "thân mẫu", "mẫu thân", "mẫu hậu", 
        "từ dụ", "sinh mẫu", "bà sinh", "đích mẫu", "thứ mẫu",
        "dưỡng mẫu", "mẹ nuôi"
    ],
    
    "LÀ_CON_CỦA": [
        "con của", "con trai của", "con gái của", "người con của",
        "nữ nhi của", "trưởng nữ của", "trưởng nam của", "thứ nam của",
        "thế tử của", "hoàng nam của", "hoàng tử của", "hoàng nữ của", "cha là", "mẹ là",
        "công chúa của", "nghĩa tử của", "con nuôi của", "hậu duệ của"
    ],
    
    "PHỐI_NGẪU_VỚI": [
        # Vợ/Chồng chính thức
        "vợ của", "chồng của", "phu nhân của", "phu quân của", "chính thất", "kết hôn với", "cưới", "gả cho", "sánh duyên", "vợ là", "chồng là","vợ", "chồng",
        # Từ ngữ cung đình (Vua -> Vợ)
        "hoàng hậu", "phi tần", "quý phi", "chiêu nghi", "tiệp dư", "tài nhân", "cung tần", 
        "sắc phong làm phi", "nạp làm phi", "tuyển vào cung", "sủng ái",
        # Vợ -> Chồng (Vua)
        "phò mã", "làm rể"
    ],
    
    "LÀ_ANH_CHỊ_EM_CỦA": [
        "anh trai", "em trai", "chị gái", "em gái", 
        "anh ruột", "em ruột", "chị ruột", 
        "huynh đệ", "tỷ muội", "bào huynh", "bào đệ", "bào tỷ", "hoàng huynh", "hoàng đệ"
    ],
    
    "LÀ_ÔNG_BÀ_CỦA": [
        "ông nội", "bà nội", "ông ngoại", "bà ngoại", "tổ phụ", "tổ mẫu", "cháu của"
    ],

    # =========================================================================
    # NHÓM 2: CHÍNH TRỊ & KẾ VỊ (Politics & Succession)
    # =========================================================================

    "KẾ_NHIỆM_CỦA": [
        "kế nhiệm", "lên ngôi thay", "nối ngôi", "kế vị", "kế tục", "kế lập", "thừa kế", 
        "thừa kế ngai vàng", "nhận thiền", "đăng quang sau", "soán ngôi", "tiếp nối triều đại"
    ],
    
    "TIỀN_NHIỆM_CỦA": [
        "tiền nhiệm", "vua trước là", "thái thượng hoàng", "nhường ngôi", "truyền ngôi"
    ],
    
    "NHIẾP_CHÍNH_CHO": [
        "nhiếp chính", "phụ chính", "giám quốc", "quyền nhiếp chính", 
        "buông rèm nhiếp chính", "phò tá vua nhỏ", "cố mệnh đại thần"
    ],

    # =========================================================================
    # NHÓM 3: PHỤC VỤ & HÀNH CHÍNH (Service & Administration) - MỚI
    # =========================================================================

    "PHỤC_VỤ_CHO": [
        # Cấp dưới -> Cấp trên/Vua
        "phục vụ", "làm quan cho", "làm quan dưới triều", "theo phò", "đầu quân cho",
        "dưới quyền", "thuộc hạ của", "cận thần của", "tâm phúc của",
        "dâng sớ lên", "chịu lệnh của", "tuân lệnh", "bề tôi của"
    ],

    "ĐƯỢC_PHỤC_VỤ_BỞI": [ # Hoặc LÀ_CẤP_TRÊN_CỦA
        # Cấp trên/Vua -> Cấp dưới
        "trọng dụng", "tin dùng", "tin cẩn", "giao phó", 
        "sai đi", "điều đi", "triệu kiến", "nghe lời tấu",
        "các tướng", "các quan"
    ],

    "ĐƯỢC_BỔ_NHIỆM_BỞI": [
        "bổ nhiệm", "phong chức", "sắc phong", "thăng chức", "cử làm", 
        "trao chức", "ban tước", "nhận chức từ"
    ],
    
    "BỊ_PHẾ_TRUẤT_BỞI": [
        "phế truất", "cách chức", "bãi miễn", "ép thoái vị", 
        "giáng chức", "lột chức", "thu hồi ấn tín"
    ],

    "LÀ_THẦY_CỦA": [
        "thầy dạy", "sư phụ", "tôn làm thầy", "phụ đạo", "dạy học cho", "giảng sách cho", "tế tửu"
    ],

    "LÀ_HỌC_TRÒ_CỦA": [
        "học trò", "môn sinh", "đệ tử", "theo học", "thụ giáo"
    ],

    # =========================================================================
    # NHÓM 4: QUÂN SỰ & ĐỐI NGOẠI (Military & Conflict)
    # =========================================================================

    "ĐỐI_THỦ_CỦA": [
        "đánh bại", "tiêu diệt", "chống lại", "khởi nghĩa chống", "tấn công", 
        "giao chiến với", "đối đầu", "trấn áp", "dẹp loạn", "bắt giam", 
        "xử tử", "giết", "truy sát", "kẻ thù", "phản loạn", "thảo phạt"
    ],
    
    "ĐỒNG_MINH_VỚI": [
        "liên minh", "hợp tác", "cùng với", "giúp đỡ", "viện trợ", 
        "cấu kết", "thông gia", "hòa ước", "liên kết"
    ],
    
    "CHỈ_HUY_CỦA": [ # Trong trận chiến cụ thể
        "chỉ huy", "thống lĩnh", "lãnh đạo quân", "cầm đầu", "tướng lĩnh của"
    ]
}

# --- 2. QUY TẮC ĐẢO CHIỀU (INVERSE) ---
INVERSE_MAPPING = {
    # Huyết thống
    "LÀ_CHA_CỦA": "LÀ_CON_CỦA",
    "LÀ_MẸ_CỦA": "LÀ_CON_CỦA",
    "LÀ_CON_CỦA": "LÀ_CHA_HOẶC_MẸ_CỦA", # (Cần logic check giới tính để refine sau)
    "LÀ_ÔNG_BÀ_CỦA": "LÀ_CHÁU_CỦA",      # (Tự thêm quan hệ Cháu nếu muốn)
    "PHỐI_NGẪU_VỚI": "PHỐI_NGẪU_VỚI",    # Đối xứng
    "LÀ_ANH_CHỊ_EM_CỦA": "LÀ_ANH_CHỊ_EM_CỦA", # Đối xứng

    # Kế vị
    "KẾ_NHIỆM_CỦA": "TIỀN_NHIỆM_CỦA",
    "TIỀN_NHIỆM_CỦA": "KẾ_NHIỆM_CỦA",
    "NHIẾP_CHÍNH_CHO": "ĐƯỢC_NHIẾP_CHÍNH_BỞI",

    # Vua - Tôi / Cấp trên - Cấp dưới
    "PHỤC_VỤ_CHO": "ĐƯỢC_PHỤC_VỤ_BỞI",
    "ĐƯỢC_PHỤC_VỤ_BỞI": "PHỤC_VỤ_CHO",
    "ĐƯỢC_BỔ_NHIỆM_BỞI": "ĐÃ_BỔ_NHIỆM",
    "BỊ_PHẾ_TRUẤT_BỞI": "ĐÃ_PHẾ_TRUẤT",
    "CHỈ_HUY_CỦA": "DƯỚI_QUYỀN_CHỈ_HUY_CỦA",

    # Giáo dục
    "LÀ_THẦY_CỦA": "LÀ_HỌC_TRÒ_CỦA",
    "LÀ_HỌC_TRÒ_CỦA": "LÀ_THẦY_CỦA",

    # Đối ngoại
    "ĐỐI_THỦ_CỦA": "ĐỐI_THỦ_CỦA",        # Đối xứng
    "ĐỒNG_MINH_VỚI": "ĐỒNG_MINH_VỚI",    # Đối xứng

}

RELATION_WEIGHTS = {
    # --- MỨC 1: LIÊN KẾT CƠ BẢN (Yếu nhất) ---
    "LIÊN_KẾT_TỚI": 1,

    # --- MỨC 2: QUAN HỆ VĂN HÓA / SỰ KIỆN ---
    "THAM_GIA_SỰ_KIỆN": 2,
    "ĐƯỢC_THỜ_TẠI": 2,
    "SÁNG_TÁC": 3,
    "ĐỒNG_MINH_VỚI": 3,

    # --- MỨC 3: QUAN HỆ CÔNG VIỆC / HIERARCHY ---
    "PHỤC_VỤ_CHO": 4,
    "ĐƯỢC_PHỤC_VỤ_BỞI": 4,
    "CHỈ_HUY_CỦA": 5,
    "LÀ_THẦY_CỦA": 5,
    "LÀ_HỌC_TRÒ_CỦA": 5,
    "ĐỐI_THỦ_CỦA": 5,

    # --- MỨC 4: QUAN HỆ CHÍNH TRỊ QUAN TRỌNG ---
    "ĐƯỢC_BỔ_NHIỆM_BỞI": 6,
    "BỊ_PHẾ_TRUẤT_BỞI": 6,
    "NHIẾP_CHÍNH_CHO": 6,
    "LÀ_ANH_CHỊ_EM_CỦA": 6,
    "LÀ_ÔNG_BÀ_CỦA": 6,

    # --- MỨC 5: KẾ VỊ & HUYẾT THỐNG (Mạnh nhất) ---
    # Ưu tiên cao nhất để xây dựng cây gia phả chính xác
    "KẾ_NHIỆM_CỦA": 7,
    "TIỀN_NHIỆM_CỦA": 7,
    "PHỐI_NGẪU_VỚI": 8,
    "LÀ_CHA_CỦA": 8,
    "LÀ_MẸ_CỦA": 8,
    "LÀ_CON_CỦA": 8
}

class RelationRefiner:
    def __init__(self):
        self.edges_map = defaultdict(list)
        self.final_edges = []
        # Key để khử trùng lặp: (Source, Target, Type)
        # Cho phép A->B (Cha) và A->B (Tiền nhiệm) cùng tồn tại vì Type khác nhau
        self.existing_edges_set = set()
        self.skipped_no_mention = 0 

    def fetch_plaintext(self, title):
        url = "https://vi.wikipedia.org/w/api.php"
        params = { "action": "query", "format": "json", "titles": title, "prop": "extracts", "explaintext": 1 }
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10).json()
            page = next(iter(resp['query']['pages'].values()))
            return page.get("extract", "")
        except: return ""

    def split_sentences(self, text):
        return re.split(r'(?<=[.?!])\s+(?=[A-ZĐ])', text)

    # def refine_relation_direction(self, found_type, context):
    #     context_lower = context.lower()
    #     if found_type == "LÀ_CHA_CỦA":
    #         if any(kw in context_lower for kw in ["con của", "sinh bởi", "nữ nhi", "trưởng nam", "đích tử"]): return "LÀ_CON_CỦA"
    #     elif found_type == "LÀ_CON_CỦA":
    #         if any(kw in context_lower for kw in ["cha của", "mẹ của", "phụ thân", "mẫu thân"]): return "LÀ_CHA_CỦA"
    #     elif found_type == "KẾ_NHIỆM_CỦA":
    #         if "tiền nhiệm" in context_lower or "vua trước" in context_lower: return "TIỀN_NHIỆM_CỦA"
    #     elif found_type == "TIỀN_NHIỆM_CỦA":
    #         if "kế nhiệm" in context_lower or "nối ngôi" in context_lower: return "KẾ_NHIỆM_CỦA"
    #     return found_type

    def analyze_sentence_context(self, context_sentence):
        context_lower = context_sentence.lower()
        found_types = []
        for rel_type, keywords in RELATION_RULES.items():
            for kw in keywords:
                if kw in context_lower:
                    found_types.append(rel_type)
                    break 
        
        if not found_types: return "LIÊN_KẾT_TỚI"
        
        # Lấy loại có trọng số cao nhất trong câu đó
        best_type = max(found_types, key=lambda t: RELATION_WEIGHTS.get(t, 1))
        return best_type

    def add_edge(self, source, target, rel_type, evidence):
        """Chỉ thêm nếu bộ 3 (Source, Target, Type) chưa có"""
        edge_signature = (source, target, rel_type)
        if edge_signature not in self.existing_edges_set:
            self.final_edges.append({
                "source": source,
                "target": target,
                "type": rel_type,
                "evidence": evidence
            })
            self.existing_edges_set.add(edge_signature)

    def generate_inverse_edges(self):
        print("\n--- ĐANG SINH QUAN HỆ NGƯỢC (ĐA CHIỀU) ---")
        current_edges = list(self.final_edges)
        count_generated = 0

        for edge in current_edges:
            src = edge['source']
            tgt = edge['target']
            rel_type = edge['type']
            original_evidence = edge['evidence']

            inverse_type = INVERSE_MAPPING.get(rel_type)
            if inverse_type:
                if inverse_type == "LÀ_CHA_HOẶC_MẸ_CỦA": inverse_type = "LÀ_CHA_CỦA"

                inverse_evidence = f"[SUY LUẬN] Từ việc {src} là {rel_type} của {tgt}."
                
                # Logic này tự động support đa quan hệ ngược
                # Nếu có A->B (Cha) => Sinh B->A (Con)
                # Nếu có A->B (Tiền nhiệm) => Sinh B->A (Kế nhiệm)
                initial_len = len(self.existing_edges_set)
                self.add_edge(tgt, src, inverse_type, inverse_evidence)
                
                if len(self.existing_edges_set) > initial_len:
                    count_generated += 1

        print(f"✅ Đã suy luận thêm {count_generated} quan hệ mới!")

    def run(self):
        print("--- ĐANG ĐỌC DỮ LIỆU ---")
        try:
            with open(INPUT_EDGES_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.edges_map[row['source']].append(row['target'])
        except FileNotFoundError: return

        print(f"🔹 Phân tích ngữ cảnh cho {len(self.edges_map)} nhân vật...")
        
        for source, targets in tqdm(self.edges_map.items()):
            content = self.fetch_plaintext(source)
            if not content:
                # Nếu không có content, đành chấp nhận LIÊN_KẾT_TỚI
                for t in targets: self.add_edge(source, t, "LIÊN_KẾT_TỚI", "")
                continue

            sentences = self.split_sentences(content)
            
            for target in targets:
                target_mentions = [s for s in sentences if target in s]
                
                if not target_mentions:
                    self.skipped_no_mention += 1
                    continue

                # 1. Thu thập tất cả các Votes
                relation_scores = defaultdict(int)
                relation_evidence = defaultdict(list)
                
                for sent in target_mentions:
                    detected_rel = self.analyze_sentence_context(sent)
                    weight = RELATION_WEIGHTS.get(detected_rel, 1)
                    relation_scores[detected_rel] += weight
                    relation_evidence[detected_rel].append(sent)
                
                # --- LOGIC MỚI: CHỌN TOP 2 QUAN HỆ TỐT NHẤT ---
                
                # A. Nếu có bất kỳ quan hệ cụ thể nào (khác LIÊN_KẾT_TỚI), 
                # hãy loại bỏ LIÊN_KẾT_TỚI để đỡ loãng.
                if len(relation_scores) > 1 and "LIÊN_KẾT_TỚI" in relation_scores:
                    del relation_scores["LIÊN_KẾT_TỚI"]

                # B. Sắp xếp các quan hệ theo điểm số giảm dần
                # sorted_rels trả về list các tuple: [('LÀ_CHA_CỦA', 15), ('TIỀN_NHIỆM_CỦA', 10), ...]
                sorted_rels = sorted(relation_scores.items(), key=lambda item: item[1], reverse=True)

                # C. Chọn Top 2 (Nếu chỉ có 1 thì lấy 1)
                top_relations = sorted_rels[:2]

                # D. Tạo cạnh cho các quan hệ này
                for rel_type, score in top_relations:
                    # Lấy bằng chứng (chọn câu đầu tiên tìm thấy của loại đó)
                    evidence_text = relation_evidence[rel_type][0].replace('\n', ' ').strip()
                    if len(evidence_text) > 200: evidence_text = evidence_text[:200] + "..."
                    
                    self.add_edge(source, target, rel_type, evidence_text)
            
            time.sleep(0.05)

        # Kích hoạt suy luận ngược
        self.generate_inverse_edges()
        self.save_data()

    def save_data(self):
        print(f"\n--- ĐANG LƯU KẾT QUẢ RA {OUTPUT_FINAL_FILE} ---")
        with open(OUTPUT_FINAL_FILE, 'w', encoding='utf-8') as f:
            fieldnames = ["source", "target", "type", "evidence"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.final_edges)
        
        print(f"✅ Hoàn tất! Tổng cộng {len(self.final_edges)} quan hệ đa chiều.")
        print(f"ℹ️ Đã bỏ qua {self.skipped_no_mention} edges vì không có mention (mention = 0).")

if __name__ == "__main__":
    refiner = RelationRefiner()
    refiner.run()