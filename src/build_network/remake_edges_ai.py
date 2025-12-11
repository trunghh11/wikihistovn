import json
import csv
import re
import time
from collections import defaultdict
from tqdm import tqdm
import requests


# --- ⚠️ CHÚ Ý: CẦN CÀI ĐẶT/CÓ MODULE PUTER ---
# Giả định bạn đã có module 'puter' chứa class PuterAI như bạn cung cấp
# Nếu PuterAI nằm ở file khác, hãy import nó vào đây
try:
    from puter import PuterAI, PuterAuthError, PuterAPIError
except ImportError:
    print("⚠️ Không tìm thấy module 'puter'. Đảm bảo bạn đã cài đặt hoặc có file puter.py")
    # Tạo class giả để code không crash khi editor check lỗi
    class PuterAI: pass 
    class PuterAuthError(Exception): pass
    class PuterAPIError(Exception): pass

# --- 1. CẤU HÌNH ---
INPUT_EDGES_FILE = "data/processed/initial_edges_test.csv"
OUTPUT_FINAL_FILE = "data/processed/final_relations_test.csv"

# ⚠️ ĐIỀN THÔNG TIN PUTER CỦA BẠN VÀO ĐÂY
PUTER_USERNAME = "wise_river_2593"
PUTER_PASSWORD = "Trung29#10"

HEADERS = {
    "User-Agent": "VietnameseHistoryNetwork/1.0 (Relation Refiner; contact: student@vnu.edu.vn)"
}

# --- 2. QUY TẮC TỪ KHÓA (RULE-BASED) ---
RELATION_RULES = {
    # Huyết thống
    "LÀ_CHA_CỦA": ["cha là", "cha ruột", "thân phụ", "phụ hoàng", "sinh hạ", "sinh ra", "có con là", "hoàng tử là", "trưởng nam là", "con trai là", "dưỡng phụ"],
    "LÀ_MẸ_CỦA": ["mẹ là", "mẹ ruột", "mẫu thân", "mẫu hậu", "từ dụ", "sinh mẫu", "bà sinh", "đích mẫu"],
    "LÀ_CON_CỦA": ["con của", "nữ nhi của", "trưởng nữ của", "con gái của", "thế tử của", "hoàng nam của", "hoàng tử của", "nghĩa tử"],
    "PHỐI_NGẪU_VỚI": ["vợ", "chồng", "phu nhân", "chính thất", "hoàng hậu", "phi tần", "kết hôn", "cưới", "sắc phong làm phi", "phò mã"],
    "LÀ_ANH_CHỊ_EM_CỦA": ["anh trai", "em trai", "chị gái", "em gái", "anh ruột", "em ruột", "huynh đệ", "tỷ muội"],
    "LÀ_ÔNG_BÀ_CỦA": ["ông nội", "bà nội", "tổ phụ", "tổ mẫu"],

    # Chính trị & Quân sự
    "KẾ_NHIỆM_CỦA": ["kế nhiệm", "lên ngôi thay", "nối ngôi", "kế vị"],
    "TIỀN_NHIỆM_CỦA": ["tiền nhiệm", "vua trước là"],
    "NHIẾP_CHÍNH_CHO": ["nhiếp chính", "phụ chính", "giám quốc"],
    "LÀ_THẦY_CỦA": ["thầy dạy", "sư phụ", "tôn làm thầy", "phụ đạo"],
    "ĐỐI_THỦ_CỦA": ["đánh bại", "tiêu diệt", "chống lại", "khởi nghĩa chống", "tấn công", "giao chiến", "bắt giam", "xử tử", "giết"],
    "ĐỒNG_MINH_VỚI": ["liên minh", "hợp tác", "cùng với", "phò tá", "giúp đỡ"],
    
    # Cấp bậc
    "PHỤC_VỤ_CHO": ["phục vụ", "làm quan cho", "dưới quyền", "bề tôi", "theo phò"],
    "ĐƯỢC_PHỤC_VỤ_BỞI": ["trọng dụng", "tin dùng", "sai đi"],
    "CHỈ_HUY_CỦA": ["chỉ huy", "thống lĩnh"],
    
    # Hành chính
    "ĐƯỢC_BỔ_NHIỆM_BỞI": ["bổ nhiệm", "phong chức", "sắc phong", "thăng chức", "cử đi"],
    "BỊ_PHẾ_TRUẤT_BỞI": ["phế truất", "cách chức", "bãi miễn", "ép thoái vị"],
    "THAM_GIA_SỰ_KIỆN": ["tham gia", "có mặt tại", "chỉ huy trận"]
}

# Danh sách Key hợp lệ để AI chọn
VALID_RELATION_KEYS = list(RELATION_RULES.keys()) + ["LIÊN_KẾT_TỚI"]

INVERSE_MAPPING = {
    "LÀ_CHA_CỦA": "LÀ_CON_CỦA", "LÀ_MẸ_CỦA": "LÀ_CON_CỦA", "LÀ_CON_CỦA": "LÀ_CHA_HOẶC_MẸ_CỦA", 
    "PHỐI_NGẪU_VỚI": "PHỐI_NGẪU_VỚI", "LÀ_ANH_CHỊ_EM_CỦA": "LÀ_ANH_CHỊ_EM_CỦA", 
    "KẾ_NHIỆM_CỦA": "TIỀN_NHIỆM_CỦA", "TIỀN_NHIỆM_CỦA": "KẾ_NHIỆM_CỦA",
    "LÀ_THẦY_CỦA": "LÀ_HỌC_TRÒ_CỦA", "NHIẾP_CHÍNH_CHO": "ĐƯỢC_NHIẾP_CHÍNH_BỞI",
    "ĐỐI_THỦ_CỦA": "ĐỐI_THỦ_CỦA", "ĐỒNG_MINH_VỚI": "ĐỒNG_MINH_VỚI",
    "PHỤC_VỤ_CHO": "ĐƯỢC_PHỤC_VỤ_BỞI", "ĐƯỢC_PHỤC_VỤ_BỞI": "PHỤC_VỤ_CHO",
    "ĐƯỢC_BỔ_NHIỆM_BỞI": "ĐÃ_BỔ_NHIỆM", "BỊ_PHẾ_TRUẤT_BỞI": "ĐÃ_PHẾ_TRUẤT",
    "CHỈ_HUY_CỦA": "DƯỚI_QUYỀN_CHỈ_HUY_CỦA"
}

RELATION_WEIGHTS = {
    "LIÊN_KẾT_TỚI": 1,
    "THAM_GIA_SỰ_KIỆN": 2, "ĐƯỢC_THỜ_TẠI": 2, "SÁNG_TÁC": 3, "ĐỒNG_MINH_VỚI": 3,
    "PHỤC_VỤ_CHO": 4, "ĐƯỢC_PHỤC_VỤ_BỞI": 4, "CHỈ_HUY_CỦA": 5, "LÀ_THẦY_CỦA": 5, "LÀ_HỌC_TRÒ_CỦA": 5, "ĐỐI_THỦ_CỦA": 5,
    "ĐƯỢC_BỔ_NHIỆM_BỞI": 6, "BỊ_PHẾ_TRUẤT_BỞI": 6, "NHIẾP_CHÍNH_CHO": 6, "LÀ_ANH_CHỊ_EM_CỦA": 6, "LÀ_ÔNG_BÀ_CỦA": 6,
    "KẾ_NHIỆM_CỦA": 7, "TIỀN_NHIỆM_CỦA": 7,
    "PHỐI_NGẪU_VỚI": 8, "LÀ_CHA_CỦA": 8, "LÀ_MẸ_CỦA": 8, "LÀ_CON_CỦA": 8
}

import requests # requests cho wiki api

class RelationRefiner:
    def __init__(self):
        self.edges_map = defaultdict(list)
        self.final_edges = []
        self.existing_edges_set = set()
        
        # --- TÍCH HỢP PUTER AI ---
        print("--- ĐANG KẾT NỐI PUTER AI ---")
        self.has_ai = True
        self.puter_ai = None
        
        # try:
        #     # 1. Khởi tạo Client
        #     self.puter_ai = PuterAI(username=PUTER_USERNAME, password=PUTER_PASSWORD)
            
        #     # 2. Đăng nhập
        #     if self.puter_ai.login():
        #         print("✅ Puter Login successful! AI Agent sẵn sàng.")
        #         self.has_ai = True
        #     else:
        #         print("⚠️ Puter Login failed. Vui lòng kiểm tra user/pass.")
        
        # except NameError:
        #      print("❌ Lỗi: Chưa import class PuterAI. Hãy đảm bảo bạn có file thư viện.")
        # except PuterAuthError as e:
        #     print(f"❌ Authentication Error: {e}")
        # except Exception as e:
        #     print(f"⚠️ Không thể kết nối Puter ({e}). Sẽ chạy chế độ Offline (Rule-only).")

    def ask_ai_agent(self, source, target, context):
        """
        Dùng Puter AI để xác định quan hệ khi Rule bó tay.
        """
        if not self.has_ai or not context: return "LIÊN_KẾT_TỚI"
        
        prompt = f"""
        Bạn là chuyên gia lịch sử Việt Nam. Dựa vào văn bản sau, hãy xác định quan hệ giữa:
        - A: {source}
        - B: {target}
        - Văn bản: "{context}"
        
        Hãy chọn ĐÚNG 1 loại trong danh sách này:
        {json.dumps(VALID_RELATION_KEYS, ensure_ascii=False)}
        
        Nếu văn bản thể hiện quan hệ cha-con, vợ-chồng, vua-tôi... hãy chọn key tương ứng.
        Nếu không rõ ràng, trả về: LIÊN_KẾT_TỚI
        CHỈ TRẢ VỀ MÃ QUAN HỆ, KHÔNG GIẢI THÍCH.
        """
        
        try:
            # 3. Gọi Chat
            # self.puter_ai.set_model("gemini-2.5-flash")
            print("--- GỬI YÊU CẦU ĐẾN AI ---")
            print(f"   > Prompt: {prompt[:100]}...")
            response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": "Bearer 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",
                    "Content-Type": "application/json",
                },
                data=json.dumps({
                    "model": "google/gemini-2.0-flash-exp:free",
                    "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                    ]
                })
            )
            print(response.status_code)
            print(response.json())
            print(response.choices[0].message.content)
            # Xử lý kết quả (Clean text)
            result = str(response).strip()
            result = re.sub(r'[^A-Z_À-Ỹ]', '', result) # Chỉ giữ lại ký tự chữ hoa và gạch dưới
            
            if result in VALID_RELATION_KEYS:
                return result
            else:
                return "LIÊN_KẾT_TỚI"
        
        except PuterAPIError as e:
            print(f"   [Puter API Error]: {e}")
            return "LIÊN_KẾT_TỚI"
        except Exception:
            return "LIÊN_KẾT_TỚI"

    # --- CÁC HÀM XỬ LÝ TEXT (GIỮ NGUYÊN) ---
    def fetch_plaintext(self, title):
        url = "https://vi.wikipedia.org/w/api.php"
        params = { "action": "query", "format": "json", "titles": title, "prop": "extracts", "explaintext": 1}
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10).json()
            page = next(iter(resp['query']['pages'].values()))
            return page.get("extract", "")
        except: return ""

    def split_sentences(self, text):
        return re.split(r'(?<=[.?!])\s+(?=[A-ZĐ])', text)

    def refine_relation_direction(self, found_type, context):
        context_lower = context.lower()
        if found_type == "LÀ_CHA_CỦA":
            if any(kw in context_lower for kw in ["con của", "sinh bởi", "nữ nhi", "trưởng nam", "đích tử"]): return "LÀ_CON_CỦA"
        elif found_type == "LÀ_CON_CỦA":
            if any(kw in context_lower for kw in ["cha của", "mẹ của", "phụ thân", "mẫu thân"]): return "LÀ_CHA_CỦA"
        return found_type

    def analyze_sentence_context(self, context_sentence):
        context_lower = context_sentence.lower()
        found_types = []
        for rel_type, keywords in RELATION_RULES.items():
            for kw in keywords:
                if kw in context_lower:
                    found_types.append(rel_type)
                    break 
        
        if not found_types: return "LIÊN_KẾT_TỚI"
        
        best_type = max(found_types, key=lambda t: RELATION_WEIGHTS.get(t, 1))
        return self.refine_relation_direction(best_type, context_sentence)

    def add_edge(self, source, target, rel_type, evidence):
        edge_signature = (source, target, rel_type)
        if edge_signature not in self.existing_edges_set:
            self.final_edges.append({
                "source": source, "target": target, "type": rel_type, "evidence": evidence
            })
            self.existing_edges_set.add(edge_signature)

    def generate_inverse_edges(self):
        print("\n--- ĐANG SINH QUAN HỆ NGƯỢC ---")
        current_edges = list(self.final_edges)
        count = 0
        for edge in current_edges:
            src, tgt, rel, evi = edge['source'], edge['target'], edge['type'], edge['evidence']
            inv_type = INVERSE_MAPPING.get(rel)
            if inv_type:
                if inv_type == "LÀ_CHA_HOẶC_MẸ_CỦA": inv_type = "LÀ_CHA_CỦA"
                inv_evi = f"[SUY LUẬN] Từ việc {src} là {rel} của {tgt}."
                
                initial_len = len(self.existing_edges_set)
                self.add_edge(tgt, src, inv_type, inv_evi)
                if len(self.existing_edges_set) > initial_len: count += 1
        print(f"✅ Đã thêm {count} quan hệ ngược.")

    def run(self):
        print("--- ĐANG ĐỌC DỮ LIỆU ---")
        try:
            with open(INPUT_EDGES_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader: self.edges_map[row['source']].append(row['target'])
        except FileNotFoundError: return

        print(f"🔹 Xử lý {len(self.edges_map)} nhân vật...")
        
        for source, targets in tqdm(self.edges_map.items()):
            content = self.fetch_plaintext(source)  # In 500 ký tự đầu để tham khảo
            if not content:
                for t in targets: self.add_edge(source, t, "LIÊN_KẾT_TỚI", "")
                continue

            sentences = self.split_sentences(content)
            
            for target in targets:
                target_mentions = [s for s in sentences if target in s]
                print(f"Target: {target}, Mentions: {len(target_mentions)}")
                
                if not target_mentions:
                    self.add_edge(source, target, "LIÊN_KẾT_TỚI", "")
                    continue

                # 1. Rule-based Voting
                relation_scores = defaultdict(int)
                relation_evidence = defaultdict(list)
                
                for sent in target_mentions:
                    detected_rel = self.analyze_sentence_context(sent)
                    weight = RELATION_WEIGHTS.get(detected_rel, 1)
                    relation_scores[detected_rel] += weight
                    relation_evidence[detected_rel].append(sent)
                
                # Sắp xếp
                if len(relation_scores) > 1 and "LIÊN_KẾT_TỚI" in relation_scores:
                    del relation_scores["LIÊN_KẾT_TỚI"]
                
                sorted_rels = sorted(relation_scores.items(), key=lambda item: item[1], reverse=True)
                best_rel = sorted_rels[0][0]
                print(best_rel)
                # 2. Hybrid: Nếu Rule thất bại -> Hỏi Puter AI
                if best_rel == "LIÊN_KẾT_TỚI" and self.has_ai:
                    # Gom context (max 3 câu)
                    context_for_ai = " ".join(target_mentions)
                    
                    # Gọi AI
                    ai_rel = self.ask_ai_agent(source, target, context_for_ai)
                    print(f"   > Puter AI đề xuất: {ai_rel}")
                    if ai_rel != "LIÊN_KẾT_TỚI":
                        best_rel = ai_rel
                        evidence_text = f"[PUTER AI] {context_for_ai[:100]}..."
                        self.add_edge(source, target, best_rel, evidence_text)
                        continue 

                # 3. Chốt kết quả (Top 2 từ Rule hoặc từ AI nếu AI fail)
                top_relations = sorted_rels[:2]
                for rel_type, score in top_relations:
                    evidence_text = relation_evidence[rel_type][0].replace('\n', ' ').strip() if relation_evidence[rel_type] else ""
                    if len(evidence_text) > 200: evidence_text = evidence_text[:200] + "..."
                    self.add_edge(source, target, rel_type, evidence_text)
            
            time.sleep(0.05)

        self.generate_inverse_edges()
        self.save_data()

    def save_data(self):
        print(f"\n--- ĐANG LƯU KẾT QUẢ RA {OUTPUT_FINAL_FILE} ---")
        with open(OUTPUT_FINAL_FILE, 'w', encoding='utf-8') as f:
            fieldnames = ["source", "target", "type", "evidence"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.final_edges)
        print(f"✅ Hoàn tất! {len(self.final_edges)} quan hệ.")

if __name__ == "__main__":
    refiner = RelationRefiner()
    refiner.run()