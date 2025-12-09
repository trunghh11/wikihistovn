import os
import json
import time
import re
import requests
from tqdm import tqdm

# --- CẤU HÌNH ---

# ⚠️ DÁN TOÀN BỘ API KEY CỦA BẠN VÀO DANH SÁCH NÀY
API_KEYS = [
    os.getenv("GOOGLE_API_KEY_1"),
    os.getenv("GOOGLE_API_KEY_2"),
    os.getenv("GOOGLE_API_KEY_3"),
]

INPUT_RELATIONS_FILE = 'data/processed/network_relationships_contextual.json' 
OUTPUT_DATASET_FILE = 'data/processed/evaluation_dataset_yes_no.json'

NUM_RELATIONS_TO_PROCESS = 10 
DELAY_BETWEEN_CALLS = 0.5 # Giảm delay vì có nhiều key để backup

# Biến toàn cục theo dõi Key đang dùng
current_key_index = 0

def get_current_key():
    """Lấy API Key hiện tại"""
    global current_key_index
    if current_key_index >= len(API_KEYS):
        print("\n❌ TẤT CẢ API KEY ĐỀU ĐÃ HẾT QUOTA! Dừng chương trình.")
        exit(1)
    return API_KEYS[current_key_index]

def switch_next_key():
    """Chuyển sang Key tiếp theo"""
    global current_key_index
    old_key = API_KEYS[current_key_index][-4:] # 4 ký tự cuối để log
    current_key_index += 1
    if current_key_index < len(API_KEYS):
        new_key = API_KEYS[current_key_index][-4:]
        print(f"\n⚠️ Key ...{old_key} hết hạn ngạch (429). Đang chuyển sang Key ...{new_key}")
        return True
    else:
        return False

def call_gemini_direct(prompt):
    """
    Gọi API với cơ chế tự động đổi Key khi gặp lỗi 429.
    """
    model_name = "gemini-2.0-flash"
    
    # Vòng lặp Retry: Nếu lỗi 429 -> Đổi Key -> Thử lại ngay lập tức
    while True:
        api_key = get_current_key()
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        
        headers = {'Content-Type': 'application/json'}
        data = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"response_mime_type": "application/json"}
        }
        
        try:
            response = requests.post(url, headers=headers, json=data)
            
            # --- XỬ LÝ LỖI QUOTA (429) ---
            if response.status_code == 429:
                if switch_next_key():
                    continue # Thử lại ngay với Key mới
                else:
                    print("\n❌ Đã thử hết tất cả API Key. Dừng lại.")
                    return None
            
            # Nếu thành công (200)
            if response.status_code == 200:
                return response.json()
            
            # Các lỗi khác (400, 500...) -> Không đổi key, log lỗi và bỏ qua
            print(f"\n[!] API Error {response.status_code}: {response.text}")
            return None

        except Exception as e:
            print(f"\n[!] Lỗi kết nối mạng: {e}")
            return None

def generate_tf_pairs(fact_triple):
    source = fact_triple.get('source', 'A')
    target = fact_triple.get('target', 'B')
    rel_type = fact_triple.get('type', 'LIÊN_KẾT')
    rel_text = rel_type.replace('_', ' ').lower()
    
    # Prompt tối giản: Chỉ lấy question và expected_answer
    prompt = f"""
    Dữ kiện: "{source} có quan hệ {rel_text} với {target}".
    
    Nhiệm vụ: Tạo 2 câu hỏi Yes/No (1 Đúng, 1 Sai).
    
    Yêu cầu Output JSON List (Chỉ 2 trường):
    [
        {{
            "question": "Câu hỏi đúng?",
            "expected_answer": "Đúng"
        }},
        {{
            "question": "Câu hỏi sai?",
            "expected_answer": "Sai"
        }}
    ]
    """
    
    api_response = call_gemini_direct(prompt)
    
    if not api_response:
        return None

    try:
        candidates = api_response.get('candidates', [])
        if not candidates: return None
        
        raw_text = candidates[0].get('content', {}).get('parts', [])[0].get('text', '')
        
        # Parse JSON
        data = json.loads(raw_text)
        
        # Không thêm origin_fact hay explanation nữa
        return data

    except (json.JSONDecodeError, KeyError, IndexError):
        # Fallback Regex nếu cần
        try:
             json_match = re.search(r'\[.*\]', raw_text, re.DOTALL)
             if json_match:
                 return json.loads(json_match.group(0))
        except:
            pass
        return None

def main():
    if not os.path.exists(INPUT_RELATIONS_FILE):
        print(f"❌ Không tìm thấy file: {INPUT_RELATIONS_FILE}")
        return

    print(f"--- ĐANG ĐỌC DỮ LIỆU ---")
    with open(INPUT_RELATIONS_FILE, 'r', encoding='utf-8') as f:
        relationships = json.load(f)
        
    target_rels = relationships[:NUM_RELATIONS_TO_PROCESS]
    final_dataset = []
    
    print(f"--- BẮT ĐẦU SINH DỮ LIỆU (MULTI-KEY MODE) ---")
    print(f"Số lượng Key khả dụng: {len(API_KEYS)}")

    for i, rel in enumerate(tqdm(target_rels)):
        questions = generate_tf_pairs(rel)
        if questions:
            final_dataset.extend(questions)
        
        time.sleep(DELAY_BETWEEN_CALLS)

    print(f"\n--- HOÀN TẤT: Đã tạo {len(final_dataset)} câu hỏi ---")
    
    with open(OUTPUT_DATASET_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_dataset, f, ensure_ascii=False, indent=4)
    
    print(f"✅ File đã lưu tại: {OUTPUT_DATASET_FILE}")

if __name__ == "__main__":
    main()