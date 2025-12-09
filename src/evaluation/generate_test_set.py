import os
import json
import time
import re
from tqdm import tqdm
from openai import OpenAI
from openai import RateLimitError # Lỗi cần bắt cho Key Rotation

# --- CẤU HÌNH API VÀ KEYS ---

# ⚠️ DÁN TOÀN BỘ API KEY CỦA BẠN VÀO DANH SÁCH NÀY
API_KEYS = [
    os.getenv("OPENAI_API_KEY"),
    # Thêm bao nhiêu key tùy thích
]

INPUT_RELATIONS_FILE = 'data/processed/network_relationships_contextual.json' 
OUTPUT_DATASET_FILE = 'data/processed/test_2000.json'

NUM_RELATIONS_TO_PROCESS = 10 
DELAY_BETWEEN_CALLS = 0.5 

# Biến toàn cục theo dõi Key đang dùng
current_key_index = 0

def get_openai_client():
    """Tạo đối tượng OpenAI client với Key hiện tại."""
    global current_key_index
    if current_key_index >= len(API_KEYS):
        print("\n❌ TẤT CẢ API KEY ĐỀU ĐÃ HẾT QUOTA! Dừng chương trình.")
        exit(1)
    
    # Khởi tạo client với API Key hiện tại
    return OpenAI(api_key=API_KEYS[current_key_index])

def switch_next_key():
    """Chuyển sang Key tiếp theo và kiểm tra vòng lặp."""
    global current_key_index
    old_key = API_KEYS[current_key_index][-4:]
    current_key_index += 1
    
    if current_key_index < len(API_KEYS):
        new_key = API_KEYS[current_key_index][-4:]
        print(f"\n⚠️ Key ...{old_key} hết hạn ngạch (429). Đang chuyển sang Key ...{new_key}")
        return True
    else:
        return False


def generate_tf_pairs(fact_triple):
    """
    Hàm gọi OpenAI để sinh câu hỏi với chế độ JSON Mode.
    """
    source = fact_triple.get('source', 'A')
    target = fact_triple.get('target', 'B')
    rel_type = fact_triple.get('type', 'LIÊN_KẾT')
    rel_text = rel_type.replace('_', ' ').lower()
    
    # Prompt được tối ưu cho JSON Mode và yêu cầu output tối giản
    prompt_content = f"""
    Dữ kiện: "{source} có quan hệ {rel_text} với {target}".
    
    Nhiệm vụ: Tạo 2 câu hỏi Yes/No (1 câu Đúng, 1 câu Sai).
    
    Yêu cầu Output JSON (Không giải thích, chỉ 2 trường):
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
    
    # Vòng lặp Retry và Key Rotation
    while True:
        try:
            client = get_openai_client() # Lấy client với key hiện tại
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo-0125",  # Model có hỗ trợ JSON Mode và giá tốt
                response_format={"type": "json_object"}, # Kích hoạt chế độ JSON Mode
                messages=[
                    {"role": "system", "content": "Bạn là chuyên gia lịch sử. Bạn phải trả lời bằng một mảng JSON (List) TUYỆT ĐỐI KHÔNG chứa nội dung nào khác."},
                    {"role": "user", "content": prompt_content}
                ]
            )
            
            # Lấy nội dung JSON từ phản hồi
            raw_json_str = response.choices[0].message.content
            return json.loads(raw_json_str)
        
        except RateLimitError:
            # Bắt lỗi 429 (Key Exhausted) và đổi Key
            if switch_next_key():
                time.sleep(1) # Nghỉ 1 giây trước khi retry
                continue # Retry ngay lập tức với key mới
            else:
                return None # Đã thử hết Keys
        
        except json.JSONDecodeError:
            # Xử lý nếu mô hình trả về JSON bị lỗi
            print(f"\n[!] Lỗi JSON Decode: Mô hình trả về không đúng cấu trúc.")
            return None
            
        except Exception as e:
            # Bắt các lỗi khác (ví dụ: Network Error, Invalid Key 401)
            return None


def main():
    if len(API_KEYS) == 0 or "CUA_BAN" in API_KEYS[0]:
        print("❌ LỖI CẤU HÌNH: Vui lòng dán danh sách API KEY vào biến API_KEYS.")
        return

    if not os.path.exists(INPUT_RELATIONS_FILE):
        print(f"❌ Không tìm thấy file: {INPUT_RELATIONS_FILE}")
        return

    print(f"--- ĐANG ĐỌC DỮ LIỆU ---")
    with open(INPUT_RELATIONS_FILE, 'r', encoding='utf-8') as f:
        relationships = json.load(f)
        
    target_rels = relationships[:NUM_RELATIONS_TO_PROCESS]
    final_dataset = []
    
    print(f"--- BẮT ĐẦU SINH DỮ LIỆU (OPENAI MULTI-KEY) ---")
    print(f"Sử dụng Model: gpt-3.5-turbo-0125")

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