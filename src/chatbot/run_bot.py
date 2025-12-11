import sys
import json
import torch
from neo4j import GraphDatabase
from transformers import AutoModelForCausalLM, AutoTokenizer

# --- CẤU HÌNH ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "12345678"
MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"

# --- 1. KHỞI TẠO MODEL (CHẾ ĐỘ CPU SAFE MODE) ---
print(f"\n>>> ⏳ Đang khởi tạo Model ({MODEL_ID})...")
try:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # ⚠️ QUAN TRỌNG: Ép chạy CPU để tránh lỗi "NDArray > 2**32" trên Mac
    print("    - Đang cấu hình chạy trên CPU (Chế độ ổn định cho Mac)...")
    
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_ID,
        device_map="cpu",  # <--- KHÔNG DÙNG "auto" hay "mps"
        torch_dtype=torch.float32, # <--- CPU chạy ổn định nhất với float32
        low_cpu_mem_usage=True
    )
    print("    - Model đã load thành công!")

except Exception as e:
    print(f"❌ Lỗi load model: {e}")
    sys.exit(1)

# --- 2. KẾT NỐI NEO4J ---
try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print("    - Neo4j đã kết nối!")
except Exception as e:
    print(f"❌ Lỗi kết nối Neo4j: {e}")
    sys.exit(1)

# --- 3. CÁC HÀM TRUY VẤN ---

def query_summary(keyword):
    cypher = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.6
    RETURN node.title as name, node.summary as summary
    LIMIT 1
    """
    with driver.session() as session:
        record = session.run(cypher, kw=keyword).single()
        if record and record["summary"]:
            return f"TÓM TẮT VỀ {record['name']}:\n{record['summary']}"
    return None

def query_relations(keyword):
    cypher = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.6
    WITH node LIMIT 1
    MATCH (node)-[r]-(n1)
    WHERE NOT type(r) IN ['LIÊN_KẾT_TỚI']
    RETURN node.title AS center, type(r) AS rel_type, n1.title AS neighbor
    LIMIT 30
    """
    lines = []
    with driver.session() as session:
        for r in session.run(cypher, kw=keyword):
            r_type = r['rel_type'].replace('_', ' ').lower()
            lines.append(f"- {r['center']} là {r_type} của {r['neighbor']}")
    return "\n".join(lines) if lines else None

# --- 4. ROUTER ---

def detect_intent_and_keyword(question):
    prompt = f"""Phân tích câu hỏi sau và trả về định dạng JSON duy nhất.
Câu hỏi: "{question}"

Yêu cầu:
1. "intent": Chọn "SUMMARY" (tiểu sử, là ai) hoặc "RELATION" (quan hệ, cha con).
2. "keyword": Tên nhân vật chính.

Ví dụ: "Vua Minh Mạng là ai?" -> {{"intent": "SUMMARY", "keyword": "Minh Mạng"}}

Trả về JSON:"""

    messages = [{"role": "user", "content": prompt}]
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer([text], return_tensors="pt") # Không cần .to(device) vì đang ở CPU

    # Sinh JSON
    with torch.no_grad():
        outputs = model.generate(
            inputs.input_ids,
            max_new_tokens=64,
            do_sample=False, # Greedy decoding cho JSON
            pad_token_id=tokenizer.eos_token_id,
            attention_mask=inputs.attention_mask # Sửa warning attention mask
        )
    response = tokenizer.decode(outputs[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)

    try:
        start = response.find("{")
        end = response.rfind("}") + 1
        if start != -1 and end != -1:
            json_str = response[start:end]
            return json.loads(json_str)
        else:
            return {"intent": "RELATION", "keyword": question}
    except:
        return {"intent": "RELATION", "keyword": question}

# --- 5. RAG GENERATOR ---

def generate_rag_response(question):
    # 1. Router
    analysis = detect_intent_and_keyword(question)
    intent = analysis.get("intent", "RELATION")
    keyword = analysis.get("keyword", question)

    print(f"\n[DEBUG] Intent: {intent} | Keyword: {keyword}")

    # 2. Retriever
    if intent == "SUMMARY":
        context = query_summary(keyword)
        if not context:
            context = query_relations(keyword)
            intent = "RELATION (Fallback)"
    else:
        context = query_relations(keyword)

    if not context:
        return "Xin lỗi, tôi không tìm thấy thông tin trong cơ sở dữ liệu."

    # 3. Generator
    system_prompt = "Bạn là trợ lý lịch sử Việt Nam. Chỉ trả lời dựa trên thông tin được cung cấp. Trả lời ngắn gọn bằng tiếng Việt."
    
    user_prompt = f"""THÔNG TIN TỪ DATABASE ({intent}):
----------------
{context}
----------------

CÂU HỎI: {question}
TRẢ LỜI:"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer([text], return_tensors="pt")
    
    with torch.no_grad():
        outputs = model.generate(
            inputs.input_ids,
            attention_mask=inputs.attention_mask, # Fix warning
            max_new_tokens=300,
            temperature=0.3,
            do_sample=True, # Fix warning (temperature cần do_sample=True)
            repetition_penalty=1.1,
            pad_token_id=tokenizer.eos_token_id
        )
    
    answer = tokenizer.decode(outputs[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)
    return answer.strip()

# --- 6. MAIN LOOP ---
def start_chat():
    print("\n" + "="*50)
    print("🤖 Chatbot SỬ VIỆT (Transformers CPU Mode)")
    print("⚠️ Lưu ý: Chạy trên CPU sẽ chậm hơn GPU/MLX")
    print("="*50)
    
    while True:
        try:
            q = input("\nBạn: ").strip()
            if q.lower() in ["exit", "quit", "thoát"]:
                print("Bot: Tạm biệt!")
                break
            if not q: continue

            ans = generate_rag_response(q)
            print(f"Bot: {ans}")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Lỗi Runtime: {e}")

if __name__ == "__main__":
    start_chat()