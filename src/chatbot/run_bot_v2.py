import sys
import json
from neo4j import GraphDatabase
from mlx_lm import load, generate

# --- CONFIG ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "12345678"
# Model MLX Native (Tải bản này để tối ưu cho Mac)
MODEL_ID = "Qwen/Qwen3-0.6B-MLX-bf16" 

# --- 1. LOAD MLX MODEL ---
print("\n>>> ⏳ Đang khởi tạo Model MLX (Siêu tốc cho Mac)...")
try:
    # Load model và tokenizer bằng thư viện mlx_lm
    model, tokenizer = load(MODEL_ID)
    print("    - Model đã load thành công!")
except Exception as e:
    print(f"❌ Lỗi load MLX model: {e}")
    sys.exit(1)

# --- 2. CONNECT NEO4J ---
try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print("    - Neo4j đã kết nối!")
except Exception as e:
    print(f"❌ Lỗi kết nối Neo4j: {e}")
    sys.exit(1)

# --- 3. QUERY FUNCTIONS ---

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
            lines.append(f"- {r['center']} --[{r['rel_type']}]--> {r['neighbor']}")
    return "\n".join(lines) if lines else None

# --- 4. INTENT DETECTOR (MLX) ---

def run_mlx(prompt: str, max_tokens=128):
    """Helper để sinh text với MLX, tắt verbose để không in lung tung"""
    output = generate(
        model, 
        tokenizer, 
        prompt=prompt, 
        max_tokens=max_tokens, 
        verbose=True
    )
    return output

def detect_intent_and_keyword(question):
    prompt = f"""Phân tích câu hỏi sau và trả về định dạng JSON duy nhất.
Câu hỏi: "{question}"

Yêu cầu:
1. "intent": Chọn "SUMMARY" (hỏi là ai, tiểu sử) hoặc "RELATION" (hỏi quan hệ, cha con).
2. "keyword": Tên nhân vật chính.

Ví dụ: "Vua Minh Mạng là ai?" -> {{"intent": "SUMMARY", "keyword": "Minh Mạng"}}

Trả về JSON:"""

    # Áp dụng chat template nếu có
    if hasattr(tokenizer, "apply_chat_template") and tokenizer.chat_template:
        messages = [{"role": "user", "content": prompt}]
        final_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        final_prompt = prompt

    raw = run_mlx(final_prompt, max_tokens=100)

    # Cố gắng trích xuất JSON từ phản hồi
    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start != -1 and end != -1:
            json_str = raw[start:end]
            return json.loads(json_str)
        else:
            return {"intent": "RELATION", "keyword": question}
    except:
        return {"intent": "RELATION", "keyword": question}

# --- 5. RAG ANSWERING ---

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
    db_context = f"""THÔNG TIN TỪ CƠ SỞ DỮ LIỆU ({intent}):
---------------------
{context}
---------------------"""

    user_prompt = f"{db_context}\n\nDựa vào thông tin trên, hãy trả lời câu hỏi: {question}\nTrả lời ngắn gọn:"

    if hasattr(tokenizer, "apply_chat_template") and tokenizer.chat_template:
        messages = [
            {"role": "system", "content": "Bạn là trợ lý lịch sử Việt Nam trung thực. Chỉ trả lời dựa trên thông tin được cung cấp."},
            {"role": "user", "content": user_prompt},
        ]
        final_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        final_prompt = user_prompt

    # Tăng max_tokens cho câu trả lời cuối cùng
    answer = run_mlx(final_prompt, max_tokens=512)
    return answer.strip()

# --- 6. CHAT LOOP ---
def start_chat():
    print("\n" + "="*50)
    print("🤖 Chatbot Lịch sử Việt Nam - MLX Optimized")
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