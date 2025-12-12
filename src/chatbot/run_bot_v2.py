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
DEFAULT_MAX_TOKENS = 512

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


def query_db_1hop(keyword):
    """Truy vấn quan hệ trực tiếp (1 bước)"""
    cypher = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.6
    WITH node LIMIT 1
    MATCH (node)-[r]-(n1)
    WHERE NOT type(r) IN ['LIÊN_KẾT_TỚI']
    RETURN node.title AS center, type(r) AS rel_type, n1.title AS neighbor
    LIMIT 30
    """
    with driver.session() as session:
        # FIX QUAN TRỌNG: Dùng list() để lấy hết dữ liệu trước khi đóng session
        records = list(session.run(cypher, kw=keyword))
        
    lines = [f"- {r['center']} --[{r['rel_type']}]--> {r['neighbor']}" for r in records]
    return "\n".join(lines) if lines else None

def query_db_2hop(keyword):
    """Truy vấn quan hệ bắc cầu (2 bước)"""
    cypher = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.6
    WITH node LIMIT 1
    MATCH path = (node)-[*1..2]-(m)
    WHERE NONE(r IN relationships(path) WHERE type(r) IN ['LIÊN_KẾT_TỚI'])
    AND m.title <> node.title
    RETURN path
    LIMIT 50
    """
    paths_text = []
    with driver.session() as session:
        # FIX QUAN TRỌNG: Dùng list()
        result = list(session.run(cypher, kw=keyword))
        
        for record in result:
            path = record["path"]
            nodes = path.nodes
            rels = path.relationships
            chain = []
            for i in range(len(rels)):
                start = nodes[i].get("title", "Unknown")
                end = nodes[i+1].get("title", "Unknown")
                rel_type = rels[i].type
                # Xác định hướng mũi tên
                if rels[i].start_node.element_id == nodes[i].element_id:
                    chain.append(f"{start} --[{rel_type}]--> {end}")
                else:
                    chain.append(f"{end} --[{rel_type}]--> {start}")
            paths_text.append(" ; ".join(chain))
            
    return "\n".join(list(set(paths_text))) if paths_text else None

def get_context(keyword, intent, hops):
    # 1. Nếu hỏi Summary
    if intent == "SUMMARY":
        context = query_summary(keyword)
        if context: return context
        # Nếu không có summary, fallback sang RELATION
        intent = "RELATION (Fallback)"

    # 2. Nếu hỏi Relation
    if hops >= 2:
        return query_db_2hop(keyword)
    else:
        return query_db_1hop(keyword)

def run_mlx(prompt: str, max_tokens=128):
    output = generate(
        model, 
        tokenizer, 
        prompt=prompt, 
        max_tokens=max_tokens, 
        verbose=False
    )
    return output

def analyze_question(question):
    """Router thông minh: Xác định Intent, Keyword và Số bước nhảy"""
    prompt = f"""Phân tích câu hỏi và trả về JSON.
Câu hỏi: "{question}"

Yêu cầu:
1. "intent": "SUMMARY" (hỏi là ai, tiểu sử) hoặc "RELATION" (quan hệ).
2. "keyword": Tên nhân vật chính.
3. "hops": 1 (quan hệ trực tiếp: cha, con) hoặc 2 (gián tiếp: ông, cháu, bác).

Ví dụ: "Ông nội Tự Đức là ai?" -> {{"intent": "RELATION", "keyword": "Tự Đức", "hops": 2}}

JSON Output:"""

    # Format prompt theo chat template nếu có
    if hasattr(tokenizer, "apply_chat_template") and tokenizer.chat_template:
        messages = [{"role": "user", "content": prompt}]
        final_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        final_prompt = prompt

    # Gọi model
    raw = run_mlx(final_prompt, max_tokens=100)

    # Parse JSON
    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start != -1 and end != -1:
            return json.loads(raw[start:end])
    except:
        pass
    
    # Mặc định nếu lỗi
    return {"intent": "RELATION", "keyword": question, "hops": 1}

# --- 5. RAG ANSWERING ---

def get_answer(question):
    # A. Router Phase
    analysis = analyze_question(question)
    intent = analysis.get("intent", "RELATION")
    keyword = analysis.get("keyword", question)
    hops = analysis.get("hops", 1)

    # Hiển thị Debug ra Sidebar để theo dõi
    st.sidebar.markdown("### 🔍 Debug Lần Cuối")
    st.sidebar.info(f"- **Intent:** `{intent}`\n- **Keyword:** `{keyword}`\n- **Hops:** `{hops}`")

    # B. Retriever Phase
    context = get_context(keyword, intent, hops)
    
    if not context:
        return "Xin lỗi, tôi không tìm thấy thông tin trong cơ sở dữ liệu."

    # C. Generator Phase
    instruction = ""
    if hops >= 2:
        instruction = "\nHướng dẫn: Hãy suy luận bắc cầu (Ví dụ: A là cha B, B là cha C => A là ông nội C) để trả lời."

    prompt_rag = f"""DỮ LIỆU TRI THỨC:
----------------
{context}
----------------

Câu hỏi: {question}
{instruction}
Trả lời ngắn gọn:"""

    return run_mlx(prompt_rag, DEFAULT_MAX_TOKENS).strip()

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