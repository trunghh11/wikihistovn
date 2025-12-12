import os
# --- 1. FIX LỖI CRASH TRÊN MAC (TQDM) ---
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"

import sys
import json
import gradio as gr
from neo4j import GraphDatabase
from mlx_lm import load, generate

# ============================
# CONFIG
# ============================

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "12345678"
MODEL_ID   = "Qwen/Qwen3-0.6B-MLX-bf16"

# ============================
# LOAD MODEL
# ============================

print("\n>>> ⏳ Đang khởi tạo model MLX Playground...")
try:
    model, tokenizer = load(MODEL_ID)
    print("    - MLX Model đã load thành công!")
except Exception as e:
    print(f"❌ Lỗi load MLX model: {e}")
    sys.exit(1)

# ============================
# CONNECT NEO4J
# ============================

try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print("    - Neo4j đã kết nối!")
except Exception as e:
    print(f"❌ Lỗi kết nối Neo4j: {e}")
    sys.exit(1)


# ============================
# NEO4J QUERIES (SAFE MODE)
# ============================

def query_summary(keyword):
    cypher = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.6
    RETURN node.title as name, node.summary as summary
    LIMIT 1
    """
    with driver.session() as s:
        # Dùng single() an toàn
        r = s.run(cypher, kw=keyword).single()
        if r and r["summary"]:
            return f"TÓM TẮT VỀ {r['name']}:\n{r['summary']}"
    return None


def query_1hop(keyword):
    """Truy vấn quan hệ trực tiếp"""
    cypher = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.6
    WITH node LIMIT 1
    MATCH (node)-[r]-(n1)
    WHERE NOT type(r) IN ['LIÊN_KẾT_TỚI']
    RETURN node.title AS center, type(r) AS rel_type, n1.title AS neighbor
    LIMIT 30
    """
    with driver.session() as s:
        # Dùng list() để lấy hết dữ liệu trước khi đóng session
        results = list(s.run(cypher, kw=keyword))
        
    rows = [f"- {r['center']} --[{r['rel_type']}]--> {r['neighbor']}" for r in results]
    return "\n".join(rows) if rows else None


def query_2hop(keyword):
    """Truy vấn quan hệ bắc cầu (Multi-hop)"""
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
    with driver.session() as s:
        results = list(s.run(cypher, kw=keyword))
        
        for record in results:
            path = record["path"]
            nodes = path.nodes
            rels = path.relationships
            chain = []
            for i in range(len(rels)):
                start = nodes[i].get("title", "Unknown")
                end = nodes[i+1].get("title", "Unknown")
                rel_type = rels[i].type
                chain.append(f"{start} --[{rel_type}]--> {end}")
            paths_text.append(" ; ".join(chain))
            
    return "\n".join(list(set(paths_text))) if paths_text else None


# ============================
# MLX GENERATE (COMPATIBILITY)
# ============================

def run_mlx(prompt, max_tokens=128):
    return generate(model, tokenizer, prompt=prompt, max_tokens=max_tokens, verbose=False).strip()


# ============================
# INTENT DETECTOR (SMART ROUTER)
# ============================

def detect_intent_and_keyword(question):
    # Prompt nâng cao để nhận diện cả số bước nhảy (Hops)
    prompt = f"""Phân tích câu hỏi sau và trả về định dạng JSON duy nhất.
Câu hỏi: "{question}"

Yêu cầu:
1. "intent": "SUMMARY" (Nếu hỏi năm sinh, năm mất, quê quán) Hoặc "RELATION" (nếu hỏi quan hệ) .
2. "keyword": Tên nhân vật chính trong câu hỏi.
3. "hops": 1 hoặc 2.

Ví dụ: "Năm sinh của Minh Mạng là bao nhiêu?" -> {{"intent": "SUMMARY", "keyword": "Minh Mạng", "hops": 2}}

JSON Output:"""

    if hasattr(tokenizer, "apply_chat_template") and tokenizer.chat_template:
        messages = [{"role": "user", "content": prompt}]
        final_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        final_prompt = prompt

    # Dùng temp=0 để output JSON ổn định
    raw = run_mlx(final_prompt, max_tokens=128)

    try:
        json_part = raw[raw.find("{") : raw.rfind("}") + 1]
        return json.loads(json_part)
    except:
        # Fallback mặc định
        return {"intent": "RELATION", "keyword": question, "hops": 1}


# ============================
# RAG PIPELINE
# ============================

def generate_rag_response(question):
    # 1. Router
    analysis = detect_intent_and_keyword(question)
    print(analysis)
    intent = analysis.get("intent", "")
    keyword = analysis.get("keyword", question)
    hops = analysis.get("hops", 1)

    print(f"\n[DEBUG] Intent: {intent} | Keyword: {keyword} | Hops: {hops}")

    # 2. Retriever
    context = None
    if intent == "SUMMARY":
        context = query_summary(keyword)
        if not context:
            context = query_1hop(keyword)
            intent = "RELATION (Fallback)"
    else:
        # Smart Hop Selection
        if hops >= 2:
            context = query_2hop(keyword)
        else:
            context = query_1hop(keyword)

    if not context:
        return "Xin lỗi, tôi không tìm thấy thông tin trong cơ sở dữ liệu.", analysis, "No Context Found"

    # 3. Generator
    instruction = ""
    if hops >= 2:
        instruction = "\nHướng dẫn: Hãy suy luận bắc cầu (Ví dụ: A là cha B, B là cha C => A là ông nội C) để trả lời."

    db_context_display = f"THÔNG TIN ({intent} - {hops} HOP):\n---------------------\n{context}\n---------------------"
    
    user_prompt = f"""DỮ LIỆU TRI THỨC:
----------------
{context}
----------------

Câu hỏi: {question}
{instruction}
Trả lời ngắn gọn:"""

    if hasattr(tokenizer, "apply_chat_template") and tokenizer.chat_template:
        messages = [
            {"role": "system", "content": "Bạn là trợ lý lịch sử Việt Nam trung thực. Chỉ trả lời dựa trên thông tin được cung cấp."},
            {"role": "user", "content": user_prompt},
        ]
        final_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        final_prompt = user_prompt

    # Sinh câu trả lời (Temp=0.1 để ít bịa)
    answer = run_mlx(final_prompt, max_tokens=512)
    
    return answer, analysis, db_context_display


# ============================
# GRADIO PLAYGROUND UI
# ============================

def gradio_process(question):
    answer, router, ctx = generate_rag_response(question)
    return (
        answer,
        json.dumps(router, indent=2, ensure_ascii=False),
        ctx
    )

css = """
.gr-textinput {font-size: 16px !important;}
footer {visibility: hidden}
"""

with gr.Blocks(css=css, title="Sử Việt Chatbot") as demo:

    gr.Markdown("# 🇻🇳 **Playground Sử Việt (MLX + Neo4j)**")
    gr.Markdown("Hệ thống RAG hỗ trợ suy luận Multi-hop trên chip Apple Silicon.")

    with gr.Row():
        with gr.Column(scale=4):
            question = gr.Textbox(label="Câu hỏi", lines=2)
            run_btn = gr.Button("🚀 Gửi câu hỏi", variant="primary")
        
        with gr.Column(scale=2):
            router_box = gr.JSON(label="🔍 Phân tích (Router)")

    with gr.Row():
        ans_box = gr.Textbox(label="🤖 Bot trả lời", lines=5, show_copy_button=True)
    
    # with gr.Row():
    #     ctx_box = gr.Textbox(label="📚 Dữ liệu Graph trích xuất (Context)", lines=10, max_lines=20)

    # Sự kiện
    run_btn.click(
        fn=gradio_process,
        inputs=[question],
        outputs=[ans_box, router_box]
    )
    # Cho phép ấn Enter để gửi
    question.submit(
        fn=gradio_process,
        inputs=[question],
        outputs=[ans_box, router_box]
    )

# Chạy server
print(">>> 🚀 Gradio đang chạy tại: http://localhost:7860")
demo.launch(server_name="0.0.0.0", server_port=7860)