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
# NEO4J QUERIES
# ============================

def query_summary(keyword):
    cypher = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.6
    RETURN node.title as name, node.summary as summary
    LIMIT 1
    """
    with driver.session() as s:
        r = s.run(cypher, kw=keyword).single()
        if r and r["summary"]:
            return f"TÓM TẮT VỀ {r['name']}:\n{r['summary']}"
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
    rows = []
    with driver.session() as s:
        for r in s.run(cypher, kw=keyword):
            rows.append(f"- {r['center']} --[{r['rel_type']}]--> {r['neighbor']}")
    return "\n".join(rows) if rows else None


# ============================
# MLX GENERATE
# ============================

def run_mlx(prompt, max_tokens=128):
    output = generate(
        model,
        tokenizer,
        prompt=prompt,
        max_tokens=max_tokens,
        verbose=False
    )
    return output.strip()


# ============================
# INTENT DETECTOR
# ============================

def detect_intent_and_keyword(question):
    prompt = f"""Bạn là một trợ lý AI chuyên phân tích câu hỏi lịch sử. Nhiệm vụ của bạn là trích xuất thông tin từ câu hỏi của người dùng và trả về định dạng JSON.

Định nghĩa Intent (Ý định):
1. "SUMMARY": Khi người dùng hỏi thông tin chung, tiểu sử, định nghĩa.
   - Từ khóa nhận biết: "là ai", "tiểu sử", "giới thiệu", "cuộc đời", "thông tin", "sự nghiệp", "sinh năm nào", "mất năm nào".
2. "RELATION": Khi người dùng hỏi về mối quan hệ giữa các nhân vật hoặc chức vụ, vai trò.
   - Từ khóa nhận biết: "cha", "mẹ", "con", "vợ", "chồng", "anh", "em", "kế nhiệm", "tiền nhiệm", "thầy", "trò", "quan hệ", "là gì của".

Ví dụ mẫu (Hãy học theo cách phân tích này):
- Câu hỏi: "Vua Gia Long là ai?"
  -> {{"intent": "SUMMARY", "keyword": "Gia Long"}}

- Câu hỏi: "Cha của vua Minh Mạng là ai?"
  -> {{"intent": "RELATION", "keyword": "Minh Mạng"}} (Lưu ý: Lấy tên nhân vật đã biết, không lấy từ "Cha")

- Câu hỏi: "Ai là vợ của vua Bảo Đại?"
  -> {{"intent": "RELATION", "keyword": "Bảo Đại"}}

- Câu hỏi: "Hãy tóm tắt tiểu sử Trần Hưng Đạo"
  -> {{"intent": "SUMMARY", "keyword": "Trần Hưng Đạo"}}

- Câu hỏi: "Nguyễn Huệ và Nguyễn Nhạc có quan hệ gì?"
  -> {{"intent": "RELATION", "keyword": "Nguyễn Huệ"}}

Yêu cầu output:
- Chỉ trả về 1 JSON duy nhất.
- Không giải thích thêm.
- Keyword chỉ chứa tên riêng, loại bỏ các từ như "vua", "ông", "bà" nếu không cần thiết.

Câu hỏi cần phân tích: "{question}"
JSON Output:"""

    if tokenizer.chat_template:
        messages = [{"role": "user", "content": prompt}]
        final_prompt = tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
    else:
        final_prompt = prompt

    raw = run_mlx(final_prompt, max_tokens=64)

    try:
        json_part = raw[raw.find("{") : raw.rfind("}") + 1]
        return json.loads(json_part)
    except:
        return {"intent": "RELATION", "keyword": question}


# ============================
# RAG PIPELINE
# ============================

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
    return answer.strip(), analysis, db_context


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
.gr-textinput {font-size: 18px !important;}
"""

with gr.Blocks(css=css, title="MLX RAG Playground") as demo:

    gr.Markdown("# 🤖 **Vietnam History MLX Playground**\nRAG + Qwen3-0.6B + Neo4j + MLX")

    with gr.Row():
        question = gr.Textbox(label="Nhập câu hỏi", placeholder="Ví dụ: Cha của Minh Mạng là ai?", lines=2)

    run_btn = gr.Button("🚀 Generate")

    with gr.Row():
        ans_box = gr.Textbox(label="Trả lời từ Bot", lines=7)
    with gr.Row():
        router_box = gr.Textbox(label="Router (Intent + Keyword)", lines=6)
    with gr.Row():
        ctx_box = gr.Textbox(label="Context lấy từ Neo4j", lines=12)

    run_btn.click(
        fn=gradio_process,
        inputs=[question],
        outputs=[ans_box, router_box, ctx_box]
    )

demo.launch(server_name="0.0.0.0", server_port=7860)
