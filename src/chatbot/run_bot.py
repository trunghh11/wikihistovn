import sys
import torch
from neo4j import GraphDatabase
from transformers import AutoModelForCausalLM, AutoTokenizer

# --- CẤU HÌNH ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "12345678"  # <--- Nhập mật khẩu của bạn
MODEL_ID = "Qwen/Qwen2-0.5B-Instruct"

# --- 1. KHỞI TẠO MODEL ---
print("\n>>> ⏳ Đang khởi tạo hệ thống...")
try:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_ID, 
        device_map="cpu", 
        torch_dtype=torch.float32
    )
    print("    - Model Qwen2-0.5B đã sẵn sàng!")
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

# --- 3. CORE LOGIC ---

def get_graph_context(keyword):
    """
    Truy vấn đơn giản (1-hop): Chỉ lấy thông tin trực tiếp và dịch quan hệ thành tiếng Việt.
    """
    cypher_query = """
    CALL db.index.fulltext.queryNodes("title_index", $kw) YIELD node, score
    WHERE score > 0.5
    WITH node LIMIT 1
    
    // --- LẤY QUAN HỆ 1-HOP VÀ DỊCH NGAY ---
    OPTIONAL MATCH (node)-[r]-(n1)
    
    // Giữ nguyên các loại quan hệ được liệt kê, chỉ lọc ra các quan hệ ít quan trọng
    WHERE NOT type(r) IN ['LIÊN_KẾT_TỚI'] 
    
    RETURN 
        node.title AS center, 
        type(r) AS rel_type, 
        n1.title AS neighbor,
        startNode(r) = node AS is_outgoing
    LIMIT 50
    """
    
    context_lines = []
    
    def translate_relationship(center, rel_type, neighbor, is_outgoing):
      """Hàm Python dịch quan hệ (Relationship Type) sang tiếng Việt tự nhiên."""
      
      # Dùng toLower và replace('_', ' ') cho các loại quan hệ ít gặp
      rel_type_clean = rel_type.replace('_', ' ').lower()
      
      # 1. Dịch các quan hệ Huyết thống / Hôn nhân (Sống còn)
      if rel_type == 'LÀ_CHA_CỦA':
          return f"- {center} là cha của {neighbor}." if is_outgoing else f"- {center} là con của {neighbor}."
      if rel_type == 'LÀ_MẸ_CỦA':
          return f"- {center} là mẹ của {neighbor}." if is_outgoing else f"- {center} là con của {neighbor}."
      if rel_type == 'LÀ_CON_CỦA':
          return f"- {center} là con của {neighbor}." if is_outgoing else f"- {center} là cha/mẹ của {neighbor}." # Giữ logic tổng quát nếu không rõ giới tính
      if rel_type == 'PHỐI_NGẪU_VỚI':
          return f"- {center} là vợ/chồng của {neighbor}."
      if rel_type == 'LÀ_ANH_EM_CỦA':
          return f"- {center} là anh/chị/em của {neighbor}."

      # 2. Dịch các quan hệ Chính trị / Kế thừa
      if rel_type == 'KẾ_NHIỆM_CỦA':
          return f"- {center} là người kế nhiệm của {neighbor}." if is_outgoing else f"- {center} là người tiền nhiệm của {neighbor}."
      if rel_type == 'TIỀN_NHIỆM_CỦA':
          return f"- {center} là người tiền nhiệm của {neighbor}." if is_outgoing else f"- {center} là người kế nhiệm của {neighbor}."

      # 3. Dịch các quan hệ Quản lý / Sự kiện
      if rel_type == 'CHỈ_HUY':
          return f"- {center} chỉ huy {neighbor}."
      if rel_type == 'ĐƯỢC_CHỈ_HUY_BỞI':
          return f"- {center} được chỉ huy bởi {neighbor}."
      if rel_type == 'ĐƯỢC_BỔ_NHIỆM_BỞI':
          return f"- {center} được bổ nhiệm bởi {neighbor}."
      if rel_type == 'PHỤC_VỤ':
          return f"- {center} phục vụ dưới trướng {neighbor}."
      if rel_type == 'XỬ_LÝ':
          return f"- {center} đã xử lý {neighbor}."
      if rel_type == 'BỊ_XỬ_LÝ_BỞI':
          return f"- {center} bị xử lý bởi {neighbor}."
      if rel_type == 'BỊ_PHẾ_TRUẤT_BỞI':
          return f"- {center} bị phế truất bởi {neighbor}."

      # 4. Dịch các quan hệ Xã hội / Đối đầu
      if rel_type == 'ĐỒNG_MINH_VỚI':
          return f"- {center} là đồng minh với {neighbor}."
      if rel_type == 'ĐỒNG_ĐỘI_VỚI':
          return f"- {center} là đồng đội với {neighbor}."
      if rel_type == 'ĐỐI_THỦ_CỦA':
          return f"- {center} là đối thủ của {neighbor}."
      if rel_type == 'LÀ_THẦY_CỦA':
          return f"- {center} là thầy của {neighbor}."
      if rel_type == 'LÀ_TRÒ_CỦA':
          return f"- {center} là trò (học trò) của {neighbor}."
      
      # 5. Fallback cho các quan hệ không được liệt kê
      return f"- {center} có quan hệ {rel_type_clean} với {neighbor}."


    try:
        with driver.session() as session:
            result = session.run(cypher_query, kw=keyword)
            for record in result:
                line = translate_relationship(
                    record['center'], 
                    record['rel_type'], 
                    record['neighbor'], 
                    record['is_outgoing']
                )
                context_lines.append(line)
            
    except Exception as e:
        return f"Lỗi Cypher: {str(e)}"
            
    # Loại bỏ trùng lặp và nối chuỗi
    return "\n".join(list(set(context_lines))) if context_lines else ""

def generate_response(question):
    """Trả về cả câu trả lời VÀ context"""
    context = get_graph_context(question)
    
    if not context:
        return "Xin lỗi, tôi không tìm thấy thông tin trong dữ liệu.", ""

    # Prompt được tinh chỉnh để Bot logic hơn
    prompt_template = f"""Context information is below.
---------------------
{context}
---------------------
Given the context information and not prior knowledge, answer the query.
Query: {question}
Answer (in Vietnamese, be direct):"""  

    messages = [
        {"role": "system", "content": "You are a history bot. Use the Context to answer. If the answer involves multiple steps (like grandfather), deduce it from the relations provided."},
        {"role": "user", "content": prompt_template}
    ]
    
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer([text], return_tensors="pt").to(model.device)
    input_len = inputs.input_ids.shape[1]

    outputs = model.generate(
        inputs.input_ids,
        attention_mask=inputs.attention_mask,
        max_new_tokens=100,
        temperature=0.1, # Giảm nhiệt độ để bớt "sáng tạo" sai sự thật
        pad_token_id=tokenizer.pad_token_id
    )
    
    generated_tokens = outputs[0][input_len:]
    response = tokenizer.decode(generated_tokens, skip_special_tokens=True)
    
    return response.strip(), context

# --- 4. MAIN LOOP ---

def start_chat_session():
    print("\n" + "="*50)
    print("🤖 CHATBOT SỬ VIỆT (DEBUG MODE)")
    print("💡 Context từ Neo4j sẽ được hiển thị màu vàng.")
    print("="*50 + "\n")

    while True:
        try:
            user_input = input("Bạn: ").strip()
            if user_input.lower() in ['exit', 'quit', 'thoát']:
                print("Bot: Tạm biệt!")
                break
            if not user_input:
                continue

            print("Bot: Đang truy vấn...", end="\r")
            
            # Gọi hàm lấy cả answer và context
            answer, context = generate_response(user_input)
            
            # Xóa dòng chờ
            print(" " * 30, end="\r")
            
            # In Context (Màu vàng để dễ nhìn - ANSI code)
            if context:
                print("\033[93m" + "--- [NEO4J CONTEXT] ---")
                print(context)
                print("-----------------------" + "\033[0m")
            else:
                print("\033[91m" + "[!] Không tìm thấy Context trong Graph" + "\033[0m")

            # In câu trả lời
            print(f"Bot: {answer}\n")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Lỗi: {e}")

if __name__ == "__main__":
    start_chat_session()