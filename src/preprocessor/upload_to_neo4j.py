import json
import csv
import os
from neo4j import GraphDatabase

# --- ⚠️ BƯỚC 1: CẤU HÌNH LOCAL (Thay đổi mật khẩu của bạn) ---
LOCAL_URI = "neo4j://127.0.0.1:7687"
LOCAL_USER = "neo4j"
LOCAL_PASSWORD = "12345678"  # <--- Thay mật khẩu bạn đã đặt lúc cài Neo4j Desktop
# -------------------------------------------------------------

# Đường dẫn file dữ liệu
BASE_PATH = 'data/processed/' # Lưu ý đường dẫn tương đối khi chạy từ thư mục gốc dự án
JSON_NODES_IN = os.path.join(BASE_PATH, 'nodes_metadata_enriched.json')
CSV_RELS_IN = os.path.join(BASE_PATH, 'final_relations.csv')

# --- CÁC CÂU LỆNH CYPHER ---

# 1. Lệnh xóa sạch dữ liệu cũ
CYPHER_DELETE_ALL = "MATCH (n) DETACH DELETE n"

# 2. Tạo Constraint (Ràng buộc duy nhất) & Index
# Trên Local chạy cái này thoải mái để tìm kiếm nhanh và tránh trùng lặp
CYPHER_CREATE_CONSTRAINTS = """
// Khóa chính mới: Đảm bảo Title là duy nhất (Unique)
CREATE CONSTRAINT unique_entity_title IF NOT EXISTS
FOR (n:ThucThe) REQUIRE n.title IS UNIQUE
"""

CYPHER_CREATE_INDEX = """
// Index phụ cho page_id (để tìm kiếm)
CREATE INDEX page_id_index IF NOT EXISTS
FOR (n:ThucThe) ON (n.page_id)
"""

# 3. Nạp Nodes (MERGE trên Title)
CYPHER_UPLOAD_NODES = """
UNWIND $nodes_list AS node
// Bắt buộc phải có title để làm khóa MERGE mới
WITH node
WHERE node.title IS NOT NULL
MERGE (n:ThucThe {title: node.title})
ON CREATE SET
    // Page ID giờ chỉ là thuộc tính, nếu null thì gán ID dự phòng
    n.page_id = CASE 
                WHEN node.page_id IS NOT NULL THEN node.page_id 
                ELSE 'TITLE_KEY_' + node.title 
                END,
    n.infobox = node.infobox,
    n.summary = node.summary
// Thêm nhãn phụ (Person/Event) từ dữ liệu
WITH n, node
CALL apoc.create.addLabels(n, [node.label]) YIELD node AS result
RETURN count(result) AS count
"""

# 4. Nạp Relationships (MATCH trên Title)

CYPHER_UPLOAD_RELS = """
UNWIND $rels_list AS rel
MATCH (a:ThucThe {title: rel.source})
MATCH (b:ThucThe {title: rel.target})
CALL apoc.create.relationship(
    a,
    rel.type,
    { evidence: rel.evidence },
    b
) YIELD rel AS result
RETURN count(result) AS count
"""

def upload_graph_to_local(uri, user, password, nodes_list, rels_list):
    """
    Kết nối với Neo4j Local, xóa dữ liệu cũ và nạp mới.
    """
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        print(f"✅ Đã kết nối với Neo4j Local tại {uri}")
    except Exception as e:
        print(f"❌ Lỗi kết nối Local: {e}")
        print("Vui lòng kiểm tra: Neo4j Desktop đã Start chưa? Mật khẩu đúng chưa?")
        return

    with driver.session() as session:
        
        # BƯỚC A: XÓA DỮ LIỆU CŨ
        print("1️⃣  Đang xóa sạch dữ liệu cũ (Reset Database)...")
        session.run(CYPHER_DELETE_ALL)
        print("   > Đã xóa xong.")

        # BƯỚC B: TẠO CONSTRAINT & INDEX
        print("2️⃣  Đang tạo Constraint và Index...")
        try:
            session.run(CYPHER_CREATE_CONSTRAINTS)
            session.run(CYPHER_CREATE_INDEX)
            print("   > Constraint/Index đã được thiết lập.")
        except Exception as e:
            print(f"   ⚠️ Cảnh báo tạo constraint: {e}")

        # BƯỚC C: UPLOAD NODES
        print(f"3️⃣  Đang upload {len(nodes_list)} Nodes...")
        try:
            # Batching: Nếu dữ liệu lớn (>10k), nên chia nhỏ. Ở đây giả định dữ liệu < 10k chạy 1 lần.
            result = session.run(CYPHER_UPLOAD_NODES, nodes_list=nodes_list)
            print(f"   > Đã xử lý nodes thành công.")
        except Exception as e:
            print(f"   ❌ LỖI KHI UPLOAD NODES: {e}")
            if "apoc" in str(e).lower():
                print("   💡 GỢI Ý: Bạn chưa cài APOC Plugin. Vào Neo4j Desktop -> Plugins -> Install APOC.")
            driver.close()
            return

        # BƯỚC D: UPLOAD RELATIONSHIPS
        print(f"4️⃣  Đang upload {len(rels_list)} Relationships...")
        try:
            result = session.run(CYPHER_UPLOAD_RELS, rels_list=rels_list)
            print(f"   > Đã xử lý relationships thành công.")
        except Exception as e:
            print(f"   ❌ Lỗi khi upload relationships: {e}")

    driver.close()

if __name__ == "__main__":
    print("--- 🚀 Bắt đầu nạp dữ liệu vào Neo4j Local ---")

    # Kiểm tra file tồn tại
    if not os.path.exists(JSON_NODES_IN) or not os.path.exists(CSV_RELS_IN):
        print(f"❌ Lỗi: Không tìm thấy file dữ liệu.")
        print(f"Kiểm tra đường dẫn: {JSON_NODES_IN}")
        print("Hãy chạy script 'build_full_network.py' trước.")
    else:
        try:
            with open(JSON_NODES_IN, 'r', encoding='utf-8') as f:
                nodes_list = json.load(f)
            rels_list = []
            with open(CSV_RELS_IN, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rels_list.append(row)
                
            if not nodes_list:
                print("❌ File Nodes bị rỗng!")
            else:
                # --- TIỀN XỬ LÝ (SERIALIZATION) ---
                print("⚙️  Đang chuẩn bị dữ liệu (Serialization Infobox)...")
                for node in nodes_list:
                    # Chuyển infobox từ dict sang string để lưu vào Neo4j
                    if 'infobox' in node and isinstance(node['infobox'], dict):
                        node['infobox'] = json.dumps(node['infobox'], ensure_ascii=False)
                    elif 'infobox' not in node:
                        node['infobox'] = "{}"
                    
                    # Đảm bảo có page_id (dùng title nếu thiếu)
                    if 'page_id' not in node:
                        node['page_id'] = node.get('title')

                # Chạy hàm upload
                upload_graph_to_local(LOCAL_URI, LOCAL_USER, LOCAL_PASSWORD, nodes_list, rels_list)
                print("\n--- 🎉 HOÀN TẤT! Hãy mở Neo4j Browser để kiểm tra. ---")

        except Exception as e:
            print(f"❌ Đã xảy ra lỗi không mong muốn: {e}")