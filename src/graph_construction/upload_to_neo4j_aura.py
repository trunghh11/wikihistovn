import json
import os
from neo4j import GraphDatabase
from typing import List, Dict

# --- ⚠️ BƯỚC 1: ĐIỀN THÔNG TIN CỦA BẠN VÀO ĐÂY ---
AURA_URI = "neo4j+s://70915764.databases.neo4j.io"
AURA_USER = "neo4j"
AURA_PASSWORD = "Lxe23lDlLrKWtl-GI_Ui0jtO5ndvSUU2_wwwjb0X2sg"
# --------------------------------------------------

# Đường dẫn file dữ liệu
BASE_PATH = '../../data/processed/'
JSON_NODES_IN = os.path.join(BASE_PATH, 'network_nodes_full.json')
JSON_RELS_IN = os.path.join(BASE_PATH, 'network_relationships_full.json')

# --- CÁC CÂU LỆNH CYPHER (ĐÚNG) ---
# Các câu lệnh Cypher này là ĐÚNG và không cần thay đổi,
# vì chúng ta sẽ sửa đổi dữ liệu Python *trước khi* gửi đi.

CYPHER_CREATE_CONSTRAINTS = """
CREATE CONSTRAINT unique_entity_title IF NOT EXISTS
FOR (n:ThucThe) REQUIRE (n.title) IS UNIQUE;
"""

CYPHER_UPLOAD_NODES = """
UNWIND $nodes_list AS node
MERGE (n:ThucThe {title: node.title})
ON CREATE SET
    n.page_id = node.page_id,
    n.infobox = node.infobox
WITH n, node
CALL apoc.create.addLabels(n, [node.label]) YIELD node AS result
RETURN count(result) AS count
"""

CYPHER_UPLOAD_RELS = """
UNWIND $rels_list AS rel
MATCH (a:ThucThe {title: rel.source})
MATCH (b:ThucThe {title: rel.target})
CALL apoc.create.relationship(a, rel.type, {}, b) YIELD rel AS result
RETURN count(result) AS count
"""

def upload_graph_to_aura(uri, user, password, nodes_list, rels_list):
    """
    Kết nối với Neo4j Aura và upload toàn bộ dữ liệu.
    """
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        print("Đã kết nối với Neo4j Aura.")
    except Exception as e:
        print(f"Lỗi kết nối Aura: {e}")
        print("Vui lòng kiểm tra lại AURA_URI, AURA_USER, AURA_PASSWORD.")
        return

    with driver.session(database="neo4j") as session:
        
        # print("Đang tạo constraint (khóa chính)...")
        # try:
        #     session.run(CYPHER_CREATE_CONSTRAINTS)
        #     print(" > Constraint đã được tạo.")
        # except Exception as e:
        #     print(f"Lỗi khi tạo constraint: {e}")
        #     driver.close()
        #     return
        
        print("Đang upload các Nodes...")
        try:
            result = session.run(CYPHER_UPLOAD_NODES, nodes_list=nodes_list)
            print(f" > Đã upload {result.single()['count']} nodes.")
        except Exception as e:
            print(f"LOKHI UPLOAD NODES: {e}") # Sửa lỗi chính tả
            driver.close()
            return

        print("Đang upload các Relationships...")
        try:
            result = session.run(CYPHER_UPLOAD_RELS, rels_list=rels_list)
            print(f" > Đã upload {result.single()['count']} relationships.")
        except Exception as e:
            print(f"Lỗi khi upload relationships: {e}")

    driver.close()

if __name__ == "__main__":
    print("--- 🚀 Bắt đầu upload graph lên Neo4j Aura ---")

    if AURA_URI == "neo4j+s://your-database-uri.databases.neo4j.io":
        print("="*50)
        print("‼️ LỖI: Bạn chưa điền thông tin đăng nhập.")
        print("Vui lòng mở file 'upload_to_aura.py' và điền")
        print("AURA_URI, AURA_USER, và AURA_PASSWORD ở đầu file.")
        print("="*50)
    else:
        try:
            with open(JSON_NODES_IN, 'r', encoding='utf-8') as f:
                nodes_list = json.load(f)
            with open(JSON_RELS_IN, 'r', encoding='utf-8') as f:
                rels_list = json.load(f)
                
            if not nodes_list or not rels_list:
                print("Lỗi: File JSON node hoặc relationship bị rỗng.")
            else:
                
                # --- PHẦN SỬA LỖI ---
                # Chuyển đổi infobox (dict) thành (string)
                print(" > Đang chuẩn bị dữ liệu (Serialization Infobox)...")
                for node in nodes_list:
                    if 'infobox' in node and isinstance(node['infobox'], dict):
                        # Chuyển đổi dictionary thành một chuỗi JSON
                        node['infobox'] = json.dumps(node['infobox'], ensure_ascii=False)
                    elif 'infobox' not in node or not isinstance(node['infobox'], str):
                        # Đảm bảo thuộc tính tồn tại và là string
                        node['infobox'] = "{}"
                print(" > Chuẩn bị dữ liệu hoàn tất.")
                # --- KẾT THÚC PHẦN SỬA LỖI ---

                # 3. Gọi hàm upload với danh sách node đã được làm sạch
                upload_graph_to_aura(AURA_URI, AURA_USER, AURA_PASSWORD, nodes_list, rels_list)
                print("\n--- 🎉 Hoàn tất! Dữ liệu của bạn đã có trên cloud. ---")

        except FileNotFoundError:
            print(f"Lỗi: Không tìm thấy file dữ liệu tại '{JSON_NODES_IN}' hoặc '{JSON_RELS_IN}'")
            print("Vui lòng chạy script 'build_full_network.py' trước.")
        except Exception as e:
            print(f"Đã xảy ra lỗi: {e}")