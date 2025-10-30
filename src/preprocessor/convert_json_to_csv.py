import pandas as pd
import json
import os

# --- CẤU HÌNH ĐƯỜNG DẪN ---
BASE_PATH = '../../data/processed/'
JSON_NODES_IN = os.path.join(BASE_PATH, 'network_nodes_full.json')
JSON_RELS_IN = os.path.join(BASE_PATH, 'network_relationships_full.json')

CSV_NODES_OUT = os.path.join(BASE_PATH, 'nodes_for_neo4j.csv')
CSV_RELS_OUT = os.path.join(BASE_PATH, 'relationships_for_neo4j.csv')

def process_nodes():
    """
    Chuyển đổi file JSON chứa các node thành CSV.
    - Làm phẳng 'infobox' thành một chuỗi JSON.
    - Xử lý các giá trị 'page_id' bị thiếu (NaN).
    """
    print(f"Đang đọc file node từ: '{JSON_NODES_IN}'...")
    try:
        df = pd.read_json(JSON_NODES_IN)
    except Exception as e:
        print(f"Lỗi: Không thể đọc file JSON. {e}")
        return

    if df.empty:
        print("Cảnh báo: File JSON node bị rỗng.")
        return

    print("Đang xử lý các node...")
    
    # 1. Đổi tên cột 'label' thành ':LABEL' để Neo4j tự động nhận diện
    df.rename(columns={'label': ':LABEL'}, inplace=True)
    
    # 2. Đổi tên cột 'title' thành 'title:ID'
    # Đây là cách đặt tên "chuẩn" cho Neo4j để nó biết đây là ID duy nhất.
    df.rename(columns={'title': 'title:ID'}, inplace=True)

    # 3. Chuyển đổi cột 'infobox' (là dict) thành một chuỗi JSON
    # Neo4j có thể lưu trữ toàn bộ chuỗi này như một thuộc tính.
    df['infobox'] = df['infobox'].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else "{}"
    )
    
    # 4. Xử lý các page_id bị thiếu (NaN)
    # Thay thế NaN bằng 0 (hoặc một giá trị mặc định)
    df['page_id'] = df['page_id'].fillna(0).astype(int)
    
    # 5. Chọn các cột cuối cùng
    final_columns = ['title:ID', ':LABEL', 'page_id', 'infobox']
    df_final = df[final_columns]
    
    # 6. Lưu ra file CSV
    df_final.to_csv(CSV_NODES_OUT, index=False, encoding='utf-8')
    print(f"✅ Đã lưu file node CSV vào: '{CSV_NODES_OUT}'")

def process_relationships():
    """
    Chuyển đổi file JSON chứa các cạnh (relationships) thành CSV.
    Đổi tên cột cho phù hợp với Neo4j.
    """
    print(f"\nĐang đọc file cạnh từ: '{JSON_RELS_IN}'...")
    try:
        df = pd.read_json(JSON_RELS_IN)
    except Exception as e:
        print(f"Lỗi: Không thể đọc file JSON. {e}")
        return

    if df.empty:
        print("Cảnh báo: File JSON cạnh bị rỗng.")
        return
        
    print("Đang xử lý các cạnh...")
    
    # Đổi tên các cột cho chuẩn với Neo4j
    # :START_ID phải khớp với cột 'title:ID' trong file node
    # :END_ID cũng phải khớp với cột 'title:ID' trong file node
    # :TYPE là loại mối quan hệ
    df.rename(columns={
        'source': ':START_ID',
        'target': ':END_ID',
        'type': ':TYPE'
    }, inplace=True)
    
    # 2. Lưu ra file CSV
    df.to_csv(CSV_RELS_OUT, index=False, encoding='utf-8')
    print(f"✅ Đã lưu file cạnh CSV vào: '{CSV_RELS_OUT}'")

if __name__ == "__main__":
    print("--- Bắt đầu quá trình chuyển đổi JSON sang CSV cho Neo4j ---")
    process_nodes()
    process_relationships()
    print("\n--- Hoàn tất! ---")