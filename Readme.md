# Mạng lưới các nhân vật lịch sử và sự kiện của Việt Nam trong triều đại nhà Nguyễn

Tác vụ: Xây dựng đồ thị tri thức từ Wikipedia tiếng Việt về thời Nguyễn, xuất dữ liệu dưới dạng JSON/CSV và nạp vào Neo4j để trực quan hóa và truy vấn.

---

## I. Giới thiệu chung

### a. Giới thiệu công việc
- Thu thập và chuẩn hóa dữ liệu từ Wikipedia (dump và API).
- Xây mạng lưới (nodes/edges) cho nhân vật và sự kiện thời Nguyễn.
- Đưa dữ liệu vào Neo4j để phân tích mối quan hệ.

### b. Giới thiệu qua bài toán
- Đầu vào:
  - Dump: `data/raw/viwiki-latest-pages-articles.xml`
  - API Wikipedia (vi.wikipedia.org/w/api.php)
  - Seed (13 vua): `data/processed/seed_data_nguyen_kings_from_api.json`
- Đầu ra:
  - Nodes: `data/processed/network_nodes_full.json`
  - Edges: `data/processed/network_relationships_full.json`
  - Bản CSV phục vụ Neo4j: `data/processed/nodes.csv`, `data/processed/relationships_for_neo4j.csv`

---

## II. Yêu cầu bài toán

### a. Giới thiệu chi tiết mạng đã xây dựng
- Node (Thực thể):
  - Mỗi trang Wikipedia liên quan thời Nguyễn → 1 node.
  - Thuộc tính chính: `page_id` (khóa chính, ổn định), `title`, `label`, `infobox` (thô).
  - Nhãn:
    - Vua hạt giống: “Vua Nhà Nguyễn”
    - Các neighbor: phân loại “Nhân vật Lịch sử”, “Sự kiện” (từ khóa trong nội dung/infobox).
- Edge (Quan hệ có hướng, type):
  - Trích từ Infobox theo các ánh xạ: kế vị, thân phụ, phối ngẫu, phục vụ, chỉ huy, tham chiến…
  - Ưu tiên tạo cạnh theo `page_id` (ổn định hơn `title`).
  - Fallback “LIÊN_KẾT_TỚI” cho các cặp nút có liên kết nhưng chưa suy ra quan hệ ngữ nghĩa.

### b. Lựa chọn nodes và lựa chọn edges
- Nodes:
  - Lớp 1 (Seeds): 13 vua từ file seed.
  - Lớp 2 (Neighbors): các trang được tham chiếu từ Infobox/links của seeds (đã lọc hợp lệ).
- Edges:
  - Nội tộc (KING_REL_MAP) và Ngoại tộc (NEIGHBOR_REL_MAP).
  - Hỗ trợ khóa động trong infobox (ví dụ “chỉ huy 1/2/3” khớp “chỉ huy”).
  - Quan hệ đối xứng (PHỐI_NGẪU_VỚI) được khử trùng lặp theo cặp vô hướng.

---

## III. Tóm tắt lý thuyết, stack công nghệ để thu thập, xử lý dữ liệu wiki

### a. Tóm tắt lý thuyết, giới thiệu công nghệ
- Wikitext & Infobox: chứa cấu trúc trường–giá trị dùng suy luận quan hệ.
- Đồ thị tri thức: mô hình node/edge/type cho truy vấn quan hệ và đo lường trung tâm.
- Neo4j & Cypher: cơ sở dữ liệu đồ thị, trực quan hóa, phân tích.

### b. Áp dụng công nghệ
- Python stack:
  - `requests` (API), `mwparserfromhell` (parse wikitext), `re`, `json`, `csv`.
- Mã nguồn chính:
  - `src/preprocessor/build_seed_file_from_xml.py` (tùy chọn)
  - `src/preprocessor/build_seed_data_from_api.py` (tạo seed)
  - `src/preprocessor/build_full_network.py` (xây mạng 2 lớp)
  - `src/preprocessor/convert_json_to_csv.py` hoặc `build_graph_csvs.py` (xuất CSV)
  - `src/preprocessor/upload_to_neo4j_aura.py` (nạp Neo4j)
- Neo4j:
  - Aura Free/Desktop. Lưu ý Aura Free hạn chế quyền tạo constraint.

---

## IV. Tiền xử lý dữ liệu, các thống kê về mạng đã xây dựng được

### a. Tiền xử lý dữ liệu
- Phân loại nhãn:
  - Person keywords: vua/hoàng đế/hoàng hậu/quan/tướng/nhân vật/…
  - Event keywords: trận/khởi nghĩa/chiến dịch/hiệp ước/hòa ước/hội nghị/…
- Làm sạch giá trị Infobox:
  - Tách theo `<br>`/newline; loại chú thích `[]`/`()`; bỏ wikitext; chuẩn hóa khoảng trắng.
  - Hỗ trợ khóa động: duyệt mọi key trong infobox có chứa cụm (vd: “chỉ huy” khớp “chỉ huy 1/2”).
- Xác định quan hệ bằng `page_id`:
  - Ưu tiên `page_id`; nếu chỉ có `title`, gọi API để lấy `page_id`.
  - Khử trùng lặp cạnh bằng `rel_set`; tôn trọng đối xứng/phi đối xứng.
- Lọc links hợp lệ:
  - Bỏ không gian tên ngoài main, anchor, trang mơ hồ/tổng hợp khi có thể.
- Xuất dữ liệu:
  - JSON: `data/processed/network_nodes_full.json`, `data/processed/network_relationships_full.json`
  - CSV: `data/processed/nodes.csv`, `data/processed/relationships_for_neo4j.csv`

### b. Các thống kê (hướng dẫn tạo nhanh)
Sau khi chạy pipeline, có thể in thống kê cơ bản:

```python
# Chạy trong Python REPL hoặc notebook
import json, os, collections
base = 'data/processed'
nodes = json.load(open(os.path.join(base,'network_nodes_full.json'),'r',encoding='utf-8'))
rels  = json.load(open(os.path.join(base,'network_relationships_full.json'),'r',encoding='utf-8'))

print('Số node:', len(nodes))
print('Số edge:', len(rels))
print('Theo label:', collections.Counter(n.get('label','Khác') for n in nodes))
print('Theo loại quan hệ (top 10):', collections.Counter(r['type'] for r in rels).most_common(10))
```

Một số truy vấn Cypher hữu ích trong Neo4j:
- Đếm node/edge:
  - `MATCH (n:ThucThe) RETURN count(n);`
  - `MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS c ORDER BY c DESC;`
- Top node theo bậc:
  - `MATCH (n)-[r]-() RETURN n.title AS title, count(r) AS deg ORDER BY deg DESC LIMIT 10;`
- Kế vị:
  - `MATCH (a)-[:KẾ_NHIỆM_CỦA]->(b) RETURN a.title AS ke_nhiem, b.title AS tien_nhiem LIMIT 20;`

---

## V. Kết quả đạt được, hướng phát triển

### a. Kỹ năng & kiến thức thu thập được
- Pipeline bóc tách tri thức từ Wikipedia (Wikitext → Infobox → Graph).
- Chuẩn hóa định danh theo `page_id`, khử trùng lặp quan hệ, xử lý khóa động.
- Nạp và truy vấn đồ thị trong Neo4j, xây truy vấn phân tích cơ bản.

### b. Hướng phát triển tiếp theo để hoàn thiện giải pháp
- Mở rộng ánh xạ Infobox (thêm quan hệ: THAM_GIA_TRẬN, KÝ_HIỆP_ƯỚC_VỚI, LIÊN_MINH_VỚI…).
- Kết hợp NLP cho văn bản tự do để trích quan hệ ngoài Infobox.
- Bộ nhớ đệm `page_id` và fetch theo batch để giảm số lần gọi API.
- Chuẩn hóa tên: không dấu/biến thể, đồng bộ đổi tên trang.
- Đo lường chất lượng: tỉ lệ cạnh từ Infobox vs fallback; lý do edge bị loại (thiếu node, trùng lặp…).
- Xây dashboard trực quan (Neo4j Bloom/Graph Apps hoặc web app nhẹ).

---

## Phụ lục

### A. Cách chạy pipeline (Mac/VS Code)
```bash
# 1) (Tùy chọn) tạo/cập nhật seed từ API
python src/preprocessor/build_seed_data_from_api.py

# 2) Xây mạng đầy đủ (JSON)
python src/preprocessor/build_full_network.py

# 4) Nạp Neo4j Aura/Desktop
python src/preprocessor/upload_to_neo4j_aura.py
```
Lưu ý: Neo4j Aura Free không cho tạo constraint. Trong `upload_to_neo4j_aura.py` đã bỏ qua bước này.

### B. Thư mục dữ liệu
```
data/
  raw/viwiki-latest-pages-articles.xml
  processed/
    seed_data_nguyen_kings_from_api.json
    network_nodes_full.json
    network_relationships_full.json
    nodes.csv
    relationships_for_neo4j.csv
```

### C. Ghi chú kỹ thuật chính từ code
- `build_full_network.py`:
  - Dùng `title` để lưu quan hệ;
  - Duyệt khóa động trong infobox: tìm các key chứa cụm (ví dụ: “chỉ huy” khớp “chỉ huy 1/2”).
  - Khử trùng lặp bằng `rel_set`; xử lý đối xứng cho các quan hệ như `PHỐI_NGẪU_VỚI`.
- `upload_to_neo4j_aura.py`:
  - Mở session và UNWIND danh sách nodes/relationships để nạp; bỏ bước tạo constraint trên Aura Free.

---