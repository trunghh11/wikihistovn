import xml.etree.cElementTree as ET
import mwparserfromhell
import json
import os

# --- ĐÃ CẬP NHẬT THEO YÊU CẦU ---
# Tập hạt giống: Dictionary chứa tên và Page ID của 13 vị vua triều Nguyễn.
NGUYEN_KINGS_PAGES = {
    11680: "Gia Long",
    11667: "Minh Mạng",
    41716: "Thiệu Trị",
    41279: "Tự Đức",
    41729: "Dục Đức",
    357286: "Hiệp Hòa",
    41740: "Kiến Phúc",
    41820: "Hàm Nghi",
    41744: "Đồng Khánh",
    41630: "Thành Thái",
    41862: "Duy Tân",
    42008: "Khải Định",
    15247: "Bảo Đại"
}

def extract_infobox_data(wikicode):
    """Trích xuất dữ liệu từ template Infobox đầu tiên tìm thấy."""
    infoboxes = wikicode.filter_templates(matches=lambda t: 'infobox' in t.name.lower())
    if not infoboxes:
        return None
    
    infobox = infoboxes[0]
    data = {}
    for param in infobox.params:
        key = param.name.strip()
        value = param.value.strip_code().strip()
        if value:
            data[key] = value
    return data

def create_seed_list_from_xml(dump_path, seed_ids, output_path):
    """
    Quét file XML, tìm các bài viết trong tập hạt giống bằng Page ID,
    và lưu kết quả vào một file JSON duy nhất.
    """
    print(f"👑 Bắt đầu quét file XML để xây dựng tập hạt giống (phiên bản ID đã cập nhật)...")
    
    # Dùng set để tìm kiếm ID nhanh hơn
    target_ids = set(seed_ids.keys())
    
    # Nơi lưu trữ dữ liệu của các vua tìm được
    seed_data = []
    
    # Mở file XML để đọc
    with open(dump_path, 'r', encoding='utf-8') as f:
        # Sử dụng iterparse để đọc XML theo từng phần, tiết kiệm bộ nhớ
        context = ET.iterparse(f, events=('end',))
        
        for event, elem in context:
            # Namespace của MediaWiki XML
            namespace = '{http://www.mediawiki.org/xml/export-0.10/}'
            
            # Khi một thẻ <page> kết thúc, chúng ta xử lý nó
            if elem.tag == f'{namespace}page':
                id_elem = elem.find(f'{namespace}id')
                page_id = int(id_elem.text)
                
                # Kiểm tra xem page_id có nằm trong tập hạt giống không
                if page_id in target_ids:
                    title_elem = elem.find(f'{namespace}title')
                    text_elem = elem.find(f'{namespace}revision/{namespace}text')
                    
                    if title_elem is None or text_elem is None or text_elem.text is None:
                        elem.clear()
                        continue
                        
                    title = title_elem.text
                    print(f"  -> Tìm thấy: {title} (ID: {page_id})")
                    
                    # Phân tích wikitext
                    wikicode = mwparserfromhell.parse(text_elem.text)
                    
                    # Trích xuất dữ liệu
                    infobox = extract_infobox_data(wikicode)
                    links = [link.title.strip() for link in wikicode.filter_wikilinks()]
                    
                    # Thêm dữ liệu vào danh sách kết quả
                    seed_data.append({
                        'page_id': page_id,
                        'title': title,
                        'infobox': infobox,
                        'links': links
                    })
                    
                    # Xóa ID đã tìm thấy để tăng tốc và dừng sớm
                    target_ids.remove(page_id)
                    
                # Giải phóng bộ nhớ
                elem.clear()
                
                # Nếu đã tìm thấy tất cả, dừng việc đọc file
                if not target_ids:
                    print("\n🎉 Đã tìm thấy tất cả các vị vua trong tập hạt giống! Dừng xử lý.")
                    break
    
    print(f"\n✅ Hoàn thành! Tìm thấy {len(seed_data)}/{len(NGUYEN_KINGS_PAGES)} nhân vật.")

    # Lưu danh sách ban đầu vào một file JSON mới
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(seed_data, f, indent=4, ensure_ascii=False)
    
    print(f"Danh sách hạt giống ban đầu đã được lưu tại: '{output_path}'")

if __name__ == '__main__':
    XML_DUMP_PATH = '../../data/raw/viwiki-latest-pages-articles.xml'
    OUTPUT_SEED_PATH = '../../data/processed/seed_data_nguyen_kings.json'
    
    if not os.path.exists(XML_DUMP_PATH):
        print(f"❌ Lỗi: Không tìm thấy file XML tại '{XML_DUMP_PATH}'.")
        print("Hãy chắc chắn rằng bạn đã giải nén file dump vào đúng thư mục.")
    else:
        create_seed_list_from_xml(XML_DUMP_PATH, NGUYEN_KINGS_PAGES, OUTPUT_SEED_PATH)