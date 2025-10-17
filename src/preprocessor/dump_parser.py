import bz2
import xml.etree.cElementTree as ET
import mwparserfromhell
import json
import os
import time
from tqdm import tqdm  # thêm thư viện này để hiển thị tiến trình

def extract_infobox_data(wikicode):
    """
    Trích xuất dữ liệu từ template Infobox đầu tiên tìm thấy.
    """
    infoboxes = wikicode.filter_templates(matches=lambda t: 'infobox' in t.name.lower())
    if not infoboxes:
        return None
    
    infobox = infoboxes[0]
    data = {'template_name': infobox.name.strip()}
    for param in infobox.params:
        key = param.name.strip()
        value = param.value.strip_code().strip()
        if value:
            data[key] = value
    return data

def count_pages_in_dump(dump_path):
    """
    Đếm số lượng thẻ <page> trong file dump để ước lượng tiến trình.
    """
    count = 0
    with bz2.open(dump_path, 'rt', encoding='utf-8') as bz2f:
        for line in bz2f:
            if '<page>' in line:
                count += 1
    return count

def process_wikipedia_dump(dump_path, output_path):
    """
    Đọc file dump của Wikipedia, xử lý từng bài viết và lưu
    thông tin cần thiết (tiêu đề, infobox, liên kết) vào file JSON Lines.
    """
    print("🔄 Đang đếm tổng số trang trong dump để hiển thị tiến trình (có thể mất vài phút)...")
    total_pages = count_pages_in_dump(dump_path)
    print(f"➡️  Ước lượng khoảng {total_pages:,} trang cần xử lý.\n")

    start_time = time.time()
    processed_with_infobox = 0

    with bz2.open(dump_path, 'rt', encoding='utf-8') as bz2f, \
         open(output_path, 'w', encoding='utf-8') as out_f, \
         tqdm(total=total_pages, desc="Đang xử lý", unit="trang") as pbar:

        context = ET.iterparse(bz2f, events=('end',))
        namespace = '{http://www.mediawiki.org/xml/export-0.10/}'
        
        for event, elem in context:
            if elem.tag == f'{namespace}page':
                title_elem = elem.find(f'{namespace}title')
                text_elem = elem.find(f'{namespace}revision/{namespace}text')
                
                if title_elem is None or text_elem is None or text_elem.text is None:
                    elem.clear()
                    pbar.update(1)
                    continue

                title = title_elem.text
                if ':' in title:  # bỏ trang đặc biệt
                    elem.clear()
                    pbar.update(1)
                    continue

                wikicode = mwparserfromhell.parse(text_elem.text)
                infobox = extract_infobox_data(wikicode)

                if infobox:
                    links = [link.title.strip() for link in wikicode.filter_wikilinks()]
                    page_data = {
                        'title': title,
                        'infobox': infobox,
                        'links': links
                    }
                    out_f.write(json.dumps(page_data, ensure_ascii=False) + '\n')
                    processed_with_infobox += 1

                elem.clear()
                pbar.update(1)

    elapsed = time.time() - start_time
    print(f"\n✅ Hoàn thành!")
    print(f"- Tổng số trang xử lý: {total_pages:,}")
    print(f"- Số trang có Infobox: {processed_with_infobox:,}")
    print(f"- Tỷ lệ: {processed_with_infobox / total_pages * 100:.2f}%")
    print(f"- Thời gian chạy: {elapsed / 60:.2f} phút")
    print(f"- Tốc độ trung bình: {total_pages / elapsed:.2f} trang/giây")
    print(f"Dữ liệu đã được lưu tại: {output_path}")

if __name__ == '__main__':
    DUMP_FILE = '../../data/raw/viwiki-latest-pages-articles.xml.bz2'
    OUTPUT_FILE = '../../data/processed/viwiki_extracted.jsonl'

    if not os.path.exists(DUMP_FILE):
        print(f"❌ Lỗi: Không tìm thấy file dump tại '{DUMP_FILE}'.")
        print("Vui lòng tải file và đặt vào đúng thư mục.")
    else:
        process_wikipedia_dump(DUMP_FILE, OUTPUT_FILE)
