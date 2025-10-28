import requests
from bs4 import BeautifulSoup
import json

def get_infobox_data(page_title):
    """
    Lấy và bóc tách dữ liệu Infobox từ một trang Wikipedia tiếng Việt.
    """
    URL = f"https://vi.wikipedia.org/wiki/{page_title}"
    
    # === THÊM DÒNG NÀY ===
    # Giả mạo làm trình duyệt Chrome trên Windows 10
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # === SỬA DÒNG NÀY ===
        # Thêm headers=headers vào yêu cầu
        page = requests.get(URL, headers=headers) 
        page.raise_for_status() # Báo lỗi nếu request hỏng
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải trang {URL}: {e}")
        return None

    soup = BeautifulSoup(page.content, "html.parser")
    
    # Infobox trên Wikipedia thường là 1 <table> có class 'infobox'
    infobox = soup.find("table", class_="infobox")
    
    if not infobox:
        print(f"Không tìm thấy Infobox cho trang {page_title}")
        return None

    infobox_data = {}
    
    # Duyệt qua từng hàng (<tr>) trong Infobox
    for row in infobox.find_all("tr"):
        # Tìm key (thường nằm trong <th>)
        header = row.find("th")
        # Tìm value (thường nằm trong <td>)
        data = row.find("td")
        
        if header and data:
            # Làm sạch text, loại bỏ các thẻ <br>, <span>...
            key = header.get_text(strip=True)
            # Lấy text của value, ưu tiên lấy text của link <a> nếu có
            value_link = data.find("a")
            if value_link:
                value = value_link.get_text(strip=True)
            else:
                value = data.get_text(strip=True)
                
            # Loại bỏ các chú thích [1], [2]...
            value = value.split('[')[0].strip() # Thêm .strip() để xóa khoảng trắng
            
            if key and value:
                infobox_data[key] = value

    return infobox_data

# === Chạy thử ===
# Bạn phải thêm phần này để code chạy được
if __name__ == "__main__":
    page_title = "Minh Mạng"
    data = get_infobox_data(page_title)

    if data:
        print(f"--- Dữ liệu Infobox của: {page_title} ---")
        # In ra dạng JSON cho dễ nhìn
        print(json.dumps(data, indent=2, ensure_ascii=False))