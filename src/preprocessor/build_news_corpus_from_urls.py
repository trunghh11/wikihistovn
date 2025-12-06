import json
import os
import time
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from config_paths import DATA_RAW, DATA_PROCESSED


# === CẤU HÌNH ĐƯỜNG DẪN ===

# File input: mỗi dòng là một URL bài báo
NEWS_URLS_TXT = os.path.join(DATA_RAW, "news_urls.txt")

# File output: corpus báo chí dạng JSON Lines
NEWS_CORPUS_OUT = os.path.join(DATA_PROCESSED, "news_corpus.jsonl")


HEADERS = {
    "User-Agent": "VietnameseHistoryNewsCollector/1.0 (Project for university; contact: 22024527@vnu.edu.vn)"
}


def load_urls(path: str) -> List[str]:
    """Đọc danh sách URL từ file txt (mỗi dòng 1 URL, bỏ dòng trống / comment #)."""
    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)
    print(f"Đã đọc {len(urls)} URL bài báo từ {path}")
    return urls


def extract_meta(soup: BeautifulSoup, *names_or_props: str) -> str:
    """Tiện ích lấy meta theo name/property trong HTML."""
    for key in names_or_props:
        tag = soup.find("meta", attrs={"name": key}) or soup.find(
            "meta", attrs={"property": key}
        )
        if tag and tag.get("content"):
            return tag["content"].strip()
    return ""


def extract_article_text(soup: BeautifulSoup) -> str:
    """
    Cố gắng bóc phần thân bài báo.
    - Ưu tiên thẻ <article>, sau đó các <div> có class gợi ý ('content', 'article', 'body').
    - Fallback: ghép tất cả các <p> trong body.
    """
    # 1. <article>
    article = soup.find("article")
    if article:
        paragraphs = [p.get_text(" ", strip=True) for p in article.find_all("p")]
        text = "\n".join(p for p in paragraphs if p)
        if text:
            return text

    # 2. div với class 'content', 'article', 'body'
    candidate_classes = ["content", "article", "body", "main-content", "news-content"]
    for div in soup.find_all("div"):
        class_list = " ".join(div.get("class", [])).lower()
        if any(c in class_list for c in candidate_classes):
            paragraphs = [p.get_text(" ", strip=True) for p in div.find_all("p")]
            text = "\n".join(p for p in paragraphs if p)
            if text:
                return text

    # 3. Fallback: tất cả <p> trong body
    body = soup.find("body") or soup
    paragraphs = [p.get_text(" ", strip=True) for p in body.find_all("p")]
    text = "\n".join(p for p in paragraphs if p)
    return text


def fetch_article(url: str) -> Dict:
    """Tải và bóc tách 1 bài báo. Trả về dict với title, published_date, source, text."""
    print(f"  > Đang tải: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"    ! Lỗi khi tải URL: {e}")
        return {}

    soup = BeautifulSoup(resp.content, "html.parser")

    # Tiêu đề
    title = extract_meta(soup, "og:title", "twitter:title") or (
        soup.title.get_text(strip=True) if soup.title else ""
    )

    # Ngày đăng (best-effort, tùy từng báo)
    published = extract_meta(
        soup,
        "article:published_time",
        "pubdate",
        "publishdate",
        "date",
        "dcterms.date",
    )

    # Nguồn (domain)
    try:
        from urllib.parse import urlparse

        source = urlparse(url).netloc
    except Exception:
        source = ""

    text = extract_article_text(soup)

    if not text:
        print("    ⚠️ Không bóc được nội dung chính, bỏ qua.")
        return {}

    return {
        "url": url,
        "source": source,
        "title": title,
        "published": published,
        "text": text,
    }


def build_news_corpus(urls_path: str, out_path: str) -> None:
    """
    Từ danh sách URL báo chí, xây một corpus báo chí dạng JSON Lines để làm giàu dữ liệu.

    Mỗi dòng trong out_path:
      {
        "id": int,
        "url": "...",
        "source": "vnexpress.net",
        "title": "...",
        "published": "2024-01-01T10:00:00+07:00" (nếu bóc được),
        "text": "Nội dung chính của bài báo..."
      }
    """
    urls = load_urls(urls_path)
    if not urls:
        print("❌ Không có URL nào trong file news_urls.txt.")
        return

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    written = 0
    with open(out_path, "w", encoding="utf-8") as fout:
        for idx, url in enumerate(urls, start=1):
            article = fetch_article(url)
            # Lịch sự với server
            time.sleep(1)
            if not article:
                continue

            record = {"id": idx, **article}
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1

    print(f"\n✅ Đã ghi {written}/{len(urls)} bài báo vào: {out_path}")


if __name__ == "__main__":
    print("--- 🚀 Xây dựng corpus báo chí từ danh sách URL ---")
    if not os.path.exists(NEWS_URLS_TXT):
        print(f"❌ Không tìm thấy file URL tại: {NEWS_URLS_TXT}")
        print("   Hãy tạo file 'data/raw/news_urls.txt', mỗi dòng là một URL bài báo cần thu thập.")
    else:
        try:
            build_news_corpus(NEWS_URLS_TXT, NEWS_CORPUS_OUT)
            print("\n--- Hoàn tất build_news_corpus_from_urls ---")
        except Exception as e:
            print(f"❌ Đã xảy ra lỗi: {e}")


