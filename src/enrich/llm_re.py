"""
LLM-based relation extraction at sentence level.

Pipeline:
1) Đọc wikitext đã thu thập: articles_raw_wikitext.filtered.jsonl
2) Đọc graph hiện tại: network_nodes_full.enriched.json / relationships_full.enriched.json
3) Với mỗi bài viết:
   - Parse wikitext -> plain text, tách câu
   - Với mỗi câu, tìm các node khác xuất hiện trong câu (string match)
   - Hỏi LLM phân loại quan hệ giữa subject (bài viết) và target (node)
   - Nếu LLM trả về LIEN_KET hoặc trống, bỏ qua; ngược lại thêm cạnh, kèm evidence
4) Ghi đè relationships_enriched (JSON/CSV)

Yêu cầu: đặt biến môi trường OPENAI_API_KEY (hoặc chỉnh hàm call_llm để dùng provider khác).
Mặc định model: gpt-4o-mini. Bạn có thể đổi trong hàm call_llm.
"""

import json
import re
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple, Iterable

import mwparserfromhell
from openai import OpenAI
import google.generativeai as genai

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "processed"

ARTICLES_IN = DATA_DIR / "articles_raw_wikitext.filtered.jsonl"
NODES_IN = DATA_DIR / "network_nodes_full.enriched.json"
RELS_IN = DATA_DIR / "network_relationships_full.enriched.json"

RELS_OUT = DATA_DIR / "network_relationships_full.enriched.json"
RELS_CSV_OUT = DATA_DIR / "relationships_for_neo4j.enriched.csv"

MODEL_NAME_OPENAI = "gpt-4o-mini"
# Danh sách Gemini ưu tiên (sẽ thử tuần tự nếu gặp 404)
DEFAULT_GEMINI_MODELS = [
    "gemini-1.5-flash-001",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
    "gemini-1.0-pro",
    "gemini-pro",
]
RATE_LIMIT_DELAY = 0.5  # seconds between calls


def load_articles(path: Path) -> List[Dict]:
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                continue
    return items


def load_json(path: Path):
    return json.load(open(path, "r", encoding="utf-8"))


def save_rels_json(path: Path, rels: List[Dict]):
    json.dump(rels, open(path, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


def save_rels_csv(path: Path, rels: List[Dict]):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([":START_ID", ":END_ID", ":TYPE", "evidence"])
        for r in rels:
            w.writerow([r.get("source", ""), r.get("target", ""), r.get("type", ""), r.get("evidence", "")])


def build_rel_set(rels: List[Dict]) -> Set[Tuple[str, str, str]]:
    return {(r.get("source", ""), r.get("target", ""), r.get("type", "")) for r in rels}


def split_sentences(text: str) -> List[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]


PROMPT_TEMPLATE = """
Bạn là hệ thống gán nhãn quan hệ. Dựa vào câu sau, hãy xác định quan hệ giữa "{subject}" (chủ đề bài viết) và "{target}".
Chỉ trả về một nhãn ngắn gọn dạng SNAKE_CASE, viết hoa. Nếu không rõ, trả về LIEN_KET.
Câu: "{sentence}"
Ví dụ nhãn: DOI_DAU, PHOI_NGAU, LA_CON_CUA, KE_NHIEM, CHI_HUY, THAM_GIA, KY_HIEP_UOC, SINH_TAI, MAT_TAI.
"""


def call_llm_openai(client: OpenAI, subject: str, target: str, sentence: str) -> str:
    prompt = PROMPT_TEMPLATE.format(subject=subject, target=target, sentence=sentence)
    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME_OPENAI,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=20,
        )
        text = resp.choices[0].message.content.strip()
        label = re.split(r"\s|\n", text)[0]
        label = re.sub(r"[^A-Z_]", "", label.upper())
        return label if label else "LIEN_KET"
    except Exception as e:
        print(f"  ! LLM OpenAI lỗi: {e}")
        return "LIEN_KET"


def call_llm_gemini(subject: str, target: str, sentence: str, model_name: str) -> Tuple[str, bool]:
    """
    Trả về (label, is_404). is_404 dùng để biết có nên thử model kế tiếp.
    """
    prompt = PROMPT_TEMPLATE.format(subject=subject, target=target, sentence=sentence)
    try:
        model = genai.GenerativeModel(model_name)
        res = model.generate_content(prompt)
        text = (res.text or "").strip()
        label = re.split(r"\s|\n", text)[0]
        label = re.sub(r"[^A-Z_]", "", label.upper())
        return (label if label else "LIEN_KET", False)
    except Exception as e:
        msg = str(e)
        print(f"  ! LLM Gemini lỗi ({model_name}): {msg}")
        is_404 = "404" in msg or "not found" in msg.lower()
        return "LIEN_KET", is_404


def read_api_key(path: Path) -> str:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def load_gemini_models() -> List[str]:
    model_file = Path("GEMINI_MODEL.txt")
    if model_file.exists():
        lines = [ln.strip() for ln in model_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
        if lines:
            return lines
    return DEFAULT_GEMINI_MODELS


def main():
    provider_file = Path("LLM_PROVIDER.txt")
    provider = provider_file.read_text(encoding="utf-8").strip().lower() if provider_file.exists() else "openai"
    client = None
    gemini_models: List[str] = []

    if provider == "openai":
        api_key = read_api_key(Path("OPENAI_API_KEY.txt"))
        if not api_key:
            print("❌ Thiếu OPENAI_API_KEY (file OPENAI_API_KEY.txt).")
            return
        client = OpenAI(api_key=api_key)
    elif provider == "gemini":
        api_key = read_api_key(Path("GOOGLE_API_KEY.txt"))
        if not api_key:
            print("❌ Thiếu GOOGLE_API_KEY (file GOOGLE_API_KEY.txt).")
            return
        gemini_models = load_gemini_models()
        genai.configure(api_key=api_key)
        print(f"Gemini models sẽ thử lần lượt: {gemini_models}")
    else:
        print("❌ LLM_PROVIDER không hợp lệ. Dùng 'openai' hoặc 'gemini'.")
        return

    if not (ARTICLES_IN.exists() and NODES_IN.exists() and RELS_IN.exists()):
        print("❌ Thiếu file input.")
        return

    print("Đang tải dữ liệu...")
    articles = load_articles(ARTICLES_IN)
    nodes = load_json(NODES_IN)
    rels = load_json(RELS_IN)

    title_lookup: Dict[str, str] = {}
    for n in nodes:
        t = n.get("title")
        if t:
            title_lookup[t.lower()] = t

    rel_set = build_rel_set(rels)
    added = 0
    calls = 0

    for art in articles:
        src_title = art.get("title", "")
        wikitext = art.get("wikitext", "")
        if not src_title or not wikitext:
            continue

        wikicode = mwparserfromhell.parse(wikitext)
        plain_text = wikicode.strip_code()
        sentences = split_sentences(plain_text)

        for sent in sentences:
            sent_low = sent.lower()
            # tìm target xuất hiện trong câu
            matched_targets = [
                tgt_title for tgt_low, tgt_title in title_lookup.items()
                if tgt_title != src_title and tgt_low in sent_low and len(tgt_low) >= 4
            ]
            if not matched_targets:
                continue

            for tgt_title in matched_targets:
                rel_type = "LIEN_KET"
                if provider == "openai":
                    rel_type = call_llm_openai(client, src_title, tgt_title, sent)
                    calls += 1
                else:
                    # thử lần lượt các model cho đến khi hết hoặc không 404
                    for model_name in gemini_models:
                        rel_type, is_404 = call_llm_gemini(src_title, tgt_title, sent, model_name)
                        calls += 1
                        if is_404:
                            time.sleep(RATE_LIMIT_DELAY)
                            continue
                        else:
                            break

                if rel_type in ("", "LIEN_KET"):
                    time.sleep(RATE_LIMIT_DELAY)
                    continue
                key = (src_title, tgt_title, rel_type)
                if key in rel_set:
                    time.sleep(RATE_LIMIT_DELAY)
                    continue
                rel_set.add(key)
                rels.append({
                    "source": src_title,
                    "target": tgt_title,
                    "type": rel_type,
                    "evidence": sent,
                })
                added += 1
                time.sleep(RATE_LIMIT_DELAY)

    print(f"Đã thêm {added} cạnh ngữ nghĩa bằng LLM (tổng số call: {calls}).")
    save_rels_json(RELS_OUT, rels)
    save_rels_csv(RELS_CSV_OUT, rels)
    print("Đã lưu:")
    print(f"- {RELS_OUT}")
    print(f"- {RELS_CSV_OUT}")


if __name__ == "__main__":
    main()

