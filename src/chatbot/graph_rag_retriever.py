import json
import os
import re
import unicodedata
from collections import defaultdict, Counter, deque
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Optional

from src.common.config_paths import DATA_PROCESSED


# === ĐƯỜNG DẪN DỮ LIỆU ===

NODES_IN = os.path.join(DATA_PROCESSED, "network_nodes_ranked.json")
RELS_BASE_IN = os.path.join(DATA_PROCESSED, "network_relationships_full.json")
RELS_TEXT_IN = os.path.join(DATA_PROCESSED, "network_relationships_text_based.json")
TEXTS_IN = os.path.join(DATA_PROCESSED, "network_nodes_texts.jsonl")
NEWS_IN = os.path.join(DATA_PROCESSED, "news_corpus.jsonl")


def strip_accents(text: str) -> str:
    """
    Chuẩn hóa tiếng Việt: bỏ dấu, dùng để so khớp từ khóa đơn giản.
    """
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", text)


def normalize_for_match(text: str) -> str:
    """
    Normalize cho việc tìm kiếm: lower + bỏ dấu + rút gọn khoảng trắng.
    """
    text = text.lower()
    text = strip_accents(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


@dataclass
class NodeInfo:
    title: str
    label: str
    pagerank: float
    combined_rank: int
    community_id: int
    community_size: int
    page_id: Optional[int] = None


class GraphRAGIndex:
    """
    Chỉ mục đơn giản cho GraphRAG:
      - load node (kèm pagerank, cộng đồng),
      - load cạnh (gốc + text),
      - load văn bản cho node + news,
      - cung cấp API: tìm seed, mở rộng subgraph, build context.
    """

    def __init__(
        self,
        nodes: Dict[str, NodeInfo],
        adj_undirected: Dict[str, Set[str]],
        node_texts: Dict[str, Dict[str, str]],
        news_docs: List[Dict],
    ):
        self.nodes = nodes
        self.adj = adj_undirected
        self.node_texts = node_texts
        self.news_docs = news_docs

        # Index phục vụ search theo keyword
        self.title_index: Dict[str, Set[str]] = defaultdict(set)
        for title in nodes.keys():
            norm = normalize_for_match(title)
            self.title_index[norm].add(title)

    # ---------- LOAD TỪ FILE ----------

    @classmethod
    def from_files(cls) -> "GraphRAGIndex":
        # 1. Load nodes
        with open(NODES_IN, "r", encoding="utf-8") as f:
            raw_nodes = json.load(f)
        nodes: Dict[str, NodeInfo] = {}
        for n in raw_nodes:
            title = n.get("title")
            if not title:
                continue
            nodes[title] = NodeInfo(
                title=title,
                label=n.get("label", "Thực thể"),
                pagerank=float(n.get("pagerank", 0.0)),
                combined_rank=int(n.get("combined_rank", 0)),
                community_id=int(n.get("community_id", 0)),
                community_size=int(n.get("community_size", 1)),
                page_id=n.get("page_id"),
            )

        # 2. Load relationships (gốc + text) → đồ thị vô hướng
        adj: Dict[str, Set[str]] = defaultdict(set)
        for path in (RELS_BASE_IN, RELS_TEXT_IN):
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as f:
                rels = json.load(f)
            for r in rels:
                s = r.get("source")
                t = r.get("target")
                if not s or not t:
                    continue
                if s not in nodes or t not in nodes:
                    continue
                if s == t:
                    continue
                adj[s].add(t)
                adj[t].add(s)

        # 3. Load văn bản cho node
        node_texts: Dict[str, Dict[str, str]] = {}
        if os.path.exists(TEXTS_IN):
            with open(TEXTS_IN, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    title = rec.get("title")
                    if not title or title not in nodes:
                        continue
                    node_texts[title] = {
                        "intro_text": rec.get("intro_text", "") or "",
                        "plain_text": rec.get("plain_text", "") or "",
                    }

        # 4. Load corpus báo chí (tuỳ chọn)
        news_docs: List[Dict] = []
        if os.path.exists(NEWS_IN):
            with open(NEWS_IN, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        news_docs.append(rec)
                    except json.JSONDecodeError:
                        continue

        print(
            f"[GraphRAGIndex] Loaded {len(nodes)} nodes, {sum(len(v) for v in adj.values()) // 2} edges,"
            f" {len(node_texts)} nodes with text, {len(news_docs)} news docs."
        )

        return cls(nodes=nodes, adj_undirected=adj, node_texts=node_texts, news_docs=news_docs)

    # ---------- TÌM SEED NODES ----------

    def search_seeds(self, question: str, top_k: int = 5) -> List[str]:
        """
        Tìm node seed từ câu hỏi dựa trên:
          - match tiêu đề (không dấu + lower),
          - đếm tần suất match, ưu tiên PageRank cao.
        """
        q_norm = normalize_for_match(question)
        tokens = [tok for tok in q_norm.split(" ") if tok]

        scores: Counter = Counter()
        for title in self.nodes.keys():
            t_norm = normalize_for_match(title)
            # đếm số token của câu hỏi xuất hiện trong title
            match_count = sum(1 for tok in tokens if tok in t_norm)
            if match_count > 0:
                scores[title] = match_count

        if not scores:
            return []

        # Kết hợp match_score + pagerank nhỏ
        def sort_key(item):
            title, score = item
            pr = self.nodes[title].pagerank
            return (score, pr)

        ranked = sorted(scores.items(), key=sort_key, reverse=True)
        seeds = [title for title, _ in ranked[:top_k]]
        return seeds

    # ---------- MỞ RỘNG SUBGRAPH ----------

    def expand_subgraph(
        self,
        seed_titles: List[str],
        max_depth: int = 2,
        max_nodes: int = 50,
    ) -> Set[str]:
        """
        BFS từ tập seed trên đồ thị vô hướng, lấy hàng xóm tới độ sâu max_depth,
        giới hạn tổng số node để context không quá lớn.
        """
        visited: Set[str] = set()
        queue = deque()

        for s in seed_titles:
            if s in self.nodes:
                visited.add(s)
                queue.append((s, 0))

        while queue and len(visited) < max_nodes:
            node, dist = queue.popleft()
            if dist >= max_depth:
                continue
            for nei in self.adj.get(node, []):
                if nei not in visited:
                    visited.add(nei)
                    queue.append((nei, dist + 1))
                    if len(visited) >= max_nodes:
                        break

        return visited

    # ---------- MULTI-HOP PATH REASONING TRÊN ĐỒ THỊ ----------

    def _shortest_path(
        self, src: str, dst: str, max_depth: int = 3
    ) -> Optional[List[str]]:
        """
        Tìm một đường đi ngắn nhất (<= max_depth cạnh) giữa src và dst trên đồ thị vô hướng.
        Trả về danh sách tiêu đề node [src, ..., dst] hoặc None nếu không tìm thấy.
        """
        if src == dst:
            return [src]
        if src not in self.adj or dst not in self.adj:
            return None

        queue = deque([(src, 0)])
        prev: Dict[str, str] = {src: ""}  # node -> parent

        while queue:
            node, dist = queue.popleft()
            if dist >= max_depth:
                continue
            for nei in self.adj.get(node, []):
                if nei in prev:
                    continue
                prev[nei] = node
                if nei == dst:
                    # reconstruct path
                    path = [dst]
                    cur = dst
                    while prev[cur]:
                        cur = prev[cur]
                        path.append(cur)
                    path.reverse()
                    return path
                queue.append((nei, dist + 1))
        return None

    def extract_multi_hop_paths(
        self, seeds: List[str], max_depth: int = 3
    ) -> List[Dict]:
        """
        Tìm các đường đi ngắn (multi-hop) giữa các seed nodes.
        Trả về list các path:
          [{"source": s, "target": t, "path": [s, ..., t]}]
        """
        seeds = [s for s in seeds if s in self.adj]
        paths: List[Dict] = []
        seen_pairs: Set[Tuple[str, str]] = set()

        for i in range(len(seeds)):
            for j in range(i + 1, len(seeds)):
                s, t = seeds[i], seeds[j]
                key = (s, t)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                path = self._shortest_path(s, t, max_depth=max_depth)
                if path and len(path) > 1:
                    paths.append({"source": s, "target": t, "path": path})
        return paths

    # ---------- XÂY CONTEXT CHO CÂU HỎI ----------

    def build_context_for_question(
        self,
        question: str,
        seed_top_k: int = 5,
        depth: int = 2,
        max_nodes: int = 40,
        max_news: int = 5,
        include_paths: bool = True,
    ) -> Dict:
        """
        Trả về:
          - danh sách node được chọn,
          - context text (Wiki + một ít báo chí) để feed vào LLM (Llama-3.2-1B-Instruct).
        """
        seeds = self.search_seeds(question, top_k=seed_top_k)
        if not seeds:
            return {"seeds": [], "nodes": [], "context": "", "paths": []}

        subgraph_nodes = self.expand_subgraph(seeds, max_depth=depth, max_nodes=max_nodes)

        # Sắp xếp node trong subgraph theo importance: cộng đồng cùng seed + PageRank
        def node_score(title: str) -> Tuple[int, float]:
            n = self.nodes[title]
            # ưu tiên cùng cộng đồng với seed đầu tiên
            same_comm = int(
                any(self.nodes[s].community_id == n.community_id for s in seeds if s in self.nodes)
            )
            return (same_comm, n.pagerank)

        selected_nodes = sorted(subgraph_nodes, key=node_score, reverse=True)

        # (tuỳ chọn) trích xuất các đường đi multi-hop giữa các seed
        paths: List[Dict] = []
        if include_paths:
            paths = self.extract_multi_hop_paths(seeds, max_depth=3)

        # Ghép đoạn context từ Wiki + mô tả path
        parts: List[str] = []

        if paths:
            parts.append("[GRAPH PATHS]")
            for p in paths:
                node_chain = " -> ".join(p["path"])
                parts.append(f"- {p['source']} ~ {p['target']}: {node_chain}")
            parts.append("")  # dòng trống
        node_summaries: List[Dict] = []

        for title in selected_nodes:
            n = self.nodes[title]
            text = self.node_texts.get(title, {})
            intro = text.get("intro_text", "")
            if not intro:
                continue
            header = f"[NODE] {title} ({n.label})"
            meta = f"(PageRank={n.pagerank:.4f}, Community={n.community_id})"
            parts.append(f"{header} {meta}\n{intro}\n")
            node_summaries.append(
                {
                    "title": title,
                    "label": n.label,
                    "pagerank": n.pagerank,
                    "community_id": n.community_id,
                }
            )

        # Thêm một ít context từ báo chí: đơn giản là những bài có từ khóa trong tiêu đề
        news_added = 0
        q_norm = normalize_for_match(question)
        for doc in self.news_docs:
            if news_added >= max_news:
                break
            title = doc.get("title", "")
            if not title:
                continue
            if any(tok in normalize_for_match(title) for tok in q_norm.split(" ") if tok):
                text = doc.get("text", "")
                if not text:
                    continue
                parts.append(f"[NEWS] {title}\n{text}\n")
                news_added += 1

        context = "\n".join(parts)
        return {
            "seeds": seeds,
            "nodes": node_summaries,
            "context": context,
            "paths": paths,
        }


if __name__ == "__main__":
    # Demo đơn giản: xây index và build context cho 1 câu hỏi
    print("--- 🚀 Xây dựng GraphRAGIndex và demo build context ---")
    index = GraphRAGIndex.from_files()
    question = "Quan hệ giữa vua Minh Mạng và Gia Long là gì?"
    result = index.build_context_for_question(question)
    print("Seeds:", result["seeds"])
    print("Số node trong context:", len(result["nodes"]))
    print("Độ dài context (kí tự):", len(result["context"]))


