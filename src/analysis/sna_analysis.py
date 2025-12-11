from neo4j import GraphDatabase
import sys

# --- CẤU HÌNH ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "12345678"

GRAPH_NAME = "history-graph"

class Neo4jGDSAnalyzer:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        self.driver.verify_connectivity()
        print("✅ Đã kết nối Neo4j GDS!")

    def close(self):
        self.driver.close()

    def run_cypher(self, query, params=None):
        with self.driver.session() as session:
            result = session.run(query, params)
            return [record for record in result]

    def check_gds_installed(self):
        try:
            self.run_cypher("CALL gds.version()")
            print("✅ GDS Plugin đã được cài đặt.")
        except Exception:
            print("❌ Lỗi: Bạn CHƯA CÀI GDS Plugin cho Neo4j!")
            sys.exit(1)

    def project_graph(self):
        print("\n--- 1. TẠO GRAPH PROJECTION ---")
        self.run_cypher(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")
        
        # Projection vô hướng (UNDIRECTED) để tính toán khách quan nhất cho các chỉ số xã hội
        query = f"""
        CALL gds.graph.project(
            '{GRAPH_NAME}',
            '*',
            {{
                RELATIONSHIP: {{
                    type: '*',
                    orientation: 'UNDIRECTED'
                }}
            }}
        )
        """
        self.run_cypher(query)
        print(f"✅ Đã nạp đồ thị '{GRAPH_NAME}' vào bộ nhớ.")

    # --- CÁC THUẬT TOÁN ---

    def analyze_degree_centrality(self):
        print("\n--- 2. DEGREE CENTRALITY (MỐI QUAN HỆ TRỰC TIẾP) ---")
        # Tính và ghi vào thuộc tính 'degree_score'
        self.run_cypher(f"""
        CALL gds.degree.write('{GRAPH_NAME}', {{
            writeProperty: 'degree_score'
        }})
        """)
        
        # Xem Top 10
        print("🏆 TOP 10 NHÂN VẬT QUAN HỆ RỘNG NHẤT:")
        top_10 = self.run_cypher("""
        MATCH (n) 
        WHERE n.degree_score IS NOT NULL 
        RETURN n.title AS name, n.degree_score AS score 
        ORDER BY score DESC LIMIT 10
        """)
        for i, r in enumerate(top_10, 1):
            print(f"   #{i}. {r['name']} (Kết nối: {int(r['score'])})")

    def analyze_betweenness_centrality(self):
        print("\n--- 3. BETWEENNESS CENTRALITY (CẦU NỐI THÔNG TIN) ---")
        # Tính và ghi vào thuộc tính 'betweenness_score'
        # Lưu ý: Thuật toán này chạy khá lâu trên graph lớn
        self.run_cypher(f"""
        CALL gds.betweenness.write('{GRAPH_NAME}', {{
            writeProperty: 'betweenness_score'
        }})
        """)
        
        # Xem Top 10
        print("🏆 TOP 10 'CẦU NỐI' QUAN TRỌNG NHẤT:")
        top_10 = self.run_cypher("""
        MATCH (n) 
        WHERE n.betweenness_score IS NOT NULL 
        RETURN n.title AS name, n.betweenness_score AS score 
        ORDER BY score DESC LIMIT 10
        """)
        for i, r in enumerate(top_10, 1):
            print(f"   #{i}. {r['name']} (Score: {r['score']:.2f})")

    def analyze_pagerank(self):
        print("\n--- 4. PAGERANK (TẦM ẢNH HƯỞNG) ---")
        self.run_cypher(f"""
        CALL gds.pageRank.write('{GRAPH_NAME}', {{
            maxIterations: 20,
            dampingFactor: 0.85,
            writeProperty: 'pagerank'
        }})
        """)
        
        print("🏆 TOP 10 NHÂN VẬT ẢNH HƯỞNG (PAGERANK):")
        top_10 = self.run_cypher("""
        MATCH (n) 
        WHERE n.pagerank IS NOT NULL 
        RETURN n.title AS name, n.pagerank AS score 
        ORDER BY score DESC LIMIT 10
        """)
        for i, r in enumerate(top_10, 1):
            print(f"   #{i}. {r['name']} ({r['score']:.4f})")

    def analyze_communities(self):
        print("\n--- 5. PHÁT HIỆN CỘNG ĐỒNG (LOUVAIN) ---")
        res = self.run_cypher(f"""
        CALL gds.louvain.write('{GRAPH_NAME}', {{
            writeProperty: 'community_id'
        }})
        YIELD communityCount, modularity
        """)
        print(f"✅ Đã phát hiện {res[0]['communityCount']} cộng đồng.")
        
        print("🔍 Các nhóm tiêu biểu:")
        comm_query = """
        MATCH (n) 
        WHERE n.community_id IS NOT NULL
        WITH n.community_id AS commId, count(n) AS size, collect(n.title)[0..5] AS members
        ORDER BY size DESC LIMIT 5
        RETURN commId, size, members
        """
        for c in self.run_cypher(comm_query):
            print(f"   - Nhóm {c['commId']} ({c['size']} người): {', '.join(c['members'])}...")

    def analyze_small_world(self):
        print("\n--- 6. CHỨNG MINH THẾ GIỚI NHỎ ---")
        res = self.run_cypher(f"CALL gds.localClusteringCoefficient.stats('{GRAPH_NAME}') YIELD averageClusteringCoefficient")
        avg_cc = res[0]['averageClusteringCoefficient']
        print(f"📊 Hệ số phân cụm trung bình: {avg_cc:.4f}")
        
    def cleanup(self):
        print("\n--- 7. DỌN DẸP ---")
        self.run_cypher(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")
        print("✅ Đã giải phóng bộ nhớ GDS.")

    def run(self):
        self.check_gds_installed()
        self.project_graph()
        
        # Chạy lần lượt các thuật toán
        self.analyze_degree_centrality()
        self.analyze_betweenness_centrality()
        self.analyze_pagerank()
        self.analyze_communities()
        self.analyze_small_world()
        
        self.cleanup()
        self.close()

if __name__ == "__main__":
    analyzer = Neo4jGDSAnalyzer()
    analyzer.run()