import networkx as nx
from pyvis.network import Network
from mysql_database import MySQLManager
from loguru import logger

class KnowledgeGraphGenerator:
    def __init__(self):
        self.db = MySQLManager()
        self.graph = nx.Graph()
        
    def build_keyword_relations(self, min_shared=2):
        """Construir relaciones por keywords compartidas"""
        logger.info("Construyendo relaciones por keywords...")
        
        session = self.db.get_session()
        from sqlalchemy import text
        
        query = text("""
            SELECT p.id_paper, GROUP_CONCAT(k.word) as keywords
            FROM PAPER p
            JOIN PAPER_KEYWORD pk ON p.id_paper = pk.id_paper
            JOIN KEYWORD k ON pk.id_keyword = k.id_keyword
            GROUP BY p.id_paper
        """)
        
        results = session.execute(query).fetchall()
        session.close()
        
        paper_keywords = {}
        for row in results:
            if row.keywords:
                paper_keywords[row.id_paper] = set(row.keywords.split(','))
        
        relations = 0
        paper_ids = list(paper_keywords.keys())
        
        for i in range(len(paper_ids)):
            for j in range(i+1, min(i+50, len(paper_ids))):  # Limitar para eficiencia
                p1, p2 = paper_ids[i], paper_ids[j]
                
                shared = paper_keywords[p1].intersection(paper_keywords[p2])
                
                if len(shared) >= min_shared:
                    strength = len(shared) / (len(paper_keywords[p1]) + len(paper_keywords[p2]))
                    
                    self.graph.add_edge(p1, p2, weight=strength)
                    relations += 1
        
        logger.success(f"{relations} relaciones creadas")
        return relations
    
    def generate_all_relations(self):
        """Generar todas las relaciones"""
        logger.info("Generando grafo de conocimiento...")
        
        relations = self.build_keyword_relations()
        
        # Visualizar
        self.visualize_graph()
        
        return {
            'nodes': self.graph.number_of_nodes(),
            'edges': self.graph.number_of_edges()
        }
    
    def visualize_graph(self, max_nodes=50):
        """Generar visualización HTML"""
        logger.info("Generando visualización...")
        
        net = Network(height="750px", width="100%", bgcolor="#222", font_color="white")
        
        # Subgrafo si es muy grande
        if self.graph.number_of_nodes() > max_nodes:
            degrees = dict(self.graph.degree())
            top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:max_nodes]
            subgraph = self.graph.subgraph([n[0] for n in top_nodes])
        else:
            subgraph = self.graph
        
        # Agregar nodos
        for node in subgraph.nodes():
            net.add_node(node, label=f"Paper {node}", size=20)
        
        # Agregar edges
        for edge in subgraph.edges(data=True):
            net.add_edge(edge[0], edge[1], value=edge[2].get('weight', 1) * 10)
        
        net.save_graph("outputs/knowledge_graph.html")
        logger.success("Grafo guardado en outputs/knowledge_graph.html")
    
    def get_paper_neighbors(self, paper_id, limit=10):
        """Obtener papers relacionados"""
        if paper_id not in self.graph:
            return []
        
        neighbors = list(self.graph.neighbors(paper_id))[:limit]
        
        return [{
            'id': n,
            'strength': self.graph[paper_id][n].get('weight', 0)
        } for n in neighbors]


if __name__ == "__main__":
    kg = KnowledgeGraphGenerator()
    stats = kg.generate_all_relations()
    print(f"Nodos: {stats['nodes']}, Relaciones: {stats['edges']}")