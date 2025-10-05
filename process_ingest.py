import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import nltk
from nltk.tokenize import sent_tokenize
import json
from tqdm import tqdm
from mysql_database import MySQLManager
from loguru import logger

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)

class CompletePipeline:
    def __init__(self):
        logger.info("Inicializando pipeline IA...")
        
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        self.db = MySQLManager()
        
        logger.success("Pipeline IA listo")
    
    def summarize_text(self, text, max_sentences=3):
        """Resumen extractivo simple"""
        if not text or len(text) < 100:
            return text
        
        sentences = sent_tokenize(text)
        if len(sentences) <= max_sentences:
            return text
        
        return ' '.join(sentences[:max_sentences])
    
    def generate_student_explanation(self, abstract):
        """Generar explicación simple para estudiantes"""
        if not abstract:
            return "No hay información disponible."
        
        summary = self.summarize_text(abstract, 2)
        
        explanation = (
            f"En términos simples: {summary} "
            f"Este estudio es importante para entender cómo los organismos "
            f"responden al ambiente espacial."
        )
        
        return explanation
    
    def extract_keywords(self, text):
        """Extraer keywords básicas"""
        if not text:
            return []
        
        space_biology_terms = [
            'microgravity', 'radiation', 'spaceflight', 'ISS',
            'cell', 'bone', 'muscle', 'gene', 'protein',
            'stem cell', 'immune', 'cardiovascular', 'plant'
        ]
        
        keywords = []
        text_lower = text.lower()
        
        for term in space_biology_terms:
            if term in text_lower:
                keywords.append(term)
        
        return keywords[:10]
    
    def generate_embeddings(self, text):
        """Generar embeddings vectoriales"""
        embedding = self.embedding_model.encode(text)
        return embedding.tolist()
    def process_single_paper(self, paper_data):
        """Procesar un paper completo"""
        paper_id = paper_data['id_paper']
        
        text = f"{paper_data.get('title', '')} {paper_data.get('abstract', '')}"
        
        results = {
            'id_paper': paper_id,
            'summary_abstract': None,
            'summary_results': None,     
            'summary_conclusions': None, 
            'hypothesis': None,
            'key_findings': [],
            'student_explanation': None,
            'keywords': [],
            'embedding': None
        }
        
        if paper_data.get('abstract'):
            results['summary_abstract'] = self.summarize_text(paper_data['abstract'])
            results['student_explanation'] = self.generate_student_explanation(paper_data['abstract'])
        
        if paper_data.get('results_section'):
            results['summary_results'] = self.summarize_text(paper_data['results_section'], max_sentences=4)
    
        if paper_data.get('conclusions_section'):
            results['summary_conclusions'] = self.summarize_text(paper_data['conclusions_section'], max_sentences=3)
        
        results['hypothesis'] = f"Estudio sobre {paper_data.get('title', 'biología espacial')}"
        
        results['keywords'] = self.extract_keywords(text)
        
        if text.strip():
            results['embedding'] = self.generate_embeddings(text)
        
        return results
    def process_all_papers(self, limit=None):
        """Procesar todos los papers"""
        papers = self.db.get_all_papers(limit=limit or 1000)
        
        logger.info(f"Procesando {len(papers)} papers con IA...")
        
        all_embeddings = []
        paper_ids = []
        
        for paper in tqdm(papers, desc="Procesando papers"):
            try:
                results = self.process_single_paper(paper)
                self.save_results_to_db(results)
        
                if results['embedding']:
                    all_embeddings.append(results['embedding'])
                    paper_ids.append(paper['id_paper'])
                
            except Exception as e:
                logger.warning(f"Error procesando paper {paper['id_paper']}: {e}")
                continue
        
        self.save_embeddings(all_embeddings, paper_ids)
        
        logger.success("Procesamiento IA completado")
    
    def save_results_to_db(self, results):
   
        self.db.insert_ai_summary(results)

        for keyword in results['keywords']:
            kw_id = self.db.insert_keyword(keyword)
            self.db.link_paper_keyword(results['id_paper'], kw_id)
    
    def save_embeddings(self, embeddings, paper_ids):

        import faiss
        
        if not embeddings:
            logger.warning("No hay embeddings para guardar")
            return
        
        embeddings_array = np.array(embeddings).astype('float32')
        dimension = embeddings_array.shape[1]
  
        index = faiss.IndexFlatL2(dimension)
        index.add(embeddings_array)
        
        faiss.write_index(index, "outputs/faiss_index.bin")
        
        with open("outputs/paper_id_mapping.json", 'w') as f:
            json.dump(paper_ids, f)
        
        logger.success(f"Índice FAISS guardado: {len(paper_ids)} papers")


if __name__ == "__main__":
    pipeline = CompletePipeline()
    pipeline.process_all_papers(limit=20)