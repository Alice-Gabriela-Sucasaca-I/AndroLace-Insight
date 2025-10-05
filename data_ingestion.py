"""
Data Ingestion - Carga de Papers NASA
"""

import pandas as pd
import requests
from mysql_database import MySQLManager
from loguru import logger

class DataIngestion:
    def __init__(self):
        self.db = MySQLManager()
        self.base_url = "https://raw.githubusercontent.com/jgalazka/SB_publications/main/"
        
    def load_csv_papers(self, csv_path):
        """Cargar papers desde CSV"""
        logger.info("Cargando papers desde CSV...")
        
        try:
            # Intentar desde GitHub primero
            df = pd.read_csv(self.base_url + "SB_publications.csv")
            logger.info(f"Cargado desde GitHub: {len(df)} papers")
        except:
            # Si falla, cargar local
            df = pd.read_csv(csv_path)
            logger.info(f"Cargado desde archivo local: {len(df)} papers")
        
        return df
    
    def extract_metadata(self, row):
        """Extraer metadata del CSV"""
        import re
        
        # Extraer año
        year = None
        date_str = str(row.get('Publication Date', ''))
        if date_str and date_str != 'nan':
            match = re.search(r'\b(19|20)\d{2}\b', date_str)
            if match:
                year = int(match.group())
        
        return {
            'title': str(row.get('Title', '')),
            'abstract': str(row.get('Abstract', '')) if pd.notna(row.get('Abstract')) else None,
            'year': year,
            'journal': str(row.get('Journal', '')) if pd.notna(row.get('Journal')) else None,
            'doi': str(row.get('DOI', '')) if pd.notna(row.get('DOI')) else None,
            'pdf_url': str(row.get('URL', '')) if pd.notna(row.get('URL')) else None,
            'authors': str(row.get('Authors', '')).split(';') if pd.notna(row.get('Authors')) else []
        }
    
    def ingest_papers(self, df, process_full_text=False):
        """Ingestar papers a Aiven"""
        logger.info(f"Iniciando ingesta de {len(df)} papers a Aiven...")
        
        for idx, row in df.iterrows():
            try:
                metadata = self.extract_metadata(row)
                
                # Insertar paper
                paper_id = self.db.insert_paper(metadata)
                
                # Insertar autores
                for author_name in metadata['authors']:
                    if author_name and author_name.strip():
                        parts = author_name.strip().split()
                        first_name = parts[0] if len(parts) > 0 else ''
                        last_name = ' '.join(parts[1:]) if len(parts) > 1 else ''
                        
                        author_id = self.db.insert_author(first_name, last_name)
                        self.db.link_paper_author(paper_id, author_id)
                
                if (idx + 1) % 50 == 0:
                    logger.info(f"Procesados {idx + 1}/{len(df)} papers")
                    
            except Exception as e:
                logger.warning(f"Error procesando paper {idx}: {e}")
                continue
        
        logger.success(f"Ingesta completa: {len(df)} papers")


if __name__ == "__main__":
    ingestion = DataIngestion()
    df = ingestion.load_csv_papers("SB_publications.csv")
    ingestion.ingest_papers(df)