import argparse
from loguru import logger
import sys
import json
import re
from pathlib import Path
from sqlalchemy import text
from datetime import datetime
import pandas as pd

logger.remove()
logger.add(
    sys.stdout, 
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)
logger.add("logs/app_{time}.log", rotation="1 day", retention="7 days")

Path("logs").mkdir(exist_ok=True)
Path("outputs").mkdir(exist_ok=True)

from mysql_database import MySQLManager
from process_ingest import CompletePipeline
from knowledge_graph import KnowledgeGraphGenerator
from nasa_api_integration import NASAAPIIntegration

class MainOrchestrator:
    def __init__(self):
        logger.info("=" * 80)
        logger.info("NASA SPACE BIOLOGY KNOWLEDGE ENGINE")
        logger.info("=" * 80)
        
        try:
            self.db = MySQLManager()
            self.pipeline = None
            self.kg = None
            self.nasa_api = NASAAPIIntegration()
            logger.success("Inicialización exitosa")
        except Exception as e:
            logger.error(f"Error inicializando: {e}")
            sys.exit(1)
    
    def step1_ingest_and_extract(self, csv_path: str, limit: int = None):
        """PASO 1: Ingestar papers con visual_extractor"""
        logger.info("=" * 80)
        logger.info("PASO 1: INGESTA Y EXTRACCIÓN COMPLETA")
        logger.info("=" * 80)
        
        try:
            from visual_extractor import VisualElementsExtractor
            
            # Cargar CSV
            try:
                df = pd.read_csv("https://raw.githubusercontent.com/jgalazka/SB_publications/main/SB_publications.csv")
                logger.info(f"CSV cargado desde GitHub: {len(df)} papers")
            except:
                df = pd.read_csv(csv_path)
                logger.info(f"CSV cargado localmente: {len(df)} papers")
            
            if limit:
                df = df.head(limit)
                logger.info(f"Limitado a {limit} papers")
            
            ingested = 0
            skipped = 0
            
            for idx, row in df.iterrows():
                try:
                    title = str(row.get('Title', '')).strip()
                    url = str(row.get('Link', '')).strip()
                    
                    if not title or title == 'nan' or not url or url == 'nan':
                        skipped += 1
                        continue
                    
                    # Verificar duplicados
                    session = self.db.get_session()
                    existing = session.execute(
                        text("SELECT id_paper FROM PAPER WHERE title = :title LIMIT 1"),
                        {'title': title}
                    ).fetchone()
                    session.close()
                    
                    if existing:
                        logger.debug(f"Ya existe: {title[:50]}...")
                        continue
                    
                    logger.info(f"[{idx+1}/{len(df)}] Extrayendo: {title[:60]}...")
                    
                    # Extraer con visual_extractor
                    extractor = VisualElementsExtractor(url, f"temp_paper_{idx}")
                    soup = extractor.fetch_page_content()
                    
                    if not soup:
                        logger.warning(f"No se pudo obtener contenido")
                        skipped += 1
                        continue
                    
                    metadata = extractor.extract_metadata(soup)
                    
                    # Preparar datos
                    paper_data = {
                        'title': metadata.get('title') or title,
                        'abstract': metadata.get('abstract'),
                        'year': int(metadata.get('year')) if metadata.get('year') else None,
                        'journal': metadata.get('journal'),
                        'doi': metadata.get('doi'),
                        'pdf_url': url,
                        'full_text': None,
                        'methods_section': None,
                        'results_section': None,
                        'conclusions_section': None
                    }
                    
                    # Insertar paper
                    paper_id = self.db.insert_paper(paper_data)
                    
                    # Guardar recursos visuales
                    self._save_visual_resources(paper_id, soup, url)
                    
                    # Autores
                    for pos, author_name in enumerate(metadata.get('authors', [])[:10]):
                        if author_name and len(author_name) > 5:
                            parts = author_name.split()
                            first_name = parts[0] if parts else ''
                            last_name = ' '.join(parts[1:]) if len(parts) > 1 else ''
                            
                            author_id = self.db.insert_author(first_name, last_name)
                            self.db.link_paper_author(paper_id, author_id, pos)
                    
                    ingested += 1
                    
                    if (idx + 1) % 10 == 0:
                        logger.info(f"Progreso: {idx + 1}/{len(df)}")
                    
                    import time
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error en paper {idx}: {e}")
                    skipped += 1
                    continue
            
            logger.success(f"Completado: {ingested} papers, {skipped} omitidos")
            return ingested
            
        except Exception as e:
            logger.error(f"Error: {e}")
            raise
    
    def _save_visual_resources(self, paper_id: int, soup, base_url: str):
        """Guardar recursos visuales en BD"""
        try:
            session = self.db.get_session()
            
            from urllib.parse import urljoin
            figures = soup.select('div.figure, figure')
            for i, fig in enumerate(figures[:10], 1):
                img = fig.find('img')
                if img and img.get('src'):
                    img_url = img['src']
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = urljoin(base_url, img_url)
                    
                    caption_elem = fig.select_one('.caption, .fig-caption')
                    caption = caption_elem.get_text(strip=True) if caption_elem else ''
                    
                    session.execute(text("""
                        INSERT INTO PAPER_RESOURCE 
                        (id_paper, resource_type, resource_number, caption, file_url, file_format)
                        VALUES (:paper_id, 'figure', :number, :caption, :url, 'image')
                    """), {
                        'paper_id': paper_id,
                        'number': f"Fig. {i}",
                        'caption': caption[:500] if caption else None,
                        'url': img_url
                    })
            
            session.commit()
            session.close()
            
        except Exception as e:
            logger.warning(f"Error guardando recursos: {e}")
    
    # ... resto de métodos (step2, step3, etc.) igual que en tu documento 30 ...


def main():
    parser = argparse.ArgumentParser(description='NASA Space Biology KB')
    parser.add_argument('--action', choices=['ingest', 'citations', 'process', 'themes', 'effects', 'graph', 'comparisons', 'full', 'status'], required=True)
    parser.add_argument('--csv', type=str, default='SB_publications.csv')
    parser.add_argument('--limit', type=int)
    
    args = parser.parse_args()
    orchestrator = MainOrchestrator()
    
    if args.action == 'ingest':
        orchestrator.step1_ingest_and_extract(args.csv, args.limit)
    elif args.action == 'full':
        result = orchestrator.run_full_pipeline(args.csv, args.limit)
        if not result['success']:
            sys.exit(1)
    elif args.action == 'status':
        status = orchestrator.get_system_status()
        print("\n" + "=" * 60)
        for key, value in status.items():
            print(f"{key:25s}: {value}")

if __name__ == "__main__":
    main()