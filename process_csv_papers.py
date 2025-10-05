import pandas as pd
import sys
from pathlib import Path
import time
from typing import Dict, Optional
import re
import json

# Agregar la raíz al path
root_path = Path(__file__).parent
sys.path.insert(0, str(root_path))

from mysql_database import MySQLManager
from visual_extractor import VisualElementsExtractor
from loguru import logger


class CSVPaperProcessor:
    """Procesa papers desde un CSV y los inserta en la base de datos"""
    
    def __init__(self, db: MySQLManager):
        self.db = db
        self.session = self.db.get_session()
        self.stats = {
            'total': 0,
            'inserted': 0,
            'skipped': 0,
            'failed': 0
        }
    
    def extract_year_from_text(self, text: str) -> Optional[int]:
        """Extraer año de publicación del texto"""
        if not text:
            return None
        
        years = re.findall(r'\b(19|20)\d{2}\b', text)
        if years:
            year = int(years[0])
            if 1900 <= year <= 2025:
                return year
        return None
    
    def extract_sections(self, soup):
        """Extraer secciones específicas del paper"""
        sections = {
            'abstract': None,
            'results': None,
            'conclusions': None,
            'methods': None,
            'full_text': None
        }
        
        # Abstract
        abstract_selectors = [
            'div.abstract', 'section.abstract', 'div#abstract',
            'div[class*="abstract"]', 'p.abstract'
        ]
        for selector in abstract_selectors:
            elem = soup.select_one(selector)
            if elem:
                sections['abstract'] = elem.get_text(strip=True)
                break
        
        # Results
        results_headers = soup.find_all(['h2', 'h3', 'h4'], 
                                       string=re.compile(r'results?', re.IGNORECASE))
        if results_headers:
            content = []
            for sibling in results_headers[0].find_next_siblings():
                if sibling.name in ['h2', 'h3', 'h4']:
                    break
                content.append(sibling.get_text(strip=True))
            sections['results'] = '\n'.join(content) if content else None
        
        # Conclusions
        conclusion_headers = soup.find_all(['h2', 'h3', 'h4'], 
                                          string=re.compile(r'conclusion|discussion', re.IGNORECASE))
        if conclusion_headers:
            content = []
            for sibling in conclusion_headers[0].find_next_siblings():
                if sibling.name in ['h2', 'h3', 'h4']:
                    break
                content.append(sibling.get_text(strip=True))
            sections['conclusions'] = '\n'.join(content) if content else None
        
        # Methods
        methods_headers = soup.find_all(['h2', 'h3', 'h4'], 
                                       string=re.compile(r'method|material', re.IGNORECASE))
        if methods_headers:
            content = []
            for sibling in methods_headers[0].find_next_siblings():
                if sibling.name in ['h2', 'h3', 'h4']:
                    break
                content.append(sibling.get_text(strip=True))
            sections['methods'] = '\n'.join(content) if content else None
        
        # Full text
        sections['full_text'] = soup.get_text(separator='\n', strip=True)
        
        return sections
    
    def process_paper_from_url(self, paper_url: str, csv_title: str = None) -> Optional[int]:
       
        try:
            logger.info(f"Procesando: {paper_url}")
            
            # 1. Crear extractor y obtener contenido
            extractor = VisualElementsExtractor(
                paper_url, 
                output_dir=f"temp_extraction_{int(time.time())}"
            )
            
            soup = extractor.fetch_page_content()
            if not soup:
                logger.error("No se pudo obtener el contenido del paper")
                return None
            
            # 2. Extraer metadatos
            metadata = extractor.extract_metadata(soup)
            
            # 3. Extraer secciones
            sections = self.extract_sections(soup)
            
            # 4. Preparar datos del paper
            title = metadata.get('title') or csv_title or 'Sin título'
            year = self.extract_year_from_text(
                str(metadata.get('year', '')) + ' ' + title
            )
            
            # Verificar si ya existe por DOI o título similar
            if metadata.get('doi'):
                from sqlalchemy import text
                existing = self.session.execute(
                    text("SELECT id_paper FROM PAPER WHERE DOI = :doi"),
                    {'doi': metadata['doi']}
                ).fetchone()
                
                if existing:
                    logger.warning(f"Paper ya existe con ID: {existing[0]}")
                    return existing[0]
            
            # 5. Preparar datos para inserción
            paper_data = {
                'title': title[:500],  # Límite de VARCHAR
                'abstract': sections['abstract'][:5000] if sections['abstract'] else None,
                'year': year,
                'journal': metadata.get('journal', '')[:200] if metadata.get('journal') else None,
                'doi': metadata.get('doi', '')[:100] if metadata.get('doi') else None,
                'pdf_url': paper_url[:500],
                'full_text': sections['full_text'][:65000] if sections['full_text'] else None,
                'results_section': sections['results'][:5000] if sections['results'] else None,
                'conclusions_section': sections['conclusions'][:5000] if sections['conclusions'] else None,
                'methods_section': sections['methods'][:5000] if sections['methods'] else None
            }
            
            # 6. Insertar paper
            paper_id = self.db.insert_paper(paper_data)
            
            if not paper_id:
                logger.error("No se obtuvo ID del paper insertado")
                return None
            
            logger.success(f"Paper insertado con ID: {paper_id}")
            
            # 7. Insertar autores
            if metadata.get('authors'):
                self._process_authors(paper_id, metadata['authors'])
            
            # 8. Insertar keywords
            if metadata.get('keywords'):
                self._process_keywords(paper_id, metadata['keywords'])
            
            # 9. Extraer y guardar elementos visuales
            try:
                visual_results = extractor.run_extraction()
                if visual_results:
                    self._process_visual_elements(paper_id, visual_results)
            except Exception as e:
                logger.warning(f"Error extrayendo visuales (continuando): {e}")
            
            return paper_id
            
        except Exception as e:
            logger.error(f"Error procesando paper {paper_url}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _process_authors(self, paper_id: int, authors: list):
        """Procesar e insertar autores"""
        for position, author_name in enumerate(authors, 1):
            try:
                # Limpiar y validar nombre del autor
                author_name = author_name.strip()
                
                # Ignorar autores inválidos
                if (len(author_name) < 2 or 
                    len(author_name) > 100 or 
                    '@' in author_name or 
                    'http' in author_name.lower() or
                    author_name.startswith('✉') or
                    'competing interests' in author_name.lower() or
                    'conceived and designed' in author_name.lower()):
                    continue
                
                # Separar nombre y apellido
                parts = author_name.split()
                if len(parts) < 1:
                    continue
                    
                first_name = parts[0][:100]  # Limitar a 100 caracteres
                last_name = ' '.join(parts[1:])[:100] if len(parts) > 1 else ''
                
                # Validar que no sean demasiado largos
                if len(first_name) > 50 or len(last_name) > 50:
                    continue
                
                author_id = self.db.insert_author(first_name, last_name)
                self.db.link_paper_author(paper_id, author_id, position)
                
            except Exception as e:
                logger.warning(f"Error insertando autor {author_name[:50]}: {str(e)[:100]}")
    
    def _process_keywords(self, paper_id: int, keywords: list):
        """Procesar e insertar keywords"""
        for keyword_text in keywords:
            try:
                keyword_text = keyword_text.strip().lower()
                keyword_id = self.db.insert_keyword(keyword_text)
                self.db.link_paper_keyword(paper_id, keyword_id)
                
            except Exception as e:
                logger.warning(f"Error insertando keyword {keyword_text}: {e}")
    
    def _process_visual_elements(self, paper_id: int, visual_results: dict):
        """Procesar e insertar elementos visuales"""
        from sqlalchemy import text
        
        total = 0
        
        # Figuras
        for fig in visual_results.get('figures', []):
            try:
                self.session.execute(
                    text("""
                        INSERT INTO paper_resource 
                        (id_paper, resource_type, resource_number, caption, file_format)
                        VALUES (:id_paper, 'figure', :number, :caption, :format)
                    """),
                    {
                        'id_paper': paper_id,
                        'number': f"Fig. {fig.get('number')}",
                        'caption': fig.get('caption', '')[:1000] if fig.get('caption') else None,
                        'format': Path(fig.get('local_path', '')).suffix[1:] or 'unknown'
                    }
                )
                total += 1
            except Exception as e:
                logger.warning(f"Error insertando figura: {e}")
        
        # Tablas
        for table in visual_results.get('tables', []):
            try:
                self.session.execute(
                    text("""
                        INSERT INTO paper_resource 
                        (id_paper, resource_type, resource_number, caption, file_format)
                        VALUES (:id_paper, 'table', :number, :caption, 'html')
                    """),
                    {
                        'id_paper': paper_id,
                        'number': f"Table {table.get('number')}",
                        'caption': table.get('caption', '')[:1000] if table.get('caption') else None
                    }
                )
                total += 1
            except Exception as e:
                logger.warning(f"Error insertando tabla: {e}")
        
        # Imágenes
        for img in visual_results.get('images', []):
            try:
                self.session.execute(
                    text("""
                        INSERT INTO paper_resource 
                        (id_paper, resource_type, resource_number, caption, file_format)
                        VALUES (:id_paper, 'image', :number, :caption, :format)
                    """),
                    {
                        'id_paper': paper_id,
                        'number': f"Img {img.get('number')}",
                        'caption': img.get('alt_text', '')[:1000] if img.get('alt_text') else None,
                        'format': Path(img.get('local_path', '')).suffix[1:] or 'unknown'
                    }
                )
                total += 1
            except Exception as e:
                logger.warning(f"Error insertando imagen: {e}")
        
        self.session.commit()
        logger.info(f"Insertados {total} elementos visuales")
    
    def process_csv(self, csv_path: str, delay: int = 3):
        """
        Procesar todos los papers de un CSV
        
        Args:
            csv_path: Ruta al archivo CSV
            delay: Segundos de espera entre papers
        """
        logger.info(f"Leyendo CSV: {csv_path}")
        
        # Leer CSV
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"CSV cargado: {len(df)} filas")
        except Exception as e:
            logger.error(f"Error leyendo CSV: {e}")
            return
        
        # Normalizar nombres de columnas a minúsculas
        df.columns = df.columns.str.lower().str.strip()
        
        # Validar columnas
        if 'url' not in df.columns and 'link' not in df.columns:
            logger.error(f"CSV debe tener columna 'url' o 'link'. Columnas encontradas: {list(df.columns)}")
            return
        
        url_column = 'url' if 'url' in df.columns else 'link'
        title_column = 'title' if 'title' in df.columns else None
        
        logger.info(f"Columnas detectadas: {list(df.columns)}")
        logger.info(f"Usando columna de URL: '{url_column}'")
        logger.info(f"Usando columna de título: '{title_column}'")
        
        self.stats['total'] = len(df)
        
        # Procesar cada fila
        for idx, row in df.iterrows():
            logger.info(f"\n{'='*60}")
            logger.info(f"Procesando paper {idx + 1}/{len(df)}")
            logger.info(f"{'='*60}")
            
            paper_url = row[url_column]
            csv_title = row[title_column] if title_column else None
            
            if pd.isna(paper_url) or not paper_url:
                logger.warning("URL vacía, saltando...")
                self.stats['skipped'] += 1
                continue
            
            try:
                paper_id = self.process_paper_from_url(paper_url, csv_title)
                
                if paper_id:
                    self.stats['inserted'] += 1
                    logger.success(f"Paper insertado exitosamente: ID {paper_id}")
                else:
                    self.stats['failed'] += 1
                    logger.error("Falló la inserción del paper")
                
                # Delay entre papers
                if idx < len(df) - 1:
                    logger.info(f"Esperando {delay} segundos...")
                    time.sleep(delay)
                    
            except Exception as e:
                self.stats['failed'] += 1
                logger.error(f"Error procesando paper: {e}")
                continue
        
        # Resumen final
        self._print_summary()
    
    def _print_summary(self):
        """Imprimir resumen de procesamiento"""
        logger.info("\n" + "="*60)
        logger.info("RESUMEN DE PROCESAMIENTO")
        logger.info("="*60)
        logger.info(f"Total papers en CSV: {self.stats['total']}")
        logger.info(f"Insertados exitosamente: {self.stats['inserted']}")
        logger.info(f"Fallidos: {self.stats['failed']}")
        logger.info(f"Omitidos: {self.stats['skipped']}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['inserted'] / self.stats['total']) * 100
            logger.info(f"Tasa de éxito: {success_rate:.1f}%")
        
        logger.info("="*60)
    
    def close(self):
        """Cerrar sesión"""
        self.session.close()


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Procesar papers desde CSV e insertarlos en la base de datos'
    )
    parser.add_argument(
        'csv_file',
        help='Ruta al archivo CSV con los papers'
    )
    parser.add_argument(
        '--delay',
        type=int,
        default=3,
        help='Segundos de espera entre papers (default: 3)'
    )
    
    args = parser.parse_args()
    
    # Verificar que existe el CSV
    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        logger.error(f"Archivo CSV no encontrado: {csv_path}")
        sys.exit(1)
    
    try:
        # Conectar a la base de datos
        logger.info("Conectando a la base de datos...")
        db = MySQLManager()
        
        logger.success("Conexión exitosa")
        
        # Crear procesador
        processor = CSVPaperProcessor(db)
        
        # Procesar CSV
        processor.process_csv(str(csv_path), delay=args.delay)
        
        processor.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()