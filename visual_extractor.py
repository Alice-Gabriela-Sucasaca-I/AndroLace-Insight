#!/usr/bin/env python3
"""
Visual Elements Extractor for Research Papers
Extracts images, figures, tables, and graphs from research papers
and prepares them for database insertion.
"""

import requests
import json
import os
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VisualElementsExtractor:
    def __init__(self, base_url: str, output_dir: str = "extracted_visuals"):
        """
        Initialize the extractor with base URL and output directory.
        
        Args:
            base_url: URL of the research paper
            output_dir: Directory to save extracted visual elements
        """
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.output_dir / "images").mkdir(exist_ok=True)
        (self.output_dir / "tables").mkdir(exist_ok=True)
        (self.output_dir / "figures").mkdir(exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        self.extracted_elements = {
            'images': [],
            'tables': [],
            'figures': [],
            'metadata': {
                'source_url': base_url,
                'extraction_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_elements': 0
            }
        }

    def fetch_page_content(self) -> Optional[BeautifulSoup]:
        """Fetch and parse the webpage content."""
        try:
            logger.info(f"Fetching content from: {self.base_url}")
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            logger.info("Successfully fetched and parsed page content")
            return soup
            
        except requests.RequestException as e:
            logger.error(f"Error fetching page content: {e}")
            return None

    def extract_figures_urls_only(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract figures with URLs only (no download)"""
        figures = []
        
        figure_selectors = ['div.figure', 'div.fig', 'figure', 'div[class*="figure"]']
        
        for selector in figure_selectors:
            figure_elements = soup.select(selector)
            
            for i, fig in enumerate(figure_elements):
                
                caption = ''
                caption_elem = fig.select_one('.caption, .fig-caption, [class*="caption"]')
                if caption_elem:
                    caption = caption_elem.get_text(strip=True)
                
                # Extract image URL (NO DESCARGAR)
                img = fig.find('img')
                if img and img.get('src'):
                    img_url = img.get('src')
                    
                    # Convertir a URL absoluta
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = urljoin(self.base_url, img_url)
                    
                    figures.append({
                        'type': 'figure',
                        'number': i + 1,
                        'caption': caption,
                        'image_url': img_url,  # Solo URL
                        'format': 'image'
                    })
        
        logger.info(f"Extracted {len(figures)} figure URLs")
        return figures

    def extract_tables_simple(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract tables with captions (no download)"""
        tables = []
        
        for i, table in enumerate(soup.find_all('table')):
            caption = ''
            
            # Buscar caption
            caption_elem = table.find('caption')
            if caption_elem:
                caption = caption_elem.get_text(strip=True)
            
            tables.append({
                'type': 'table',
                'number': i + 1,
                'caption': caption,
                'html_content': str(table)[:5000],  # Límite de 5000 chars
                'format': 'html'
            })
        
        logger.info(f"Extracted {len(tables)} tables")
        return tables
    def extract_tables(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract tables and their captions from the page."""
        tables = []
        
        # Find all table elements
        table_elements = soup.find_all('table')
        
        for i, table in enumerate(table_elements):
            table_data = {
                'type': 'table',
                'number': i + 1,
                'caption': '',
                'html_content': str(table),
                'local_path': '',
                'description': '',
                'context': ''
            }
            
            # Look for caption in various locations
            caption_found = False
            
            # Check if table has a caption element
            caption = table.find('caption')
            if caption:
                table_data['caption'] = caption.get_text(strip=True)
                caption_found = True
            
            # Look for caption in previous sibling
            if not caption_found:
                prev_sibling = table.find_previous_sibling()
                if prev_sibling and ('caption' in prev_sibling.get('class', []) or 
                                   'table-caption' in prev_sibling.get('class', [])):
                    table_data['caption'] = prev_sibling.get_text(strip=True)
                    caption_found = True
            
            # Look for caption in parent div
            if not caption_found:
                parent = table.parent
                if parent:
                    caption_elem = parent.find(['div', 'p'], class_=re.compile(r'caption|table-caption'))
                    if caption_elem:
                        table_data['caption'] = caption_elem.get_text(strip=True)
                        caption_found = True
            
            # Extract table data as structured format
            table_data['data'] = self.extract_table_data(table)
            
            # Save table as HTML file
            table_file = self.output_dir / "tables" / f"table_{i+1}.html"
            with open(table_file, 'w', encoding='utf-8') as f:
                f.write(f"<html><head><meta charset='utf-8'></head><body>{str(table)}</body></html>")
            table_data['local_path'] = str(table_file)
            
            # Extract context
            parent = table.parent
            if parent:
                context_text = parent.get_text(strip=True)
                table_data['context'] = context_text[:500] + "..." if len(context_text) > 500 else context_text
            
            tables.append(table_data)
        
        logger.info(f"Extracted {len(tables)} tables")
        return tables

    def extract_table_data(self, table) -> List[Dict[str, Any]]:
        """Extract structured data from HTML table."""
        data = []
        rows = table.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            row_data = []
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                row_data.append(cell_text)
            if row_data:
                data.append(row_data)
        
        return data

    def extract_images(self, soup: BeautifulSoup, exclude_figures: bool = True) -> List[Dict[str, Any]]:
        """Extract standalone images from the page, optionally excluding figure images."""
        images = []
        
        # Find all img elements
        img_elements = soup.find_all('img')
        
        for i, img in enumerate(img_elements):
            # Skip images that are already captured as figures
            if exclude_figures:
                # Check if this img is inside a figure element
                parent_figure = img.find_parent(['div', 'figure'], class_=lambda x: x and 'fig' in x.lower() if x else False)
                if parent_figure:
                    continue
                
                # Check if this img is inside a figure container
                figure_containers = img.find_parents(['div', 'figure'], class_=lambda x: x and any(
                    keyword in x.lower() for keyword in ['figure', 'fig', 'caption']
                ) if x else False)
                if figure_containers:
                    continue
            
            # Get image URL first to check if it should be excluded
            img_url = img.get('src')
            if not img_url:
                continue
                
            # Skip interface/UI images
            if self._should_skip_image(img, img_url):
                continue
            
            img_data = {
                'type': 'image',
                'number': len(images) + 1,  # Use sequential numbering for standalone images
                'alt_text': img.get('alt', ''),
                'image_url': '',
                'local_path': '',
                'description': '',
                'context': ''
            }
            
            # Process image URL
            if img_url.startswith('//'):
                img_url = 'https:' + img_url
            elif img_url.startswith('/'):
                img_url = urljoin(self.base_url, img_url)
            
            img_data['image_url'] = img_url
            
            # Download image
            local_path = self.download_image(img_url, f"image_{len(images)+1}")
            img_data['local_path'] = str(local_path) if local_path else ''
            
            # Extract context from parent elements
            parent = img.parent
            if parent:
                context_text = parent.get_text(strip=True)
                img_data['context'] = context_text[:500] + "..." if len(context_text) > 500 else context_text
            
            if img_data['image_url']:
                images.append(img_data)
        
        logger.info(f"Extracted {len(images)} standalone images")
        return images

    def _should_skip_image(self, img, img_url: str) -> bool:
        """Check if an image should be skipped based on various criteria."""
        
        # Skip data URLs (base64 encoded images)
        if img_url.startswith('data:'):
            return True
        
        # Skip common interface/UI images
        skip_patterns = [
            'static/img/',  # Static site images
            'icon-',        # Icons
            'logo',         # Logos
            'banner',       # Banners
            'header',       # Header images
            'footer',       # Footer images
            'nav-',         # Navigation images
            'button',       # Button images
            'arrow',        # Arrow icons
            'close',        # Close buttons
            'search',       # Search icons
            'menu',         # Menu icons
            'flag',         # Flag images
            'dot-gov',      # Government icons
            'https',        # HTTPS icons
            'usa-icons',    # USA government icons
            'ncbi-logos',   # NCBI logos
        ]
        
        # Check URL patterns
        for pattern in skip_patterns:
            if pattern in img_url.lower():
                return True
        
        # Check alt text for interface indicators
        alt_text = img.get('alt', '').lower()
        skip_alt_patterns = [
            'logo', 'icon', 'button', 'arrow', 'close', 'search', 
            'menu', 'flag', 'banner', 'header', 'footer', 'nav'
        ]
        
        for pattern in skip_alt_patterns:
            if pattern in alt_text:
                return True
        
        # Check if image is very small (likely an icon)
        width = img.get('width')
        height = img.get('height')
        if width and height:
            try:
                w, h = int(width), int(height)
                if w < 50 or h < 50:  # Very small images are likely icons
                    return True
            except ValueError:
                pass
        
        # Check parent elements for interface indicators
        parent = img.parent
        if parent:
            parent_class = parent.get('class', [])
            parent_id = parent.get('id', '')
            
            interface_indicators = [
                'header', 'footer', 'nav', 'menu', 'sidebar', 'toolbar',
                'banner', 'logo', 'icon', 'button', 'control'
            ]
            
            for indicator in interface_indicators:
                if (indicator in ' '.join(parent_class).lower() or 
                    indicator in parent_id.lower()):
                    return True
        
        return False

    def download_image(self, url: str, filename: str) -> Optional[Path]:
        """Download an image from URL and save it locally."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Determine file extension
            parsed_url = urlparse(url)
            ext = os.path.splitext(parsed_url.path)[1]
            if not ext:
                ext = '.jpg'  # Default extension
            
            filename = f"{filename}{ext}"
            file_path = self.output_dir / "images" / filename
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded image: {filename}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading image {url}: {e}")
            return None
    
    def extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract paper metadata."""
        metadata = {
            'title': '',
            'authors': [],
            'abstract': '',
            'doi': '',
            'journal': '',
            'year': '',
            'keywords': []
        }
        
        # Extract title
        title_selectors = [
            'h1.content-title',
            'h1',
            'title',
            '.article-title',
            '.title'
        ]
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                metadata['title'] = title_elem.get_text(strip=True)
                if metadata['title'] and len(metadata['title']) > 10:
                    break
        
        # Extract authors - MEJORADO
        # Buscar en elementos específicos de autores
        author_containers = soup.select('.contrib-group .contrib')
        if author_containers:
            for author_elem in author_containers:
                # Buscar nombre dentro del elemento
                name_elem = author_elem.select_one('.name, .contrib-name')
                if name_elem:
                    author_text = name_elem.get_text(strip=True)
                    # Limpiar y validar
                    if (author_text and 
                        len(author_text) > 2 and 
                        len(author_text) < 100 and
                        '@' not in author_text and
                        'http' not in author_text.lower()):
                        metadata['authors'].append(author_text)
        
        # Si no encontró autores, buscar de forma alternativa
        if not metadata['authors']:
            author_selectors = [
                '.author-list .author',
                '.authors-list .author',
                'meta[name="citation_author"]'
            ]
            for selector in author_selectors:
                if selector.startswith('meta'):
                    author_elems = soup.select(selector)
                    for elem in author_elems[:20]:  # Máximo 20 autores
                        content = elem.get('content', '').strip()
                        if content and len(content) < 100:
                            metadata['authors'].append(content)
                else:
                    author_elems = soup.select(selector)
                    for elem in author_elems[:20]:
                        author_text = elem.get_text(strip=True)
                        if author_text and len(author_text) < 100:
                            metadata['authors'].append(author_text)
        
        # Extract abstract
        abstract_selectors = ['.abstract', '.summary', '[class*="abstract"]']
        for selector in abstract_selectors:
            abstract_elem = soup.select_one(selector)
            if abstract_elem:
                metadata['abstract'] = abstract_elem.get_text(strip=True)
                break
        
        # Extract DOI
        doi_pattern = r'10\.\d+/[^\s]+'
        text_content = soup.get_text()
        doi_match = re.search(doi_pattern, text_content)
        if doi_match:
            metadata['doi'] = doi_match.group()
        
        # Extract journal
        journal_elem = soup.select_one('.journal-title, meta[name="citation_journal_title"]')
        if journal_elem:
            if journal_elem.name == 'meta':
                metadata['journal'] = journal_elem.get('content', '')
            else:
                metadata['journal'] = journal_elem.get_text(strip=True)
                # Extract year - AGREGAR ESTO
        year_elem = soup.select_one('meta[name="citation_publication_date"]')
        if year_elem:
            date_content = year_elem.get('content', '')
            year_match = re.search(r'\b(19|20)\d{2}\b', date_content)
            if year_match:
                metadata['year'] = year_match.group()

        # Si no encontró año en meta, buscar en texto
        if not metadata['year']:
            pub_info = soup.select_one('.publication-date, .pub-date, .article-date')
            if pub_info:
                year_match = re.search(r'\b(19|20)\d{2}\b', pub_info.get_text())
                if year_match:
                    metadata['year'] = year_match.group()
        return metadata
    def save_json_metadata(self):
        """Save extracted elements metadata to JSON file."""
        json_file = self.output_dir / "visual_elements_metadata.json"
        
        # Update total count
        total_elements = (len(self.extracted_elements['images']) + 
                         len(self.extracted_elements['tables']) + 
                         len(self.extracted_elements['figures']))
        self.extracted_elements['metadata']['total_elements'] = total_elements
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.extracted_elements, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved metadata to: {json_file}")


    def run_extraction(self):
        """Run the complete extraction process."""
        logger.info("Starting visual elements extraction...")
        
        # Fetch page content ONCE
        soup = self.fetch_page_content()
        if not soup:
            logger.error("Failed to fetch page content")
            return
        
        # Extract different types of visual elements
        self.extracted_elements['figures'] = self.extract_figures(soup)
        self.extracted_elements['tables'] = self.extract_tables(soup)
        self.extracted_elements['images'] = self.extract_images(soup)
        
        # Save metadata
        self.save_json_metadata()
        
        logger.info(f"Extraction completed. Results saved in: {self.output_dir}")
        logger.info(f"Total elements extracted: {self.extracted_elements['metadata']['total_elements']}")
        
        return self.extracted_elements

def main():
    """Main function to run the extraction."""
    # URL of the research paper
    paper_url = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4136787/"
    
    # Create extractor instance
    extractor = VisualElementsExtractor(paper_url, "extracted_visuals")
    
    # Run extraction
    results = extractor.run_extraction()
    
    if results:
        print("\n" + "="*50)
        print("EXTRACTION SUMMARY")
        print("="*50)
        print(f"Total figures: {len(results['figures'])}")
        print(f"Total tables: {len(results['tables'])}")
        print(f"Total images: {len(results['images'])}")
        print(f"Total elements: {results['metadata']['total_elements']}")
        print(f"\nResults saved in: {extractor.output_dir}")
        print("="*50)

if __name__ == "__main__":
    main()
