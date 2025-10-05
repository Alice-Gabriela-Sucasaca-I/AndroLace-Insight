"""
Extractor completo de contenido de papers científicos
Extrae: título, abstract, año, journal, DOI, métodos, resultados, conclusiones
"""

import requests
from bs4 import BeautifulSoup
import re
from loguru import logger
from typing import Dict, Optional
import time

class PaperContentExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract_from_url(self, url: str, title: str = None) -> Dict:
        """
        Extrae contenido completo de un paper desde su URL
        Soporta: PubMed Central, PubMed, bioRxiv, etc.
        """
        try:
            if 'pmc/articles' in url.lower():
                return self._extract_from_pmc(url, title)
            elif 'pubmed' in url.lower():
                return self._extract_from_pubmed(url, title)
            elif 'biorxiv' in url.lower() or 'medrxiv' in url.lower():
                return self._extract_from_biorxiv(url, title)
            else:
                return self._extract_generic(url, title)
        
        except Exception as e:
            logger.warning(f"Error extrayendo de {url}: {e}")
            return self._create_minimal_paper(title, url)
    
    def _extract_from_pmc(self, url: str, title: str) -> Dict:
        """Extracción de PubMed Central (PMC)"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer titulo
            title_tag = soup.find('h1', class_='content-title')
            if not title_tag and title:
                title_tag = title
            else:
                title_tag = title_tag.get_text(strip=True) if title_tag else title
            
            # Extraer abstract
            abstract = None
            abstract_section = soup.find('div', class_='abstract') or soup.find('abstract')
            if abstract_section:
                abstract = self._clean_text(abstract_section.get_text())
            
            # Extraer año
            year = None
            year_tag = soup.find('span', class_='cit')
            if year_tag:
                year_match = re.search(r'\b(19|20)\d{2}\b', year_tag.get_text())
                if year_match:
                    year = int(year_match.group())
            
            # Extraer journal
            journal = None
            journal_tag = soup.find('span', class_='journal-title') or soup.find('a', class_='journal-link')
            if journal_tag:
                journal = self._clean_text(journal_tag.get_text())
            
            # Extraer DOI
            doi = None
            doi_tag = soup.find('span', class_='doi')
            if doi_tag:
                doi = self._clean_text(doi_tag.get_text()).replace('doi:', '').strip()
            
            # Extraer PMC ID desde URL
            pmc_match = re.search(r'PMC\d+', url)
            if pmc_match and not doi:
                doi = f"PMC:{pmc_match.group()}"
            
            # Extraer secciones
            methods = self._extract_section(soup, ['methods', 'materials and methods', 'methodology'])
            results = self._extract_section(soup, ['results', 'findings'])
            conclusions = self._extract_section(soup, ['conclusion', 'conclusions', 'discussion'])
            
            # Texto completo
            full_text = None
            body = soup.find('div', class_='body') or soup.find('div', class_='article')
            if body:
                full_text = self._clean_text(body.get_text())
            
            return {
                'title': title_tag,
                'abstract': abstract,
                'year': year,
                'journal': journal,
                'doi': doi,
                'pdf_url': url,
                'full_text': full_text,
                'methods_section': methods,
                'results_section': results,
                'conclusions_section': conclusions
            }
        
        except Exception as e:
            logger.error(f"Error en PMC {url}: {e}")
            return self._create_minimal_paper(title, url)
    
    def _extract_from_pubmed(self, url: str, title: str) -> Dict:
        """Extracción de PubMed"""
        try:
            # Extraer PMID de la URL
            pmid_match = re.search(r'/(\d+)/?$', url)
            if not pmid_match:
                return self._create_minimal_paper(title, url)
            
            pmid = pmid_match.group(1)
            
            # Usar E-utilities API
            efetch_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
            params = {
                'db': 'pubmed',
                'id': pmid,
                'retmode': 'xml'
            }
            
            response = requests.get(efetch_url, params=params, timeout=20)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            
            # Extraer datos
            article = soup.find('PubmedArticle')
            if not article:
                return self._create_minimal_paper(title, url)
            
            title_tag = article.find('ArticleTitle')
            title_text = title_tag.get_text(strip=True) if title_tag else title
            
            abstract_tag = article.find('AbstractText')
            abstract = self._clean_text(abstract_tag.get_text()) if abstract_tag else None
            
            year_tag = article.find('PubDate').find('Year') if article.find('PubDate') else None
            year = int(year_tag.get_text()) if year_tag else None
            
            journal_tag = article.find('Journal').find('Title') if article.find('Journal') else None
            journal = self._clean_text(journal_tag.get_text()) if journal_tag else None
            
            # DOI
            doi = None
            for article_id in article.find_all('ArticleId'):
                if article_id.get('IdType') == 'doi':
                    doi = article_id.get_text(strip=True)
                    break
            
            return {
                'title': title_text,
                'abstract': abstract,
                'year': year,
                'journal': journal,
                'doi': doi or f"PMID:{pmid}",
                'pdf_url': url,
                'full_text': None,
                'methods_section': None,
                'results_section': None,
                'conclusions_section': None
            }
        
        except Exception as e:
            logger.error(f"Error en PubMed {url}: {e}")
            return self._create_minimal_paper(title, url)
    
    def _extract_from_biorxiv(self, url: str, title: str) -> Dict:
        """Extracción de bioRxiv/medRxiv"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            title_tag = soup.find('h1', id='page-title')
            title_text = title_tag.get_text(strip=True) if title_tag else title
            
            abstract = None
            abstract_section = soup.find('div', class_='section abstract')
            if abstract_section:
                abstract = self._clean_text(abstract_section.get_text())
            
            # DOI desde meta tags
            doi = None
            doi_meta = soup.find('meta', attrs={'name': 'citation_doi'})
            if doi_meta:
                doi = doi_meta.get('content')
            
            # Año
            year = None
            date_meta = soup.find('meta', attrs={'name': 'citation_publication_date'})
            if date_meta:
                date_str = date_meta.get('content')
                year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
                if year_match:
                    year = int(year_match.group())
            
            return {
                'title': title_text,
                'abstract': abstract,
                'year': year,
                'journal': 'bioRxiv' if 'biorxiv' in url else 'medRxiv',
                'doi': doi,
                'pdf_url': url,
                'full_text': None,
                'methods_section': None,
                'results_section': None,
                'conclusions_section': None
            }
        
        except Exception as e:
            logger.error(f"Error en bioRxiv {url}: {e}")
            return self._create_minimal_paper(title, url)
    
    def _extract_generic(self, url: str, title: str) -> Dict:
        """Extracción genérica para otros sitios"""
        try:
            response = self.session.get(url, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Intentar extraer de meta tags
            title_meta = soup.find('meta', attrs={'name': 'citation_title'})
            title_text = title_meta.get('content') if title_meta else title
            
            abstract_meta = soup.find('meta', attrs={'name': 'citation_abstract'})
            abstract = abstract_meta.get('content') if abstract_meta else None
            
            doi_meta = soup.find('meta', attrs={'name': 'citation_doi'})
            doi = doi_meta.get('content') if doi_meta else None
            
            year = None
            date_meta = soup.find('meta', attrs={'name': 'citation_publication_date'})
            if date_meta:
                year_match = re.search(r'\b(19|20)\d{2}\b', date_meta.get('content'))
                if year_match:
                    year = int(year_match.group())
            
            journal_meta = soup.find('meta', attrs={'name': 'citation_journal_title'})
            journal = journal_meta.get('content') if journal_meta else None
            
            return {
                'title': title_text,
                'abstract': abstract,
                'year': year,
                'journal': journal,
                'doi': doi,
                'pdf_url': url,
                'full_text': None,
                'methods_section': None,
                'results_section': None,
                'conclusions_section': None
            }
        
        except Exception as e:
            logger.error(f"Error en extracción genérica {url}: {e}")
            return self._create_minimal_paper(title, url)
    
    def _extract_section(self, soup, section_keywords):
        """Extrae una sección específica del paper"""
        for keyword in section_keywords:
            section = soup.find(['div', 'section'], 
                               string=re.compile(keyword, re.IGNORECASE))
            if section:
                return self._clean_text(section.get_text())
        return None
    
    def _clean_text(self, text: str) -> str:
        """Limpia texto extraído"""
        if not text:
            return None
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text if len(text) > 10 else None
    
    def _create_minimal_paper(self, title: str, url: str) -> Dict:
        """Crea entrada mínima cuando falla la extracción"""
        return {
            'title': title or 'Unknown Paper',
            'abstract': None,
            'year': None,
            'journal': None,
            'doi': None,
            'pdf_url': url,
            'full_text': None,
            'methods_section': None,
            'results_section': None,
            'conclusions_section': None
        }


# Función de prueba
if __name__ == "__main__":
    extractor = PaperContentExtractor()
    
    # Probar con un paper de PMC
    test_url = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4136787/"
    
    print("Extrayendo contenido de paper...")
    paper_data = extractor.extract_from_url(test_url)
    
    print("\n" + "="*60)
    print("DATOS EXTRAÍDOS:")
    print("="*60)
    for key, value in paper_data.items():
        if value and key != 'full_text':
            print(f"\n{key.upper()}:")
            print(str(value)[:200] + "..." if len(str(value)) > 200 else value)