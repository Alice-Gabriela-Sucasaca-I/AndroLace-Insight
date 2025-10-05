"""
NASA API Integration (Simplificado)
"""

import requests
import time
from loguru import logger

class NASAAPIIntegration:
    def __init__(self):
        self.pubmed_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        
    def get_pubmed_abstract(self, pmid):
        """Obtener abstract desde PubMed"""
        try:
            url = f"{self.pubmed_base}efetch.fcgi"
            params = {
                'db': 'pubmed',
                'id': pmid,
                'retmode': 'xml',
                'rettype': 'abstract'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.content, 'xml')
                abstract_tag = soup.find('AbstractText')
                
                if abstract_tag:
                    return abstract_tag.get_text()
            
            return None
            
        except Exception as e:
            logger.warning(f"Error PubMed API: {e}")
            return None
    
    def enrich_paper_data(self, paper_data):
        """Enriquecer datos de un paper"""
        enriched = paper_data.copy()
        
        time.sleep(0.5)  
        
        return enriched


if __name__ == "__main__":
    nasa = NASAAPIIntegration()
    print("NASA API Integration lista")