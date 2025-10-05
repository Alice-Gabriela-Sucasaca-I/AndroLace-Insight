
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import pymysql
import json
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import pymysql
import json
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

class MySQLManager:
    def __init__(self):
        
        self.host = os.getenv('AIVEN_MYSQL_HOST')
        #self.host = os.getenv('nasa-bio-nasa-bio.e.aivencloud.com')
        
        self.port = int(os.getenv('AIVEN_MYSQL_PORT', 23069))
        self.user = os.getenv('AIVEN_MYSQL_USER', 'avnadmin')
        self.password = os.getenv('AIVEN_MYSQL_PASSWORD')
        #self.database = os.getenv('AIVEN_MYSQL_DATABASE', 'defaultdb')
        self.database = os.getenv('AIVEN_MYSQL_DATABASE', 'bio_papers_db')
        
        if not all([self.host, self.password]):
            raise ValueError(
                "Faltan variables de entorno de Aiven. "
                "Configura: AIVEN_MYSQL_HOST, AIVEN_MYSQL_PASSWORD"
            )
        
        self.connection_string = (
            f"mysql+pymysql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
            f"?charset=utf8mb4&ssl_ca=&ssl_verify_cert=false&ssl_verify_identity=false"
        )
        
        try:
            self.engine = create_engine(
                self.connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False,
                connect_args={
                    'ssl': {'check_hostname': False}
                }
            )
            
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            logger.info(f"Conectado a MySQL Aiven: {self.host}:{self.port}/{self.database}")
            
        except Exception as e:
            logger.error(f"Error inicializando conexión: {e}")
            raise
    def get_session(self):
        return self.SessionLocal()
    
    def insert_paper(self, paper_data: Dict) -> int:
        session = self.get_session()
        try:
            query = text("""
                INSERT INTO PAPER 
                (title, abstract, year, journal, DOI, pdf_url, 
                 full_text, results_section, conclusions_section, methods_section)
                VALUES 
                (:title, :abstract, :year, :journal, :doi, :pdf_url,
                 :full_text, :results, :conclusions, :methods)
                ON DUPLICATE KEY UPDATE
                abstract = VALUES(abstract),
                full_text = VALUES(full_text)
            """)
            
            result = session.execute(query, {
                'title': paper_data.get('title'),
                'abstract': paper_data.get('abstract'),
                'year': paper_data.get('year'),
                'journal': paper_data.get('journal'),
                'doi': paper_data.get('doi'),
                'pdf_url': paper_data.get('pdf_url'),
                'full_text': paper_data.get('full_text'),
                'results': paper_data.get('results_section'),
                'conclusions': paper_data.get('conclusions_section'),
                'methods': paper_data.get('methods_section')
            })
            
            session.commit()
            paper_id = result.lastrowid
            logger.info(f" Paper insertado: ID {paper_id}")
            return paper_id
            
        except Exception as e:
            session.rollback()
            logger.error(f" Error insertando paper: {e}")
            raise
        finally:
            session.close()
    
    def get_paper_by_id(self, paper_id: int) -> Optional[Dict]:
        """Obtener paper por ID"""
        session = self.get_session()
        try:
            query = text("SELECT * FROM PAPER WHERE id_paper = :id")
            result = session.execute(query, {'id': paper_id}).fetchone()
            
            if result:
                return dict(result._mapping)
            return None
            
        finally:
            session.close()
    
    def get_all_papers(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Obtener todos los papers con paginación"""
        session = self.get_session()
        try:
            query = text("""
                SELECT id_paper, title, abstract, year, journal, DOI
                FROM PAPER
                ORDER BY year DESC, id_paper DESC
                LIMIT :limit OFFSET :offset
            """)
            
            results = session.execute(query, {'limit': limit, 'offset': offset}).fetchall()
            return [dict(row._mapping) for row in results]
            
        finally:
            session.close()
    
    def search_papers(self, search_term: str, limit: int = 50) -> List[Dict]:
        """Búsqueda de papers por texto"""
        session = self.get_session()
        try:
            query = text("""
                SELECT id_paper, title, abstract, year, journal,
                       MATCH(title, abstract) AGAINST(:search) as relevance
                FROM PAPER
                WHERE MATCH(title, abstract) AGAINST(:search IN NATURAL LANGUAGE MODE)
                ORDER BY relevance DESC
                LIMIT :limit
            """)
            
            results = session.execute(query, {
                'search': search_term,
                'limit': limit
            }).fetchall()
            
            return [dict(row._mapping) for row in results]
            
        finally:
            session.close()
    def test_connection(self) -> bool:
        """Probar conexión a la base de datos"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("✅ Conexión a base de datos exitosa")
                return True
        except Exception as e:
            logger.error(f"❌ Error de conexión: {e}")
            return False
        # autor
    
    def insert_author(self, first_name: str, last_name: str, affiliation: str = None) -> int:
        """Insertar autor"""
        session = self.get_session()
        try:
            query = text("""
                INSERT INTO AUTHOR (first_name, last_name, affiliation)
                VALUES (:first_name, :last_name, :affiliation)
                ON DUPLICATE KEY UPDATE id_author = LAST_INSERT_ID(id_author)
            """)
            
            result = session.execute(query, {
                'first_name': first_name,
                'last_name': last_name,
                'affiliation': affiliation
            })
            
            session.commit()
            return result.lastrowid
            
        except Exception as e:
            session.rollback()
            logger.error(f" Error insertando autor: {e}")
            raise
        finally:
            session.close()
    
    def link_paper_author(self, paper_id: int, author_id: int, position: int = 0):
        """Vincular paper con autor"""
        session = self.get_session()
        try:
            query = text("""
                INSERT IGNORE INTO PAPER_AUTHOR (id_paper, id_author, author_position)
                VALUES (:paper_id, :author_id, :position)
            """)
            
            session.execute(query, {
                'paper_id': paper_id,
                'author_id': author_id,
                'position': position
            })
            session.commit()
            
        finally:
            session.close()
    
    # keyword
    
    def insert_keyword(self, keyword: str) -> int:
        """Insertar keyword"""
        session = self.get_session()
        try:
            query = text("""
                INSERT INTO KEYWORD (word)
                VALUES (:word)
                ON DUPLICATE KEY UPDATE id_keyword = LAST_INSERT_ID(id_keyword)
            """)
            
            result = session.execute(query, {'word': keyword})
            session.commit()
            return result.lastrowid
            
        finally:
            session.close()
    
    def link_paper_keyword(self, paper_id: int, keyword_id: int, relevance: float = 1.0):
        """Vincular paper con keyword"""
        session = self.get_session()
        try:
            query = text("""
                INSERT INTO PAPER_KEYWORD (id_paper, id_keyword, relevance_score)
                VALUES (:paper_id, :keyword_id, :relevance)
                ON DUPLICATE KEY UPDATE relevance_score = :relevance
            """)
            
            session.execute(query, {
                'paper_id': paper_id,
                'keyword_id': keyword_id,
                'relevance': relevance
            })
            session.commit()
            
        finally:
            session.close()
    
    # ia
    
    def insert_ai_summary(self, summary_data: Dict):
        """Insertar resumen generado por IA"""
        session = self.get_session()
        try:
            query = text("""
                INSERT INTO AI_SUMMARY 
                (id_paper, summary_abstract, summary_results, summary_conclusions,
                 hypothesis, key_findings, student_mode_explanation, embedding_vector)
                VALUES 
                (:paper_id, :summary_abstract, :summary_results, :summary_conclusions,
                 :hypothesis, :key_findings, :student_explanation, :embedding)
                ON DUPLICATE KEY UPDATE
                summary_abstract = VALUES(summary_abstract),
                summary_results = VALUES(summary_results),
                summary_conclusions = VALUES(summary_conclusions),
                hypothesis = VALUES(hypothesis),
                key_findings = VALUES(key_findings),
                student_mode_explanation = VALUES(student_mode_explanation),
                embedding_vector = VALUES(embedding_vector),
                last_updated = CURRENT_TIMESTAMP
            """)
            
            session.execute(query, {
                'paper_id': summary_data['id_paper'],
                'summary_abstract': summary_data.get('summary_abstract'),
                'summary_results': summary_data.get('summary_results'),
                'summary_conclusions': summary_data.get('summary_conclusions'),
                'hypothesis': summary_data.get('hypothesis'),
                'key_findings': json.dumps(summary_data.get('key_findings', [])),
                'student_explanation': summary_data.get('student_explanation'),
                'embedding': json.dumps(summary_data.get('embedding', []))
            })
            
            session.commit()
            logger.info(f" AI Summary guardado para paper {summary_data['id_paper']}")
            
        except Exception as e:
            session.rollback()
            logger.error(f" Error guardando AI summary: {e}")
            raise
        finally:
            session.close()
    
    def get_ai_summary(self, paper_id: int) -> Optional[Dict]:
        """Obtener resumen IA de un paper"""
        session = self.get_session()
        try:
            query = text("SELECT * FROM AI_SUMMARY WHERE id_paper = :id")
            result = session.execute(query, {'id': paper_id}).fetchone()
            
            if result:
                data = dict(result._mapping)
        
                if data.get('key_findings'):
                    data['key_findings'] = json.loads(data['key_findings'])
                if data.get('embedding_vector'):
                    data['embedding_vector'] = json.loads(data['embedding_vector'])
                return data
            return None
            
        finally:
            session.close()
    
    # op. temas
    
    def insert_theme(self, theme_name, description, color="#3498db"):
        """Insertar un tema"""
        session = self.get_session()
        try:
            from sqlalchemy import text
            query = text("""
                INSERT INTO THEME (theme_name, description, color)
                VALUES (:theme_name, :description, :color)
            """)
            result = session.execute(query, {
                'theme_name': theme_name,
                'description': description,
                'color': color
            })
            session.commit()
            return result.lastrowid
        except Exception as e:
            print(f"Error insertando tema: {e}")
            session.rollback()
            return None
        finally:
            session.close()
    
    def link_paper_theme(self, paper_id, theme_id, confidence=0.8):
        """Vincular paper con tema"""
        session = self.get_session()
        try:
            from sqlalchemy import text
            query = text("""
                INSERT INTO PAPER_THEME (id_paper, id_theme, confidence_score)
                VALUES (:paper_id, :theme_id, :confidence)
                ON DUPLICATE KEY UPDATE confidence_score = :confidence
            """)
            session.execute(query, {
                'paper_id': paper_id,
                'theme_id': theme_id,
                'confidence': confidence
            })
            session.commit()
            return True
        except Exception as e:
            logger.error(f"Error vinculando paper-tema: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_papers_by_theme(self, theme_id: int, limit: int = 50) -> List[Dict]:
        """Obtener papers por tema"""
        session = self.get_session()
        try:
            query = text("""
                SELECT p.*, pt.confidence_score
                FROM PAPER p
                JOIN PAPER_THEME pt ON p.id_paper = pt.id_paper
                WHERE pt.id_theme = :theme_id
                ORDER BY pt.confidence_score DESC
                LIMIT :limit
            """)
            
            results = session.execute(query, {
                'theme_id': theme_id,
                'limit': limit
            }).fetchall()
            
            return [dict(row._mapping) for row in results]
            
        finally:
            session.close()
    
    def get_all_themes_with_counts(self) -> List[Dict]:
        """Obtener todos los temas con conteo de papers"""
        session = self.get_session()
        try:
            query = text("""
                SELECT t.*, COUNT(pt.id_paper) as paper_count
                FROM THEME t
                LEFT JOIN PAPER_THEME pt ON t.id_theme = pt.id_theme
                GROUP BY t.id_theme
                ORDER BY paper_count DESC
            """)
            
            results = session.execute(query).fetchall()
            return [dict(row._mapping) for row in results]
            
        finally:
            session.close()
    
    # efectos
    
    def insert_effect(self, paper_id, effect_type, effect_description, confidence_score=0.8, section_source='results'):
        """Insertar un efecto detectado en un paper"""
        session = self.get_session()
        try:
            from sqlalchemy import text
            query = text("""
                INSERT INTO EFFECT (id_paper, effect_type, effect_description, confidence_score, section_source)
                VALUES (:paper_id, :effect_type, :effect_description, :confidence_score, :section_source)
            """)
            session.execute(query, {
                'paper_id': paper_id,
                'effect_type': effect_type,
                'effect_description': effect_description,
                'confidence_score': confidence_score,
                'section_source': section_source
            })
            session.commit()
            return True
        except Exception as e:
            print(f"Error insertando efecto: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_effects_by_paper(self, paper_id: int) -> List[Dict]:
        """Obtener efectos de un paper"""
        session = self.get_session()
        try:
            query = text("SELECT * FROM EFFECT WHERE id_paper = :id")
            results = session.execute(query, {'id': paper_id}).fetchall()
            return [dict(row._mapping) for row in results]
            
        finally:
            session.close()
    
    # comparaciones
    def insert_comparison(self, comparison_data: Dict) -> int:
        """Insertar comparación - CORREGIDO para tu ENUM"""
        session = self.get_session()
        try:
            # Validar consensus_level
            valid_levels = ['strong_agreement', 'agreement', 'mixed', 'disagreement']
            consensus = comparison_data.get('consensus_level', 'mixed')
            if consensus not in valid_levels:
                consensus = 'mixed'
            
            query = text("""
                INSERT INTO COMPARISON 
                (topic, papers_supporting, papers_against, papers_neutral, 
                consensus_level, summary, paper_ids)
                VALUES 
                (:topic, :supporting, :against, :neutral, :consensus, :summary, :paper_ids)
            """)
            
            result = session.execute(query, {
                'topic': comparison_data['topic'],
                'supporting': comparison_data.get('papers_supporting', 0),
                'against': comparison_data.get('papers_against', 0),
                'neutral': comparison_data.get('papers_neutral', 0),
                'consensus': consensus,
                'summary': comparison_data.get('summary', ''),
                'paper_ids': json.dumps(comparison_data.get('paper_ids', []))
            })
            
            session.commit()
            logger.info(f"Comparación insertada: {comparison_data['topic']}")
            return result.lastrowid
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error insertando comparación: {e}")
            raise
        finally:
            session.close()
        
    def get_comparisons_by_topic(self, topic: str) -> List[Dict]:
        """Obtener comparaciones por tópico"""
        session = self.get_session()
        try:
            query = text("""
                SELECT * FROM COMPARISON 
                WHERE topic LIKE :topic
                ORDER BY created_at DESC
            """)
            
            results = session.execute(query, {
                'topic': f'%{topic}%'
            }).fetchall()
            
            comparisons = []
            for row in results:
                data = dict(row._mapping)
                if data.get('paper_ids'):
                    data['paper_ids'] = json.loads(data['paper_ids'])
                comparisons.append(data)
            
            return comparisons
            
        finally:
            session.close()
    
    # recomendaciones de papers simimlares
    
    def insert_recommendations(self, paper_id: int, recommendations: List[Dict]):
        """Insertar recomendaciones de papers similares"""
        session = self.get_session()
        try:
            query = text("""
                INSERT INTO RECOMMENDATION 
                (id_paper, recommended_papers, similarity_scores, reason)
                VALUES (:paper_id, :recommended, :scores, :reason)
                ON DUPLICATE KEY UPDATE
                recommended_papers = VALUES(recommended_papers),
                similarity_scores = VALUES(similarity_scores)
            """)
            
            rec_ids = [r['paper_id'] for r in recommendations]
            scores = [r['score'] for r in recommendations]
            
            session.execute(query, {
                'paper_id': paper_id,
                'recommended': json.dumps(rec_ids),
                'scores': json.dumps(scores),
                'reason': 'Based on semantic similarity'
            })
            
            session.commit()
            
        finally:
            session.close()
    
    def get_recommendations(self, paper_id: int) -> List[Dict]:
        """Obtener recomendaciones para un paper"""
        session = self.get_session()
        try:
            query = text("""
                SELECT r.*, p.title, p.abstract, p.year
                FROM RECOMMENDATION r
                JOIN PAPER p ON p.id_paper IN (
                    SELECT JSON_EXTRACT(r.recommended_papers, CONCAT('$[', numbers.n, ']'))
                    FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
                )
                WHERE r.id_paper = :id
                LIMIT 5
            """)
            
            results = session.execute(query, {'id': paper_id}).fetchall()
            return [dict(row._mapping) for row in results]
            
        finally:
            session.close()
    
    #estadisticas
    
    def get_statistics(self) -> Dict:
        """Obtener estadísticas generales"""
        session = self.get_session()
        try:
            stats = {}
            
            result = session.execute(text("SELECT COUNT(*) as count FROM PAPER")).fetchone()
            stats['total_papers'] = result.count
            
            result = session.execute(text("""
                SELECT year, COUNT(*) as count 
                FROM PAPER 
                WHERE year IS NOT NULL
                GROUP BY year 
                ORDER BY year DESC
            """)).fetchall()
            stats['papers_by_year'] = [dict(row._mapping) for row in result]
            
            result = session.execute(text("SELECT COUNT(*) as count FROM THEME")).fetchone()
            stats['total_themes'] = result.count
            
            result = session.execute(text("SELECT COUNT(*) as count FROM KEYWORD")).fetchone()
            stats['total_keywords'] = result.count
            
            result = session.execute(text("SELECT COUNT(*) as count FROM AI_SUMMARY")).fetchone()
            stats['papers_with_ai'] = result.count
            
            return stats
            
        finally:
            session.close()
    
    
    def get_top_keywords(self, limit: int = 20) -> List[Dict]:
        """Obtener keywords más frecuentes"""
        session = self.get_session()
        try:
            query = text("""
                SELECT k.word, COUNT(pk.id_paper) as frequency
                FROM KEYWORD k
                JOIN PAPER_KEYWORD pk ON k.id_keyword = pk.id_keyword
                GROUP BY k.id_keyword, k.word
                ORDER BY frequency DESC
                LIMIT :limit
            """)
            
            results = session.execute(query, {'limit': limit}).fetchall()
            return [dict(row._mapping) for row in results]
            
        finally:
            session.close()
    
    def close(self):
        """Cerrar conexión"""
        self.engine.dispose()
        logger.info(" Conexión MySQL cerrada")



def get_db_manager(local=True) -> MySQLManager:
    """Factory function para obtener instancia del manager"""
    return MySQLManager(use_local=local)

if __name__ == "__main__":
    print(" Probando conexión MySQL...\n")
    
    db = MySQLManager()

    
    if db.test_connection():
        print("\n Conexión exitosa!")
        
        stats = db.get_statistics()
        print(f"\nEstadísticas:")
        print(f"   Total papers: {stats['total_papers']}")
        print(f"   Total temas: {stats['total_themes']}")
        print(f"   Papers con IA: {stats['papers_with_ai']}")
    else:
        print("\n Error de conexión")
