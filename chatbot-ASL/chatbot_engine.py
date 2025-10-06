import os
import requests
from bs4 import BeautifulSoup
import google.generativeai as genai


class ChatbotEngine:
    _gemini_model = None
    _paper_cache = {} 

    @classmethod
    def _get_gemini_model(cls):
        if cls._gemini_model is None:
            api_key = os.getenv('GEMINI_API_KEY')

            if not api_key:
                raise ValueError(
                    "ERROR: GEMINI_API_KEY not found.\n"
                    "Set it in your .env file or as an environment variable.\n"
                    "Get a key at: https://makersuite.google.com/app/apikey"
                )

            print(" Configuring Gemini API...")
            genai.configure(api_key=api_key)
            cls._gemini_model = genai.GenerativeModel('gemini-1.5-flash')
            print(" Gemini API ready")

        return cls._gemini_model

    @staticmethod
    def _fetch_paper_content(url):
        try:
            print(f"📥 Downloading: {url[:60]}...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            content = {
                'title': '',
                'authors': [],
                'abstract': '',
                'sections': {},
                'images': [],
                'tables': []
            }

            meta_title = soup.find('meta', attrs={'name': 'citation_title'}) or soup.find('meta', attrs={'name': 'dc.Title'})
            if meta_title and meta_title.get('content'):
                content['title'] = meta_title['content'].strip()
            else:
                title_tag = soup.find('h1') or soup.find('h1', class_='content-title')
                if title_tag:
                    content['title'] = title_tag.get_text(strip=True)

            authors = []
            for meta in soup.find_all('meta', attrs={'name': 'citation_author'}):
                if meta.get('content'):
                    authors.append(meta['content'].strip())
            if not authors:
                auth_nodes = soup.find_all(['a', 'span'], class_=['contrib-auth', 'authors', 'author'])
                authors = [a.get_text(strip=True) for a in auth_nodes][:10]
            content['authors'] = authors

            abs_meta = soup.find('meta', attrs={'name': 'citation_abstract'}) or soup.find('meta', attrs={'name': 'description'})
            if abs_meta and abs_meta.get('content'):
                content['abstract'] = abs_meta['content'].strip()
            else:
                abstract_div = soup.find(lambda tag: tag.name in ('div', 'section') and 'abstract' in (tag.get('class') or []))
                if not abstract_div:
                    abstract_div = soup.find(id='abstract') or soup.find(id='Abs1')
                if abstract_div:
                    content['abstract'] = ' '.join([p.get_text(strip=True) for p in abstract_div.find_all('p')])

            sections = {}
            article_root = soup.find('article') or soup.find('div', id='main') or soup.find('div', id='content') or soup
            headers = article_root.find_all(['h2', 'h3'])
            for h in headers:
                sec_title = h.get_text(strip=True)
                texts = []
                for sib in h.find_next_siblings():
                    if sib.name and sib.name.startswith('h'):
                        break
                    if sib.name == 'p':
                        texts.append(sib.get_text(strip=True))
                    if sib.name in ('div', 'section'):
                        for p in sib.find_all('p'):
                            texts.append(p.get_text(strip=True))
                if texts:
                    sections[sec_title] = ' '.join(texts)

            if not sections:
                paras = [p.get_text(strip=True) for p in article_root.find_all('p')]
                if paras:
                    long_text = '\n\n'.join(paras)
                    sections['FullText'] = long_text

            content['sections'] = sections
            figs = []
            for fig in soup.find_all(['figure', 'div'], class_=['fig', 'figure']):
                cap = fig.find(['figcaption', 'div'], class_=['caption', 'fig-caption'])
                if cap:
                    figs.append(cap.get_text(strip=True))
            content['images'] = figs
            tabs = []
            for table in soup.find_all(['table', 'div'], class_=['table-wrap', 'tbl']):
                cap = table.find(['caption', 'div'], class_=['caption'])
                if cap:
                    tabs.append(cap.get_text(strip=True))
            content['tables'] = tabs

            print(f" Extracted: {len(content['sections'])} sections, {len(content['images'])} figures, {len(content['tables'])} tables")
            return content

        except Exception as e:
            print(f" Error downloading paper: {str(e)}")
            return None

    @classmethod
    def answer(cls, paper_title, pregunta, papers):
        """Answer a question about a paper using Gemini or a local responder.

        Parameters keep the original names for compatibility: `pregunta` is the question string.
        """
        try:
            paper = next((p for p in papers if p.get('Title') == paper_title or p.get('title') == paper_title), None)
            if not paper:
                return f" Paper not found: '{paper_title}'"

            paper_url = paper.get('Link') or paper.get('link')

            if paper_url in cls._paper_cache:
                print("Using paper from cache")
                content = cls._paper_cache[paper_url]
            else:
                content = cls._fetch_paper_content(paper_url)
                if content:
                    cls._paper_cache[paper_url] = content

            if not content:
                return "Could not download the paper. Check the URL."

            prompt = cls._build_prompt(content, pregunta)

            use_gemini = os.getenv('USE_GEMINI', '1') == '1'  
            api_key = os.getenv('GEMINI_API_KEY')

            if use_gemini and api_key:
                try:
                    print("🤖 Querying Gemini...")
                    model = cls._get_gemini_model()
                    response = model.generate_content(
                        prompt,
                        generation_config={
                            'temperature': 0.7,
                            'max_output_tokens': 1000,
                        }
                    )
                    answer = getattr(response, 'text', None) or str(response)
                    print("✅ Response generated by Gemini")
                    return answer
                except Exception as e:
                    print(f"⚠️ Gemini call failed, falling back to local responder: {e}")

            print("💻 Generating local response based on the paper (no external API)...")
            return cls._local_answer(content, pregunta)

        except Exception as e:
            error_msg = str(e)
            print(f"Error: {error_msg}")
            if "API key" in error_msg or "GEMINI_API_KEY" in error_msg:
                return (
                    " ERROR: Missing Gemini API Key.\n\n"
                    "Steps to fix:\n"
                    "1. Go to: https://makersuite.google.com/app/apikey\n"
                    "2. Create an API key (it's FREE)\n"
                    "3. Add it to your .env file:\n"
                    "   GEMINI_API_KEY=your_api_key_here\n"
                    "4. Restart the service"
                )
            elif "quota" in error_msg.lower() or "rate limit" in error_msg.lower():
                return "Request quota exceeded. Wait and try again."
            else:
                return f" Error processing the question: {error_msg}"

    @classmethod
    def _build_prompt(cls, content, pregunta):
        context_parts = []
        if content.get('title'):
            context_parts.append(f"**TITLE:** {content['title']}")
        if content.get('authors'):
            context_parts.append(f"**AUTHORS:** {', '.join(content['authors'][:5])}")
        if content.get('abstract'):
            abstract = content['abstract'][:1500]
            context_parts.append(f"**ABSTRACT:**\n{abstract}")

        if content.get('sections'):
            context_parts.append("\n**PAPER CONTENT:**")
            for section_name, section_text in list(content['sections'].items())[:5]:
                preview = section_text[:800]
                if len(section_text) > 800:
                    preview += "..."
                context_parts.append(f"\n--- {section_name} ---\n{preview}")

        if content.get('images'):
            context_parts.append(f"\n**FIGURES ({len(content['images'])}):**")
            for i, img in enumerate(content['images'][:3], 1):
                context_parts.append(f"{i}. {img[:150]}")

        if content.get('tables'):
            context_parts.append(f"\n**TABLES ({len(content['tables'])}):**")
            for i, table in enumerate(content['tables'][:3], 1):
                context_parts.append(f"{i}. {table[:150]}")

        context = "\n".join(context_parts)

        prompt = f"""You are an expert assistant for analyzing scientific papers. Answer the user's question using ONLY the information present in the paper.

{context}

QUESTION: {pregunta}

ANSWER:"""

        return prompt

    @classmethod
    def _local_answer(cls, content, pregunta):
        """Generate a simple local answer using only the extracted content.

        Uses the abstract and first sections to answer basic questions when no
        external API is available.
        """
        lower_q = pregunta.lower()
        preferred = None
        if any(k in lower_q for k in ('result', 'results', 'conclusion', 'conclude', 'findings')):
            for name in content.get('sections', {}):
                if any(w in name.lower() for w in ('result', 'conclus', 'conclusion')):
                    preferred = content['sections'][name]
                    break
        if not preferred and any(k in lower_q for k in ('method', 'methods', 'procedure')):
            for name in content.get('sections', {}):
                if any(w in name.lower() for w in ('method', 'procedure', 'materials')):
                    preferred = content['sections'][name]
                    break

        parts = []
        if content.get('title'):
            parts.append(f"Title: {content['title']}")
        if content.get('authors'):
            parts.append(f"Authors: {', '.join(content['authors'][:5])}")
        if content.get('abstract'):
            parts.append('\nABSTRACT:\n' + content['abstract'])

        if preferred:
            parts.append('\nRELEVANT SECTION:\n' + (preferred[:1000] + '...' if len(preferred) > 1000 else preferred))
        else:
            for name, text in list(content.get('sections', {}).items())[:3]:
                parts.append(f"\n--- {name} ---\n" + (text[:500] + '...' if len(text) > 500 else text))

        context = "\n".join(parts)

        if any(k in lower_q for k in ('summary', 'summar', 'what is', 'describe')):
            if content.get('abstract'):
                summary = content['abstract'][:1500]
            else:
                summary = ' '.join([t for _, t in list(content.get('sections', {}).items())[:3]])[:1500]
            return "Answer (based ONLY on the paper):\n\n" + summary

        out = "Answer (based ONLY on the paper):\n\n" + (context[:3000])
        out += "\n\nNote: If the answer is not explicit in the paper, this will be stated."
        return out